package indexer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	wtfabi "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/models"
)

// mockEscrowPersistence implements EscrowPersistence in-memory for testing.
type mockEscrowPersistence struct {
	checkpoints map[string]uint64
	events      []*models.EscrowEvent
	failSave    bool
}

func newMockEscrowPersistence() *mockEscrowPersistence {
	return &mockEscrowPersistence{
		checkpoints: make(map[string]uint64),
		events:      make([]*models.EscrowEvent, 0),
	}
}

func (m *mockEscrowPersistence) GetCheckpoint(ctx context.Context, chainID int64, streamID string) (uint64, bool, error) {
	if b, ok := m.checkpoints[streamID]; ok {
		return b, true, nil
	}
	return 0, false, nil
}

func (m *mockEscrowPersistence) SaveEscrowBatch(
	ctx context.Context,
	chainID int64,
	streamID string,
	events []*models.EscrowEvent,
	checkpointBlock uint64,
	checkpointHash string,
) error {
	if m.failSave {
		return fmt.Errorf("mock save error")
	}
	// Emulate idempotency: upsert by (chain_id, contract_address, tx_hash, log_index)
	for _, newEv := range events {
		found := false
		for i, existing := range m.events {
			if existing.ChainID == newEv.ChainID &&
				existing.ContractAddress == newEv.ContractAddress &&
				existing.TxHash == newEv.TxHash &&
				existing.LogIndex == newEv.LogIndex {
				m.events[i] = newEv
				found = true
				break
			}
		}
		if !found {
			m.events = append(m.events, newEv)
		}
	}
	if streamID != "" && checkpointBlock > 0 {
		m.checkpoints[streamID] = checkpointBlock
	}
	return nil
}

// escrowLogClient implements BlockchainClient for escrow unit testing.
type escrowLogClient struct {
	mockBlockchainClient
	logs []types.Log
}

func (c *escrowLogClient) GetLogs(ctx context.Context, contractAddress common.Address, fromBlock, toBlock uint64) ([]types.Log, error) {
	var matched []types.Log
	for _, l := range c.logs {
		if l.Address == contractAddress && l.BlockNumber >= fromBlock && l.BlockNumber <= toBlock {
			matched = append(matched, l)
		}
	}
	return matched, nil
}

// Helper to construct a typed mock Escrow log
func makeEscrowLog(
	parsedABI gethabi.ABI,
	contract common.Address,
	eventName string,
	indexedValues []interface{},
	nonIndexedValues []interface{},
	blockNumber uint64,
	txHash common.Hash,
	logIndex uint,
	removed bool,
) (types.Log, error) {
	ev, ok := parsedABI.Events[eventName]
	if !ok {
		return types.Log{}, fmt.Errorf("unknown event %s", eventName)
	}

	topics := []common.Hash{ev.ID}
	indexedArgs := make(gethabi.Arguments, 0)
	for _, arg := range ev.Inputs {
		if arg.Indexed {
			indexedArgs = append(indexedArgs, arg)
		}
	}

	for i, val := range indexedValues {
		arg := indexedArgs[i]
		switch arg.Type.T {
		case gethabi.AddressTy:
			addr := val.(common.Address)
			topics = append(topics, common.BytesToHash(addr.Bytes()))
		case gethabi.UintTy, gethabi.IntTy:
			bi := val.(*big.Int)
			topics = append(topics, common.BigToHash(bi))
		default:
			return types.Log{}, fmt.Errorf("unsupported indexed type: %v", arg.Type)
		}
	}

	nonIndexedArgs := ev.Inputs.NonIndexed()
	var data []byte
	var err error
	if len(nonIndexedArgs) > 0 {
		data, err = nonIndexedArgs.Pack(nonIndexedValues...)
		if err != nil {
			return types.Log{}, fmt.Errorf("pack non-indexed: %w", err)
		}
	}

	return types.Log{
		Address:     contract,
		Topics:      topics,
		Data:        data,
		BlockNumber: blockNumber,
		TxHash:      txHash,
		Index:       logIndex,
		Removed:     removed,
	}, nil
}

func TestEscrowIndexer_CanonicalEventsDecode(t *testing.T) {
	parsedABI, err := gethabi.JSON(strings.NewReader(wtfabi.WTFEscrowABI))
	if err != nil {
		t.Fatalf("parse escrow ABI: %v", err)
	}

	contract := common.HexToAddress("0x807EB6317FbdF219C18B58ac0BF941bC4af268D5")
	buyer := common.HexToAddress("0x1111111111111111111111111111111111111111")
	seller := common.HexToAddress("0x2222222222222222222222222222222222222222")
	winner := common.HexToAddress("0x3333333333333333333333333333333333333333")

	tests := []struct {
		name         string
		indexedVals  []interface{}
		nonIndexVals []interface{}
		wantEscrowID *string
		wantAmount   *string
	}{
		{
			name:         "EscrowCreated",
			indexedVals:  []interface{}{big.NewInt(42), buyer, seller},
			nonIndexVals: []interface{}{big.NewInt(1000)},
			wantEscrowID: strPtr("42"),
			wantAmount:   strPtr("1000"),
		},
		{
			name:         "EscrowReleased",
			indexedVals:  []interface{}{big.NewInt(42)},
			nonIndexVals: []interface{}{big.NewInt(1000)},
			wantEscrowID: strPtr("42"),
			wantAmount:   strPtr("1000"),
		},
		{
			name:         "EscrowRefunded",
			indexedVals:  []interface{}{big.NewInt(42)},
			nonIndexVals: []interface{}{big.NewInt(1000)},
			wantEscrowID: strPtr("42"),
			wantAmount:   strPtr("1000"),
		},
		{
			name:         "DisputeResolved",
			indexedVals:  []interface{}{big.NewInt(42), winner},
			nonIndexVals: []interface{}{big.NewInt(1000)},
			wantEscrowID: strPtr("42"),
			wantAmount:   strPtr("1000"),
		},
		{
			name:         "DeliveryAcknowledged",
			indexedVals:  []interface{}{big.NewInt(42)},
			nonIndexVals: []interface{}{big.NewInt(1700000000)},
			wantEscrowID: strPtr("42"),
			wantAmount:   nil,
		},
		{
			name:         "DisputeRaised",
			indexedVals:  []interface{}{big.NewInt(42), buyer},
			nonIndexVals: []interface{}{big.NewInt(50)},
			wantEscrowID: strPtr("42"),
			wantAmount:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txHash := common.HexToHash(fmt.Sprintf("0x%064x", 999))
			log, err := makeEscrowLog(parsedABI, contract, tc.name, tc.indexedVals, tc.nonIndexVals, 100, txHash, 0, false)
			if err != nil {
				t.Fatalf("makeEscrowLog %s: %v", tc.name, err)
			}

			client := &escrowLogClient{logs: []types.Log{log}}
			db := newMockEscrowPersistence()
			indexer, err := NewEscrowIndexer(client, db, nil, contract, 11155111, "wtf_escrow", 100, 50)
			if err != nil {
				t.Fatalf("NewEscrowIndexer: %v", err)
			}

			events, err := indexer.IndexRange(context.Background(), 100, 100)
			if err != nil {
				t.Fatalf("IndexRange %s error: %v", tc.name, err)
			}

			if len(events) != 1 {
				t.Fatalf("expected 1 event, got %d", len(events))
			}

			ev := events[0]
			if ev.EventType != tc.name {
				t.Errorf("expected EventType %s, got %s", tc.name, ev.EventType)
			}
			if ev.ContractAddress != contract {
				t.Errorf("expected ContractAddress %s, got %s", contract.Hex(), ev.ContractAddress.Hex())
			}
			if tc.wantEscrowID == nil {
				if ev.EscrowID != nil {
					t.Errorf("expected nil EscrowID for %s, got %s", tc.name, *ev.EscrowID)
				}
			} else {
				if ev.EscrowID == nil {
					t.Fatalf("expected EscrowID %s for %s, got nil", *tc.wantEscrowID, tc.name)
				}
				if *ev.EscrowID != *tc.wantEscrowID {
					t.Errorf("expected EscrowID %s, got %s", *tc.wantEscrowID, *ev.EscrowID)
				}
			}
			if tc.wantAmount != nil {
				if ev.Amount == nil || *ev.Amount != *tc.wantAmount {
					t.Errorf("expected Amount %s, got %v", *tc.wantAmount, ev.Amount)
				}
			}
		})
	}
}

func TestEscrowIndexer_MultipleEventsSameEscrow(t *testing.T) {
	parsedABI, _ := gethabi.JSON(strings.NewReader(wtfabi.WTFEscrowABI))
	contract := common.HexToAddress("0x807EB6317FbdF219C18B58ac0BF941bC4af268D5")
	buyer := common.HexToAddress("0x1111111111111111111111111111111111111111")
	seller := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// Same escrowId = 100 undergoes Created -> DisputeRaised -> DisputeResolved -> EscrowReleased
	l1, _ := makeEscrowLog(parsedABI, contract, "EscrowCreated", []interface{}{big.NewInt(100), buyer, seller}, []interface{}{big.NewInt(1000)}, 100, common.HexToHash("0x01"), 0, false)
	l2, _ := makeEscrowLog(parsedABI, contract, "DisputeRaised", []interface{}{big.NewInt(100), buyer}, []interface{}{big.NewInt(50)}, 101, common.HexToHash("0x02"), 0, false)
	l3, _ := makeEscrowLog(parsedABI, contract, "DisputeResolved", []interface{}{big.NewInt(100), seller}, []interface{}{big.NewInt(1000)}, 102, common.HexToHash("0x03"), 0, false)
	l4, _ := makeEscrowLog(parsedABI, contract, "EscrowReleased", []interface{}{big.NewInt(100)}, []interface{}{big.NewInt(1000)}, 103, common.HexToHash("0x04"), 0, false)

	client := &escrowLogClient{logs: []types.Log{l1, l2, l3, l4}}
	db := newMockEscrowPersistence()
	indexer, _ := NewEscrowIndexer(client, db, nil, contract, 11155111, "wtf_escrow", 100, 50)

	events, err := indexer.IndexRange(context.Background(), 100, 103)
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}

	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	for _, ev := range events {
		if ev.EscrowID == nil || *ev.EscrowID != "100" {
			t.Errorf("expected escrowId 100, got %v", ev.EscrowID)
		}
	}

	if len(db.events) != 4 {
		t.Errorf("expected 4 persisted events, got %d", len(db.events))
	}
}

func TestEscrowIndexer_IdempotencyAndRemovedHandling(t *testing.T) {
	parsedABI, _ := gethabi.JSON(strings.NewReader(wtfabi.WTFEscrowABI))
	contract := common.HexToAddress("0x807EB6317FbdF219C18B58ac0BF941bC4af268D5")

	txHash := common.HexToHash("0xaabbcc")
	l1, _ := makeEscrowLog(parsedABI, contract, "EscrowReleased", []interface{}{big.NewInt(55)}, []interface{}{big.NewInt(1000)}, 100, txHash, 0, false)

	client := &escrowLogClient{logs: []types.Log{l1}}
	db := newMockEscrowPersistence()
	indexer, _ := NewEscrowIndexer(client, db, nil, contract, 11155111, "wtf_escrow", 100, 50)

	// First pass: normal indexing
	_, err := indexer.IndexRange(context.Background(), 100, 100)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}

	if len(db.events) != 1 || db.events[0].Removed {
		t.Fatalf("expected 1 non-removed event, got len=%d removed=%v", len(db.events), db.events[0].Removed)
	}

	// Second pass with same log (idempotency test)
	_, err = indexer.IndexRange(context.Background(), 100, 100)
	if err != nil {
		t.Fatalf("second pass (idempotent): %v", err)
	}
	if len(db.events) != 1 {
		t.Fatalf("expected still 1 event after re-indexing, got %d", len(db.events))
	}

	// Third pass with Removed = true (reorg simulation)
	lReorg, _ := makeEscrowLog(parsedABI, contract, "EscrowReleased", []interface{}{big.NewInt(55)}, []interface{}{big.NewInt(1000)}, 100, txHash, 0, true)
	client.logs = []types.Log{lReorg}

	_, err = indexer.IndexRange(context.Background(), 100, 100)
	if err != nil {
		t.Fatalf("reorg pass: %v", err)
	}

	if len(db.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(db.events))
	}
	if !db.events[0].Removed {
		t.Errorf("expected Removed=true after reorg, got false")
	}
}

func TestEscrowIndexer_CheckpointFailureRollback(t *testing.T) {
	parsedABI, _ := gethabi.JSON(strings.NewReader(wtfabi.WTFEscrowABI))
	contract := common.HexToAddress("0x807EB6317FbdF219C18B58ac0BF941bC4af268D5")

	l1, _ := makeEscrowLog(parsedABI, contract, "EscrowReleased", []interface{}{big.NewInt(77)}, []interface{}{big.NewInt(1000)}, 100, common.HexToHash("0x11"), 0, false)

	// Mock client whose BlockHeader returns an error
	mockClient := &escrowLogClient{
		mockBlockchainClient: mockBlockchainClient{failBlockHeader: true},
		logs:                 []types.Log{l1},
	}
	db := newMockEscrowPersistence()
	indexer, _ := NewEscrowIndexer(mockClient, db, nil, contract, 11155111, "wtf_escrow", 100, 50)

	_, err := indexer.IndexRange(context.Background(), 100, 100)
	if err == nil {
		t.Fatalf("expected error on block header failure, got nil")
	}

	// Checkpoint must NOT advance
	_, found, _ := db.GetCheckpoint(context.Background(), 11155111, "wtf_escrow")
	if found {
		t.Errorf("checkpoint must not advance when block header fails")
	}
}

func strPtr(v string) *string {
	return &v
}
