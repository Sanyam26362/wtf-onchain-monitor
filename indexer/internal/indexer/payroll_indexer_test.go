package indexer

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
)

// mockPayrollPersistence implements PayrollPersistence for unit testing
type mockPayrollPersistence struct {
	checkpoints      map[string]uint64
	transactions     []*blockchain.TransactionMetadata
	chainEvents      []*decoder.DecodedEvent
	projectedEvents  []*decoder.DecodedEvent
	failCheckpoint   bool
	failTx           bool
	failChainEvent   bool
	failProject      bool
	checkpointHashes map[string]string
}

func newMockPayrollPersistence() *mockPayrollPersistence {
	return &mockPayrollPersistence{
		checkpoints:      make(map[string]uint64),
		checkpointHashes: make(map[string]string),
	}
}

func (m *mockPayrollPersistence) GetCheckpoint(ctx context.Context, chainID int64, streamID string) (uint64, bool, error) {
	key := fmt.Sprintf("%d:%s", chainID, streamID)
	blk, ok := m.checkpoints[key]
	return blk, ok, nil
}

func (m *mockPayrollPersistence) SaveCheckpoint(ctx context.Context, chainID int64, streamID string, blockNumber uint64, blockHash string) error {
	if m.failCheckpoint {
		return fmt.Errorf("simulated checkpoint save failure")
	}
	key := fmt.Sprintf("%d:%s", chainID, streamID)
	m.checkpoints[key] = blockNumber
	m.checkpointHashes[key] = blockHash
	return nil
}

func (m *mockPayrollPersistence) SaveTransaction(ctx context.Context, chainID int64, metadata *blockchain.TransactionMetadata) error {
	if m.failTx {
		return fmt.Errorf("simulated save transaction failure")
	}
	m.transactions = append(m.transactions, metadata)
	return nil
}

func (m *mockPayrollPersistence) SaveChainEvent(ctx context.Context, chainID int64, contractAddress common.Address, blockTimestamp uint64, event *decoder.DecodedEvent) error {
	if m.failChainEvent {
		return fmt.Errorf("simulated save chain event failure")
	}
	m.chainEvents = append(m.chainEvents, event)
	return nil
}

func (m *mockPayrollPersistence) ProjectPayrollEvent(ctx context.Context, chainID int64, blockTimestamp uint64, event *decoder.DecodedEvent) error {
	if m.failProject {
		return fmt.Errorf("simulated projection failure")
	}
	m.projectedEvents = append(m.projectedEvents, event)
	return nil
}

// mockPayrollClient implements blockchain.BlockchainClient for payroll tests
type mockPayrollClient struct {
	latestBlock     uint64
	logs            []types.Log
	failGetLogs     bool
	failBlock       bool
	failBlockHeader bool
	blockHeaders    map[uint64]*blockchain.BlockHeader
}

func (m *mockPayrollClient) LatestBlock(ctx context.Context) (uint64, error) {
	if m.failBlock {
		return 0, fmt.Errorf("simulated latest block failure")
	}
	return m.latestBlock, nil
}

func (m *mockPayrollClient) GetLogs(ctx context.Context, contractAddress common.Address, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	if m.failGetLogs {
		return nil, fmt.Errorf("simulated RPC failure on GetLogs in range [%d, %d]", fromBlock, toBlock)
	}
	var res []types.Log
	for _, l := range m.logs {
		if l.BlockNumber >= fromBlock && l.BlockNumber <= toBlock {
			res = append(res, l)
		}
	}
	return res, nil
}

func (m *mockPayrollClient) GetTokenLogs(ctx context.Context, tokenAddress common.Address, topic common.Hash, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	return nil, nil
}

func (m *mockPayrollClient) TransactionMetadata(ctx context.Context, txHash common.Hash) (*blockchain.TransactionMetadata, error) {
	return &blockchain.TransactionMetadata{
		Hash:        txHash,
		BlockNumber: 100,
		Sender:      common.HexToAddress("0x1111111111111111111111111111111111111111"),
		GasUsed:     21000,
		Status:      1,
	}, nil
}

func (m *mockPayrollClient) BlockTimestamp(ctx context.Context, blockNumber uint64) (uint64, error) {
	return 1700000000, nil
}

func (m *mockPayrollClient) BlockHeader(ctx context.Context, blockNumber uint64) (*blockchain.BlockHeader, error) {
	if m.failBlockHeader {
		return nil, fmt.Errorf("simulated block header failure")
	}
	if m.blockHeaders != nil {
		if h, ok := m.blockHeaders[blockNumber]; ok {
			return h, nil
		}
	}
	return &blockchain.BlockHeader{
		Number:     blockNumber,
		Hash:       common.HexToHash(fmt.Sprintf("0x%064x", blockNumber)),
		ParentHash: common.HexToHash(fmt.Sprintf("0x%064x", blockNumber-1)),
		Timestamp:  1700000000 + blockNumber,
	}, nil
}

func (m *mockPayrollClient) Close() {}

func (m *mockPayrollClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, nil
}

// TestPayroll_MultipleHistoricalRangesAndPartialRange tests:
// 1. Chunked historical backfill across multiple ranges
// 2. Final partial range stopping exactly at target block
// 3. Checkpoint progression after each range
func TestPayroll_MultipleHistoricalRangesAndPartialRange(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPayrollClient{latestBlock: 500}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      100,
		TargetBlock:     215, // 100..149 (50), 150..199 (50), 200..215 (16)
		BatchSize:       50,
		StreamID:        "monthly_payroll",
	}

	lastIndexed, err := svc.RunBackfill(ctx, opts)
	if err != nil {
		t.Fatalf("unexpected backfill error: %v", err)
	}

	if lastIndexed != 215 {
		t.Fatalf("expected lastIndexedBlock 215, got %d", lastIndexed)
	}

	// Verify checkpoint was updated to target block and real hash was stored
	cp, found, err := mockDB.GetCheckpoint(ctx, opts.ChainID, opts.StreamID)
	if err != nil || !found {
		t.Fatalf("expected checkpoint to be found, got err: %v", err)
	}
	if cp != 215 {
		t.Fatalf("expected checkpoint block 215, got %d", cp)
	}

	key := fmt.Sprintf("%d:%s", opts.ChainID, opts.StreamID)
	expectedHash215 := common.HexToHash(fmt.Sprintf("0x%064x", 215)).Hex()
	if mockDB.checkpointHashes[key] != expectedHash215 {
		t.Fatalf("expected checkpoint hash %s, got %s", expectedHash215, mockDB.checkpointHashes[key])
	}
}

// TestPayroll_RestartAndResumeBehavior tests:
// When a checkpoint already exists, backfill resumes from checkpoint + 1.
func TestPayroll_RestartAndResumeBehavior(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPayrollClient{latestBlock: 500}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Set initial checkpoint at block 149
	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, 11155111, streamID, 149, "")

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      100, // Should be ignored in favor of checkpoint + 1 = 150
		TargetBlock:     200,
		BatchSize:       50,
		StreamID:        streamID,
	}

	lastIndexed, err := svc.RunBackfill(ctx, opts)
	if err != nil {
		t.Fatalf("unexpected backfill error: %v", err)
	}

	if lastIndexed != 200 {
		t.Fatalf("expected lastIndexedBlock 200, got %d", lastIndexed)
	}

	// Verify checkpoint updated to 200
	cp, found, _ := mockDB.GetCheckpoint(ctx, opts.ChainID, streamID)
	if !found || cp != 200 {
		t.Fatalf("expected checkpoint 200, got %d", cp)
	}
}

// TestPayroll_RPCFailureWithoutCheckpointAdvancement tests:
// If RPC fails during range processing, checkpoint is NOT advanced.
func TestPayroll_RPCFailureWithoutCheckpointAdvancement(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPayrollClient{
		latestBlock: 500,
		failGetLogs: true, // RPC fails immediately
	}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Initial checkpoint at 100
	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, 11155111, streamID, 100, "")

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      100,
		TargetBlock:     200,
		BatchSize:       50,
		StreamID:        streamID,
	}

	_, err = svc.RunBackfill(ctx, opts)
	if err == nil {
		t.Fatal("expected error due to simulated RPC failure, got nil")
	}

	// Checkpoint MUST remain at 100
	cp, found, _ := mockDB.GetCheckpoint(ctx, opts.ChainID, streamID)
	if !found || cp != 100 {
		t.Fatalf("checkpoint should not have advanced from 100, got %d", cp)
	}
}

// TestPayroll_ReplayedRangesIdempotency tests:
// Running backfill over the same range again does not fail.
func TestPayroll_ReplayedRangesIdempotency(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPayrollClient{latestBlock: 500}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      100,
		TargetBlock:     150,
		BatchSize:       50,
		StreamID:        "monthly_payroll",
	}

	// First run
	_, err = svc.RunBackfill(ctx, opts)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	// Re-run (checkpoint is already at 150, start block is 100)
	// Backfill sees resumeBlock = 151 > TargetBlock 150 -> cleanly completes
	lastIndexed, err := svc.RunBackfill(ctx, opts)
	if err != nil {
		t.Fatalf("replayed backfill failed: %v", err)
	}
	if lastIndexed != 150 {
		t.Fatalf("expected last indexed 150, got %d", lastIndexed)
	}
}

// TestPayroll_EmployeeAddedAndRemovedDecodingAndProjection tests:
// ABI decoding and projection of EmployeeAdded and EmployeeRemoved events.
func TestPayroll_EmployeeAddedAndRemovedDecodingAndProjection(t *testing.T) {
	ctx := context.Background()
	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	filterer, err := abi.NewMainFilterer(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create filterer: %v", err)
	}

	employeeAddr := common.HexToAddress("0x28a009551412f87918958fac4732281301eAeFce")
	employerAddr := common.HexToAddress("0xa1c2753108F75f597E09ECa49af978696F72B554")
	txHash := common.HexToHash("0x1f98ade35e0eed6aa2a323ac9ca8fbc0082c55b0571df7cb0af8f7b3ba512b72")

	// 1. Pack EmployeeAdded event log
	employeeAddedEvent := filterer.ABI().Events["EmployeeAdded"]
	data, err := employeeAddedEvent.Inputs.NonIndexed().Pack(big.NewInt(500), big.NewInt(0))
	if err != nil {
		t.Fatalf("failed to pack event data: %v", err)
	}

	log := types.Log{
		Address: payrollAddr,
		Topics: []common.Hash{
			employeeAddedEvent.ID,
			common.BytesToHash(employeeAddr.Bytes()),
			common.BytesToHash(employerAddr.Bytes()),
		},
		Data:        data,
		BlockNumber: 11080817,
		TxHash:      txHash,
		Index:       112,
	}

	mockClient := &mockPayrollClient{
		latestBlock: 11080900,
		logs:        []types.Log{log},
	}
	mockDB := newMockPayrollPersistence()

	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	events, err := svc.IndexRange(ctx, 11155111, payrollAddr, 11080817, 11080817)
	if err != nil {
		t.Fatalf("IndexRange failed: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Type != decoder.EventEmployeeAdded {
		t.Fatalf("expected EventEmployeeAdded, got %s", events[0].Type)
	}

	eeAdded, ok := events[0].Data.(*abi.ABIEmployeeAddedEvent)
	if !ok {
		t.Fatalf("expected *abi.ABIEmployeeAddedEvent, got %T", events[0].Data)
	}

	if eeAdded.Employee != employeeAddr {
		t.Fatalf("expected employee %s, got %s", employeeAddr.Hex(), eeAdded.Employee.Hex())
	}
	if eeAdded.Employer != employerAddr {
		t.Fatalf("expected employer %s, got %s", employerAddr.Hex(), eeAdded.Employer.Hex())
	}
	if eeAdded.Salary.Cmp(big.NewInt(500)) != 0 {
		t.Fatalf("expected salary 500, got %s", eeAdded.Salary.String())
	}

	// Verify persistence calls
	if len(mockDB.transactions) != 1 {
		t.Fatalf("expected 1 transaction saved, got %d", len(mockDB.transactions))
	}
	if len(mockDB.chainEvents) != 1 {
		t.Fatalf("expected 1 chain event saved, got %d", len(mockDB.chainEvents))
	}
	if len(mockDB.projectedEvents) != 1 {
		t.Fatalf("expected 1 projected event, got %d", len(mockDB.projectedEvents))
	}
}

func TestPayroll_BlockHeaderFailure_CheckpointDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPayrollClient{
		latestBlock:     500,
		failBlockHeader: true, // Header retrieval fails
	}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      100,
		TargetBlock:     150,
		BatchSize:       50,
		StreamID:        "monthly_payroll",
	}

	_, err = svc.RunBackfill(ctx, opts)
	if err == nil {
		t.Fatal("expected backfill error when BlockHeader fails, got nil")
	}

	// Verify checkpoint was NOT updated
	_, found, _ := mockDB.GetCheckpoint(ctx, opts.ChainID, opts.StreamID)
	if found {
		t.Fatal("expected checkpoint to NOT be advanced when BlockHeader fails")
	}
}
