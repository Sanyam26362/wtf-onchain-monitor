package indexer

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
)

type mockBlockchainClient struct {
	latestBlock      uint64
	logs             []types.Log
	failGetTokenLogs bool
	failLatestBlock  bool
	failBlockTime    bool
	failBlockHeader  bool
	blockTimestamp   uint64
	txMetadata       *blockchain.TransactionMetadata
	blockHeaders     map[uint64]*blockchain.BlockHeader
}

func (m *mockBlockchainClient) LatestBlock(ctx context.Context) (uint64, error) {
	if m.failLatestBlock {
		return 0, fmt.Errorf("simulated RPC latest block error")
	}
	return m.latestBlock, nil
}

func (m *mockBlockchainClient) GetLogs(ctx context.Context, contractAddress common.Address, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	return nil, nil
}

func (m *mockBlockchainClient) GetTokenLogs(ctx context.Context, tokenAddress common.Address, topic common.Hash, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	if m.failGetTokenLogs {
		return nil, fmt.Errorf("simulated RPC network failure on GetTokenLogs")
	}
	var filtered []types.Log
	for _, l := range m.logs {
		if l.Address == tokenAddress && l.BlockNumber >= fromBlock && l.BlockNumber <= toBlock {
			if len(l.Topics) > 0 && l.Topics[0] == topic {
				filtered = append(filtered, l)
			}
		}
	}
	return filtered, nil
}

func (m *mockBlockchainClient) TransactionMetadata(ctx context.Context, txHash common.Hash) (*blockchain.TransactionMetadata, error) {
	if m.txMetadata != nil {
		return m.txMetadata, nil
	}
	return &blockchain.TransactionMetadata{
		Hash:        txHash,
		BlockNumber: 100,
		Sender:      common.HexToAddress("0x1111111111111111111111111111111111111111"),
		GasUsed:     21000,
		Status:      1,
	}, nil
}

func (m *mockBlockchainClient) BlockTimestamp(ctx context.Context, blockNumber uint64) (uint64, error) {
	if m.failBlockTime {
		return 0, fmt.Errorf("simulated RPC block timestamp error")
	}
	if m.blockTimestamp > 0 {
		return m.blockTimestamp, nil
	}
	return 1700000000, nil
}

func (m *mockBlockchainClient) BlockHeader(ctx context.Context, blockNumber uint64) (*blockchain.BlockHeader, error) {
	if m.failBlockHeader {
		return nil, fmt.Errorf("simulated RPC block header error")
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

func (m *mockBlockchainClient) Close() {}

func (m *mockBlockchainClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, nil
}

func getTestDB(t *testing.T) *persistence.Postgres {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:root@localhost:5432/wtf_onchain"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping: database unavailable: %v", err)
	}
	return pg
}

// TestTokenIndexer_BoundedRangeAndCheckpoint verifies:
// 1. Process bounded range
// 2. Checkpoint advances correctly
func TestTokenIndexer_BoundedRangeAndCheckpoint(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tokenAddr := common.HexToAddress("0xAAAA00000000000000000000000000000000AAAA")
	streamID := fmt.Sprintf("test_stream_bounded_%d", time.Now().UnixNano())

	parsedABI, err := indexerABI.LoadERC20ABI("", `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`)
	if err != nil {
		t.Fatalf("failed to load ABI: %v", err)
	}

	filterer, _ := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
	dec, _ := decoder.NewERC20Decoder(filterer)

	packedData, _ := parsedABI.Events["Transfer"].Inputs.NonIndexed().Pack(big.NewInt(1000))
	txHash := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	mockClient := &mockBlockchainClient{
		latestBlock: 200,
		logs: []types.Log{
			{
				Address: tokenAddr,
				Topics: []common.Hash{
					filterer.TransferTopic(),
					common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
					common.BytesToHash(common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()),
				},
				Data:        packedData,
				BlockNumber: 105,
				TxHash:      txHash,
				Index:       0,
			},
		},
		blockTimestamp: 1718000000,
	}

	indexerService, err := NewTokenIndexer(
		mockClient,
		dec,
		db,
		chainID,
		100, // start block
		50,  // batch size
		5,   // confirmation depth
		streamID,
	)
	if err != nil {
		t.Fatalf("failed to create token indexer: %v", err)
	}

	// 1. Process next batch: from 100 to min(100+50-1, 200-5) = 149
	processed, from, to, count, err := indexerService.ProcessNextBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessNextBatch error: %v", err)
	}
	if !processed {
		t.Fatal("expected batch to be processed")
	}
	if from != 100 || to != 149 {
		t.Fatalf("expected range 100->149, got %d->%d", from, to)
	}
	if count != 1 {
		t.Fatalf("expected 1 transfer indexed, got %d", count)
	}

	// 2. Verify durable checkpoint advanced to 149 with real block hash
	checkpoint, found, err := db.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to get checkpoint: %v", err)
	}
	if !found || checkpoint != 149 {
		t.Fatalf("expected checkpoint 149, got %d (found=%t)", checkpoint, found)
	}

	cpRecord, found, err := db.GetCheckpointWithHash(ctx, chainID, streamID)
	if err != nil || !found || cpRecord == nil {
		t.Fatalf("expected checkpoint with hash to be found, err: %v", err)
	}
	expectedHash149 := common.HexToHash(fmt.Sprintf("0x%064x", 149)).Hex()
	if cpRecord.BlockNumber != 149 || cpRecord.BlockHash != expectedHash149 {
		t.Fatalf("expected checkpoint 149 with hash %s, got block %d hash %s", expectedHash149, cpRecord.BlockNumber, cpRecord.BlockHash)
	}

	// 3. Process next batch: should resume from 150 to 195 (latest 200 - conf 5)
	processed, from, to, count, err = indexerService.ProcessNextBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessNextBatch second call error: %v", err)
	}
	if !processed {
		t.Fatal("expected second batch to be processed")
	}
	if from != 150 || to != 195 {
		t.Fatalf("expected range 150->195, got %d->%d", from, to)
	}
	if count != 0 {
		t.Fatalf("expected 0 transfers in empty range, got %d", count)
	}

	// 4. Verify checkpoint advanced to 195 with real block hash
	checkpoint, found, err = db.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to get checkpoint: %v", err)
	}
	if !found || checkpoint != 195 {
		t.Fatalf("expected checkpoint 195, got %d", checkpoint)
	}

	cpRecord, found, err = db.GetCheckpointWithHash(ctx, chainID, streamID)
	if err != nil || !found || cpRecord == nil {
		t.Fatalf("expected checkpoint with hash to be found, err: %v", err)
	}
	expectedHash195 := common.HexToHash(fmt.Sprintf("0x%064x", 195)).Hex()
	if cpRecord.BlockNumber != 195 || cpRecord.BlockHash != expectedHash195 {
		t.Fatalf("expected checkpoint 195 with hash %s, got block %d hash %s", expectedHash195, cpRecord.BlockNumber, cpRecord.BlockHash)
	}
}

// TestTokenIndexer_FailureRecovery verifies:
// 1. Simulate RPC failure
// 2. Checkpoint does not skip failed range
func TestTokenIndexer_FailureRecovery(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tokenAddr := common.HexToAddress("0xCCCC00000000000000000000000000000000CCCC")
	streamID := fmt.Sprintf("test_stream_failure_%d", time.Now().UnixNano())

	parsedABI, _ := indexerABI.LoadERC20ABI("", `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`)

	filterer, _ := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
	dec, _ := decoder.NewERC20Decoder(filterer)

	// Mock client with simulated failure
	mockClient := &mockBlockchainClient{
		latestBlock:      500,
		failGetTokenLogs: true, // SIMULATE RPC FAILURE
	}

	indexerService, _ := NewTokenIndexer(
		mockClient,
		dec,
		db,
		chainID,
		300,
		50,
		5,
		streamID,
	)

	// Attempt batch: must fail
	processed, _, _, _, err := indexerService.ProcessNextBatch(ctx)
	if err == nil {
		t.Fatal("expected error on simulated RPC failure, got nil")
	}
	if processed {
		t.Fatal("processed should be false on failure")
	}

	// Verify checkpoint DID NOT advance past initial state (no checkpoint saved)
	_, found, err := db.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to query checkpoint: %v", err)
	}
	if found {
		t.Fatal("checkpoint must NOT advance or be saved when processing fails")
	}

	// Now recover RPC (simulate RPC restored)
	mockClient.failGetTokenLogs = false

	// Retry processing: should successfully resume from 300
	processed, from, to, _, err := indexerService.ProcessNextBatch(ctx)
	if err != nil {
		t.Fatalf("expected successful recovery after RPC restored: %v", err)
	}
	if !processed || from != 300 || to != 349 {
		t.Fatalf("expected successful range 300->349, got from=%d to=%d", from, to)
	}

	// Verify checkpoint now successfully reached 349
	checkpoint, found, err := db.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("checkpoint query error: %v", err)
	}
	if !found || checkpoint != 349 {
		t.Fatalf("expected checkpoint 349 after recovery, got %d", checkpoint)
	}
}

// TestTokenIndexer_GenericTokenSwitching verifies:
// Run the EXACT same indexer code with:
// TOKEN_ADDRESS=A, TOKEN_ABI_PATH=A.json
// and then:
// TOKEN_ADDRESS=B, TOKEN_ABI_PATH=B.json
// Verify no Go source-code changes are required.
func TestTokenIndexer_GenericTokenSwitching(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tmpDir := t.TempDir()

	// Token A (e.g. Test ERC-20 Token with standard names)
	tokenAAddr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	abiAPath := filepath.Join(tmpDir, "tokenA.json")
	abiAContent := `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`
	if err := os.WriteFile(abiAPath, []byte(abiAContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Token B (e.g. Future WTF Token with underscore names)
	tokenBAddr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	abiBPath := filepath.Join(tmpDir, "tokenB.json")
	abiBContent := `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "_from", "type": "address"},
				{"indexed": true, "name": "_to", "type": "address"},
				{"indexed": false, "name": "_value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`
	if err := os.WriteFile(abiBPath, []byte(abiBContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Function that runs indexer given ONLY config strings (pure configuration, no code changes)
	runIndexerWithConfig := func(tokenAddrStr, abiPathStr, streamID string, blockNum uint64) (*models.TokenTransfer, error) {
		tokenAddr := common.HexToAddress(tokenAddrStr)
		parsedABI, err := indexerABI.LoadERC20ABI(abiPathStr, "")
		if err != nil {
			return nil, err
		}
		filterer, err := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
		if err != nil {
			return nil, err
		}
		dec, err := decoder.NewERC20Decoder(filterer)
		if err != nil {
			return nil, err
		}

		packed, _ := parsedABI.Events["Transfer"].Inputs.NonIndexed().Pack(big.NewInt(42000))
		txHash := common.HexToHash("0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")

		mockClient := &mockBlockchainClient{
			latestBlock: blockNum + 10,
			logs: []types.Log{
				{
					Address: tokenAddr,
					Topics: []common.Hash{
						filterer.TransferTopic(),
						common.BytesToHash(common.HexToAddress("0x1234123412341234123412341234123412341234").Bytes()),
						common.BytesToHash(common.HexToAddress("0x5678567856785678567856785678567856785678").Bytes()),
					},
					Data:        packed,
					BlockNumber: blockNum,
					TxHash:      txHash,
					Index:       0,
				},
			},
			blockTimestamp: 1720000000,
		}

		idx, err := NewTokenIndexer(
			mockClient,
			dec,
			db,
			chainID,
			blockNum,
			10,
			0,
			streamID,
		)
		if err != nil {
			return nil, err
		}

		transfers, err := idx.IndexRange(ctx, blockNum, blockNum)
		if err != nil {
			return nil, err
		}
		if len(transfers) == 0 {
			return nil, fmt.Errorf("no transfers returned")
		}
		return transfers[0], nil
	}

	// 1. Run with Token A
	streamA := fmt.Sprintf("stream_token_A_%d", time.Now().UnixNano())
	transferA, err := runIndexerWithConfig(tokenAAddr.Hex(), abiAPath, streamA, 1000)
	if err != nil {
		t.Fatalf("Token A run failed: %v", err)
	}
	if transferA.Token != tokenAAddr {
		t.Fatalf("expected token %s, got %s", tokenAAddr.Hex(), transferA.Token.Hex())
	}

	// 2. Run with Token B (different address, different ABI file, exact same Go code!)
	streamB := fmt.Sprintf("stream_token_B_%d", time.Now().UnixNano())
	transferB, err := runIndexerWithConfig(tokenBAddr.Hex(), abiBPath, streamB, 2000)
	if err != nil {
		t.Fatalf("Token B run failed: %v", err)
	}
	if transferB.Token != tokenBAddr {
		t.Fatalf("expected token %s, got %s", tokenBAddr.Hex(), transferB.Token.Hex())
	}

	// Verify both tokens are stored cleanly and distinctly in PostgreSQL
	transfersFromDB_A, err := db.GetTokenTransfers(ctx, chainID, tokenAAddr, 10)
	if err != nil || len(transfersFromDB_A) == 0 {
		t.Fatalf("failed to query Token A from DB: %v", err)
	}
	if transfersFromDB_A[0].Token != tokenAAddr {
		t.Fatalf("mismatched Token A in DB: %s", transfersFromDB_A[0].Token.Hex())
	}

	transfersFromDB_B, err := db.GetTokenTransfers(ctx, chainID, tokenBAddr, 10)
	if err != nil || len(transfersFromDB_B) == 0 {
		t.Fatalf("failed to query Token B from DB: %v", err)
	}
	if transfersFromDB_B[0].Token != tokenBAddr {
		t.Fatalf("mismatched Token B in DB: %s", transfersFromDB_B[0].Token.Hex())
	}
}

func TestTokenIndexer_BlockHeaderFailure_CheckpointDoesNotAdvance(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tokenAddr := common.HexToAddress("0xDDDD00000000000000000000000000000000DDDD")
	streamID := fmt.Sprintf("test_stream_header_fail_%d", time.Now().UnixNano())

	parsedABI, err := indexerABI.LoadERC20ABI("", `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`)
	if err != nil {
		t.Fatalf("failed to load ABI: %v", err)
	}

	filterer, _ := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
	dec, _ := decoder.NewERC20Decoder(filterer)

	mockClient := &mockBlockchainClient{
		latestBlock:     200,
		failBlockHeader: true, // Header retrieval fails
		blockTimestamp:  1718000000,
	}

	indexerService, err := NewTokenIndexer(
		mockClient,
		dec,
		db,
		chainID,
		100,
		50,
		5,
		streamID,
	)
	if err != nil {
		t.Fatalf("failed to create token indexer: %v", err)
	}

	// Attempt to index range 100..149: must fail because block header fails
	_, err = indexerService.IndexRange(ctx, 100, 149)
	if err == nil {
		t.Fatal("expected error when block header retrieval fails, got nil")
	}

	// Verify checkpoint was NOT created or advanced
	_, found, err := db.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to query checkpoint: %v", err)
	}
	if found {
		t.Fatal("checkpoint must NOT advance when block header retrieval fails")
	}
}
