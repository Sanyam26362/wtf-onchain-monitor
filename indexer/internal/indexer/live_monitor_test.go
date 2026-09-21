package indexer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/decoder"
)

const monitorTestERC20ABI = `[
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

// TestLiveMonitor_NextBlockCalculation tests requirement 15.1:
// checkpoint -> next block calculation.
func TestLiveMonitor_NextBlockCalculation(t *testing.T) {
	// 1. With existing checkpoint
	next := CalculateNextBlock(11724713, true, 11717931)
	if next != 11724714 {
		t.Fatalf("expected next block 11724714, got %d", next)
	}

	// 2. Without existing checkpoint (fallback to start block)
	next = CalculateNextBlock(0, false, 11717931)
	if next != 11717931 {
		t.Fatalf("expected next block 11717931, got %d", next)
	}
}

// TestLiveMonitor_ConfirmationDepthCalculation tests requirement 15.2:
// confirmation/finality calculation.
func TestLiveMonitor_ConfirmationDepthCalculation(t *testing.T) {
	testCases := []struct {
		name          string
		latestBlock   uint64
		confirmations uint64
		expectedSafe  uint64
	}{
		{"Normal depth", 11724800, 5, 11724795},
		{"Zero confirmations", 11724800, 0, 11724800},
		{"Head block less than confirmations", 4, 5, 0},
		{"Exact equality", 5, 5, 0},
		{"Single confirmation", 100, 1, 99},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			safe := CalculateSafeTarget(tc.latestBlock, tc.confirmations)
			if safe != tc.expectedSafe {
				t.Fatalf("expected safe target %d, got %d", tc.expectedSafe, safe)
			}
		})
	}
}

// TestLiveMonitor_PollingWhenNoNewBlocks tests requirement 15.3:
// polling when no new blocks exist.
func TestLiveMonitor_PollingWhenNoNewBlocks(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()
	mockClient := &mockPayrollClient{latestBlock: 100}

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatal(err)
	}

	// Set checkpoint already at safe block (latestBlock 100 - conf 5 = 95)
	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, chainID, streamID, 95, "")

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5,
		BatchSize:              10,
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        streamID,
		PayrollStartBlock:      50,
	}

	monitor, err := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)
	if err != nil {
		t.Fatal(err)
	}

	// Poll once: should do nothing because fromBlock (96) > safeTarget (95)
	err = monitor.PollOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error during poll: %v", err)
	}

	// Verify checkpoint remained at 95
	cp, found, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if !found || cp != 95 {
		t.Fatalf("checkpoint should remain at 95, got %d", cp)
	}
}

// TestLiveMonitor_ProcessingNewBlocks tests requirement 15.4:
// processing new blocks and discovering events.
func TestLiveMonitor_ProcessingNewBlocks(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	filterer, _ := indexerABI.NewMainFilterer(payrollAddr)
	eeEvent := filterer.ABI().Events["EmployeeAdded"]
	data, _ := eeEvent.Inputs.NonIndexed().Pack(big.NewInt(100), big.NewInt(0))

	employeeAddr := common.HexToAddress("0xEEEE00000000000000000000000000000000EEEE")
	employerAddr := common.HexToAddress("0xAAAA00000000000000000000000000000000AAAA")

	mockClient := &mockPayrollClient{
		latestBlock: 120, // safe target = 120 - 5 = 115
		logs: []types.Log{
			{
				Address: payrollAddr,
				Topics: []common.Hash{
					eeEvent.ID,
					common.BytesToHash(employeeAddr.Bytes()),
					common.BytesToHash(employerAddr.Bytes()),
				},
				Data:        data,
				BlockNumber: 112,
				TxHash:      common.HexToHash("0x1234"),
				Index:       1,
			},
		},
	}

	dec, _ := decoder.New(payrollAddr)
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, chainID, streamID, 110, "")

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5,
		BatchSize:              10,
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        streamID,
		PayrollStartBlock:      100,
	}

	monitor, err := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)
	if err != nil {
		t.Fatal(err)
	}

	// Poll: processes 111..115
	err = monitor.PollOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error during poll: %v", err)
	}

	// Verify checkpoint advanced to 115
	cp, found, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if !found || cp != 115 {
		t.Fatalf("expected checkpoint 115, got %d", cp)
	}

	// Verify event was saved and projected
	if len(mockDB.chainEvents) != 1 {
		t.Fatalf("expected 1 chain event saved, got %d", len(mockDB.chainEvents))
	}
	if len(mockDB.projectedEvents) != 1 {
		t.Fatalf("expected 1 projected event, got %d", len(mockDB.projectedEvents))
	}
}

// TestLiveMonitor_CheckpointAdvancementAfterSuccess tests requirement 15.5:
// checkpoint advances only after successful batch persistence.
func TestLiveMonitor_CheckpointAdvancementAfterSuccess(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()
	mockClient := &mockPayrollClient{latestBlock: 200} // safe target = 195

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, _ := decoder.New(payrollAddr)
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, chainID, streamID, 100, "")

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5,
		BatchSize:              40, // 101..140, 141..180, 181..195
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        streamID,
		PayrollStartBlock:      100,
	}

	monitor, _ := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)
	err := monitor.PollOnce(ctx)
	if err != nil {
		t.Fatalf("poll error: %v", err)
	}

	cp, found, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if !found || cp != 195 {
		t.Fatalf("expected checkpoint 195 after chunked batches, got %d", cp)
	}

	key := fmt.Sprintf("%d:%s", chainID, streamID)
	expectedHash195 := common.HexToHash(fmt.Sprintf("0x%064x", 195)).Hex()
	if mockDB.checkpointHashes[key] != expectedHash195 {
		t.Fatalf("expected checkpoint hash %s, got %s", expectedHash195, mockDB.checkpointHashes[key])
	}
}

func TestLiveMonitor_BlockHeaderFailure_CheckpointDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()
	mockClient := &mockPayrollClient{
		latestBlock:     200,
		failBlockHeader: true, // Header retrieval fails
	}

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, _ := decoder.New(payrollAddr)
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, chainID, streamID, 100, "")

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5,
		BatchSize:              50,
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        streamID,
		PayrollStartBlock:      100,
	}

	monitor, _ := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)
	_, err := monitor.PollPayroll(ctx, 195)
	if err == nil {
		t.Fatal("expected error on block header failure, got nil")
	}

	// Verify checkpoint remains unchanged at 100
	cp, found, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if !found || cp != 100 {
		t.Fatalf("expected checkpoint to stay at 100, got %d (found=%t)", cp, found)
	}
}

// TestLiveMonitor_CheckpointNotAdvancingAfterFailure tests requirement 15.6:
// checkpoint does NOT advance if RPC or persistence fails.
func TestLiveMonitor_CheckpointNotAdvancingAfterFailure(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()
	mockClient := &mockPayrollClient{
		latestBlock: 200,
		failGetLogs: true, // SIMULATE RPC FAILURE
	}

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, _ := decoder.New(payrollAddr)
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, chainID, streamID, 100, "")

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5,
		BatchSize:              50,
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        streamID,
		PayrollStartBlock:      100,
	}

	monitor, _ := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)

	// Poll: should fail
	_ = monitor.PollOnce(ctx)

	// Checkpoint MUST NOT advance from 100
	cp, found, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if !found || cp != 100 {
		t.Fatalf("checkpoint must remain at 100 after RPC failure, got %d", cp)
	}
}

// TestLiveMonitor_RetryAfterRPCFailure tests requirement 15.7:
// resilient retry after RPC outage recovery.
func TestLiveMonitor_RetryAfterRPCFailure(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()
	mockClient := &mockPayrollClient{
		latestBlock: 150,  // safe target 145
		failGetLogs: true, // Initial failure
	}

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, _ := decoder.New(payrollAddr)
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, chainID, streamID, 100, "")

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5,
		BatchSize:              50,
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        streamID,
		PayrollStartBlock:      100,
	}

	monitor, _ := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)

	// 1. First poll cycle fails
	_ = monitor.PollOnce(ctx)
	cp, _, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if cp != 100 {
		t.Fatalf("checkpoint advanced unexpectedly on failure: %d", cp)
	}

	// 2. RPC recovers
	mockClient.failGetLogs = false

	// 3. Second poll cycle succeeds and resumes from 101 to 145
	err := monitor.PollOnce(ctx)
	if err != nil {
		t.Fatalf("expected successful recovery, got: %v", err)
	}

	cp, found, _ := mockDB.GetCheckpoint(ctx, chainID, streamID)
	if !found || cp != 145 {
		t.Fatalf("expected checkpoint 145 after recovery, got %d", cp)
	}
}

// TestLiveMonitor_IdempotentRepeatedProcessing tests requirement 15.8:
// repeated processing of the same block range does not produce duplicate records.
func TestLiveMonitor_IdempotentRepeatedProcessing(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tokenAddr := common.HexToAddress("0xEEEE000000000000000000000000000000001111")
	streamID := fmt.Sprintf("test_idempotent_%d", time.Now().UnixNano())

	parsedABI, _ := indexerABI.LoadERC20ABI("", monitorTestERC20ABI)
	filterer, _ := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
	dec, _ := decoder.NewERC20Decoder(filterer)

	packed, _ := parsedABI.Events["Transfer"].Inputs.NonIndexed().Pack(big.NewInt(500))
	txHash := common.HexToHash("0x9999888877776666555544443333222211110000aaaabbbbccccddddeeeeffff")

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
				Data:        packed,
				BlockNumber: 105,
				TxHash:      txHash,
				Index:       0,
			},
		},
		blockTimestamp: 1725000000,
	}

	ti, err := NewTokenIndexer(
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
		t.Fatal(err)
	}

	// First run: range 100..149
	transfers1, err := ti.IndexRange(ctx, 100, 149)
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if len(transfers1) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(transfers1))
	}

	// Second run over the EXACT same range: should succeed idempotently
	transfers2, err := ti.IndexRange(ctx, 100, 149)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if len(transfers2) != 1 {
		t.Fatalf("expected 1 transfer on replay, got %d", len(transfers2))
	}

	// Verify database contains exactly 1 transfer (0 duplicates!)
	transfersFromDB, err := db.GetTokenTransfers(ctx, chainID, tokenAddr, 10)
	if err != nil {
		t.Fatalf("query DB failed: %v", err)
	}
	if len(transfersFromDB) != 1 {
		t.Fatalf("expected exactly 1 transfer in DB, found %d (duplicate created!)", len(transfersFromDB))
	}
}

// TestLiveMonitor_TokenStreamCheckpointIsolation tests requirement 15.9:
// different token streams track their own independent checkpoints.
func TestLiveMonitor_TokenStreamCheckpointIsolation(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	ctx := context.Background()
	chainID := int64(11155111)

	streamA := fmt.Sprintf("stream_iso_A_%d", time.Now().UnixNano())
	streamB := fmt.Sprintf("stream_iso_B_%d", time.Now().UnixNano())

	// Save checkpoints to different blocks
	err := db.SaveCheckpoint(ctx, chainID, streamA, 1000, "")
	if err != nil {
		t.Fatal(err)
	}
	err = db.SaveCheckpoint(ctx, chainID, streamB, 2000, "")
	if err != nil {
		t.Fatal(err)
	}

	// Verify Stream A is 1000
	cpA, foundA, _ := db.GetCheckpoint(ctx, chainID, streamA)
	if !foundA || cpA != 1000 {
		t.Fatalf("expected Stream A checkpoint 1000, got %d", cpA)
	}

	// Verify Stream B is 2000
	cpB, foundB, _ := db.GetCheckpoint(ctx, chainID, streamB)
	if !foundB || cpB != 2000 {
		t.Fatalf("expected Stream B checkpoint 2000, got %d", cpB)
	}

	// Advance Stream A only to 1050
	_ = db.SaveCheckpoint(ctx, chainID, streamA, 1050, "")
	cpA, _, _ = db.GetCheckpoint(ctx, chainID, streamA)
	cpB, _, _ = db.GetCheckpoint(ctx, chainID, streamB)

	if cpA != 1050 {
		t.Fatalf("expected Stream A at 1050, got %d", cpA)
	}
	if cpB != 2000 {
		t.Fatalf("Stream B should remain at 2000, got %d", cpB)
	}
}

// TestLiveMonitor_PayrollAndTokenStreamsIndependent tests requirement 15.10:
// payroll and token streams operate independently without cross-corruption.
func TestLiveMonitor_PayrollAndTokenStreamsIndependent(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()

	payrollStream := "monthly_payroll"
	tokenStream := "erc20_transfers_tokenX"

	// Initial checkpoints
	_ = mockDB.SaveCheckpoint(ctx, chainID, payrollStream, 11080850, "")
	_ = mockDB.SaveCheckpoint(ctx, chainID, tokenStream, 11724713, "")

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, _ := decoder.New(payrollAddr)

	// Simulated client where payroll logs fail, but token indexing succeeds
	mockClient := &mockPayrollClient{
		latestBlock: 11724720,
		failGetLogs: true, // Payroll fails
	}
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	cfg := LiveMonitorConfig{
		ChainID:                chainID,
		ConfirmationDepth:      5, // safeTarget = 11724715
		BatchSize:              50,
		PollInterval:           50 * time.Millisecond,
		PayrollContractAddress: payrollAddr,
		PayrollStreamID:        payrollStream,
		PayrollStartBlock:      11080692,
	}

	monitor, err := NewLiveMonitor(cfg, mockClient, svc, nil, mockDB)
	if err != nil {
		t.Fatal(err)
	}

	// Poll: payroll stream fails, but token stream is unaffected
	_ = monitor.PollOnce(ctx)

	// Payroll checkpoint remains at 11080850
	cpPayroll, _, _ := mockDB.GetCheckpoint(ctx, chainID, payrollStream)
	if cpPayroll != 11080850 {
		t.Fatalf("payroll checkpoint corrupted: %d", cpPayroll)
	}

	// Token checkpoint remains untouched at 11724713
	cpToken, _, _ := mockDB.GetCheckpoint(ctx, chainID, tokenStream)
	if cpToken != 11724713 {
		t.Fatalf("token checkpoint corrupted: %d", cpToken)
	}
}

// TestLiveMonitor_GracefulShutdown tests requirement 15.11:
// monitor stops cleanly when context receives shutdown signal.
func TestLiveMonitor_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()
	mockClient := &mockPayrollClient{latestBlock: 100}

	cfg := LiveMonitorConfig{
		ChainID:           chainID,
		ConfirmationDepth: 5,
		BatchSize:         10,
		PollInterval:      20 * time.Millisecond,
	}

	monitor, err := NewLiveMonitor(cfg, mockClient, nil, nil, mockDB)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	var runErr error
	go func() {
		defer wg.Done()
		runErr = monitor.Start(ctx)
	}()

	// Let the monitor start and run for a few milliseconds
	time.Sleep(50 * time.Millisecond)

	// Cancel context (simulate SIGINT / SIGTERM)
	cancel()

	// Wait for monitor to exit
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if runErr != nil {
			t.Fatalf("expected nil error on clean shutdown, got: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not shut down gracefully within timeout")
	}
}

// TestLiveMonitor_RemovedReorgHandling tests requirement 15.12:
// removed logs from reorg are marked as removed and not projected into domain tables.
func TestLiveMonitor_RemovedReorgHandling(t *testing.T) {
	ctx := context.Background()
	chainID := int64(11155111)
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	filterer, _ := indexerABI.NewMainFilterer(payrollAddr)
	eeEvent := filterer.ABI().Events["EmployeeAdded"]
	data, _ := eeEvent.Inputs.NonIndexed().Pack(big.NewInt(100), big.NewInt(0))

	employeeAddr := common.HexToAddress("0xEEEE00000000000000000000000000000000EEEE")
	employerAddr := common.HexToAddress("0xAAAA00000000000000000000000000000000AAAA")

	// Create a log with Removed = true (reorg)
	removedLog := types.Log{
		Address: payrollAddr,
		Topics: []common.Hash{
			eeEvent.ID,
			common.BytesToHash(employeeAddr.Bytes()),
			common.BytesToHash(employerAddr.Bytes()),
		},
		Data:        data,
		BlockNumber: 115,
		TxHash:      common.HexToHash("0xreorgedtx"),
		Index:       0,
		Removed:     true, // REORG REMOVED
	}

	mockClient := &mockPayrollClient{
		latestBlock: 130,
		logs:        []types.Log{removedLog},
	}

	dec, _ := decoder.New(payrollAddr)
	svc, _ := NewWithDecoder(mockClient, dec, mockDB)

	events, err := svc.IndexRange(ctx, chainID, payrollAddr, 115, 115)
	if err != nil {
		t.Fatalf("IndexRange failed: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event returned, got %d", len(events))
	}

	// Verify chain event was saved (preserving removed provenance)
	if len(mockDB.chainEvents) != 1 {
		t.Fatalf("expected chain event to be saved in chain_events table")
	}
	if !mockDB.chainEvents[0].Log.Removed {
		t.Fatal("expected saved event Log.Removed to be true")
	}

	// Verify event was NOT projected into domain tables!
	if len(mockDB.projectedEvents) != 0 {
		t.Fatalf("removed event must NOT be projected into domain tables, but got %d projections", len(mockDB.projectedEvents))
	}
}
