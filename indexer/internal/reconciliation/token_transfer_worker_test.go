package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// --- Mock for TokenTransferStore ---

type mockTokenTransferStore struct {
	mu        sync.Mutex
	transfers []*models.TokenTransfer
	queryErr  error
}

func (m *mockTokenTransferStore) GetTransfersByBlockRange(ctx context.Context, chainID int64, token common.Address, fromBlock, toBlock uint64) ([]*models.TokenTransfer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var res []*models.TokenTransfer
	for _, tr := range m.transfers {
		if tr.ChainID == chainID && strings.EqualFold(tr.Token.Hex(), token.Hex()) && tr.BlockNumber >= fromBlock && tr.BlockNumber <= toBlock {
			res = append(res, tr)
		}
	}
	return res, nil
}

// --- Mock for ERC20TransferDecoder ---

type mockERC20TransferDecoder struct {
	tokenAddress  common.Address
	transferTopic common.Hash
	transfers     map[string]*models.TokenTransfer
	decodeErr     error
}

func (m *mockERC20TransferDecoder) TokenAddress() common.Address {
	return m.tokenAddress
}

func (m *mockERC20TransferDecoder) TransferTopic() common.Hash {
	return m.transferTopic
}

func (m *mockERC20TransferDecoder) DecodeTransfer(chainID int64, log types.Log, blockTimestamp time.Time) (*models.TokenTransfer, error) {
	if m.decodeErr != nil {
		return nil, m.decodeErr
	}
	key := eventKey(log.TxHash, log.Index)
	if tr, ok := m.transfers[key]; ok {
		return tr, nil
	}
	return nil, errors.New("transfer not found in mock decoder")
}

var (
	testTokenAddress = common.HexToAddress("0x378AFb93CaDd39AFF154704d2D90Af8c401137E7")
	testTransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	testTokenStreamID = "erc20_transfers_0x378afb93cadd39aff154704d2d90af8c401137e7"
	testReconStreamID = "reconciliation_erc20_transfers_0x378afb93cadd39aff154704d2d90af8c401137e7"
)

// Helper to create test setup for token transfer reconciler
func setupTestTokenTransferReconciler(
	latestBlock uint64,
	confirmationDepth uint64,
	indexerCheckpoint uint64,
) (*TokenTransferReconciler, *mockBlockchainClient, *mockERC20TransferDecoder, *mockTokenTransferStore, *mockReconciliationStore, *mockCheckpointStore) {
	client := &mockBlockchainClient{latestBlock: latestBlock}
	eventDecoder := &mockERC20TransferDecoder{
		tokenAddress:  testTokenAddress,
		transferTopic: testTransferTopic,
		transfers:     make(map[string]*models.TokenTransfer),
	}
	transferStore := &mockTokenTransferStore{}
	reconStore := newMockReconciliationStore()
	checkpointStore := newMockCheckpointStore()

	if indexerCheckpoint > 0 {
		_ = checkpointStore.SaveCheckpoint(context.Background(), 11155111, testTokenStreamID, indexerCheckpoint, "")
	}

	cfg := TokenTransferReconcilerConfig{
		ChainID:           11155111,
		TokenAddress:      testTokenAddress,
		TokenStreamID:     testTokenStreamID,
		ReconStreamID:     testReconStreamID,
		ConfirmationDepth: confirmationDepth,
		StartBlock:        11717931,
	}

	rec, _ := NewTokenTransferReconciler(cfg, client, eventDecoder, transferStore, reconStore, checkpointStore)
	rec.SetRetryPolicy(indexer.RetryPolicy{
		MaxRetries:     1,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		BackoffFactor:  1.5,
		Sleeper: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	})

	return rec, client, eventDecoder, transferStore, reconStore, checkpointStore
}

// 1. MatchingTransfer_NoException: On-chain and database records match -> 0 exceptions
func TestReconcileTokenTransfer_MatchingTransfer_NoException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	fromAddr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	toAddr := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	amount, _ := new(big.Int).SetString("1000000000000000000000", 10) // 1000 tokens (uint256 > int64)

	// Blockchain log
	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11718000,
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: 11718000,
		LogIndex:    0,
	}

	// Matching database record
	tStore.transfers = []*models.TokenTransfer{
		{
			ID:          1,
			ChainID:     11155111,
			Token:       testTokenAddress,
			FromAddress: fromAddr,
			ToAddress:   toAddr,
			Amount:      new(big.Int).Set(amount),
			TxHash:      txHash,
			BlockNumber: 11718000,
			LogIndex:    0,
			Removed:     false,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11717931, 11718100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions created, got %d", res.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 0 {
		t.Errorf("expected empty exception store, got %d", len(rStore.exceptions))
	}
}

// 2. MissingTransfer_CreatesException: On-chain finalized transfer exists, DB record missing -> TOKEN_TRANSFER_MISSING
func TestReconcileTokenTransfer_MissingTransfer_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, _, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	fromAddr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	toAddr := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	amount, _ := new(big.Int).SetString("500000000000000000000", 10)

	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       2,
			BlockNumber: 11718050,
		},
	}
	decoderMock.transfers[eventKey(txHash, 2)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: 11718050,
		LogIndex:    2,
	}

	res, err := rec.ReconcileRange(ctx, 11717931, 11718100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Errorf("expected 1 mismatch, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 1 {
		t.Errorf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}

	exc := rStore.exceptions[1]
	if exc.Type != TypeTokenTransferMissing {
		t.Errorf("expected type %s, got %s", TypeTokenTransferMissing, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
	expectedRef := fmt.Sprintf("11155111:%s:%s:2:missing", strings.ToLower(testTokenAddress.Hex()), txHash.Hex())
	if exc.EntityRef != expectedRef {
		t.Errorf("expected entity_ref %s, got %s", expectedRef, exc.EntityRef)
	}
}

// 3. IncorrectAmount_CreatesException: Database amount differs -> TOKEN_TRANSFER_AMOUNT_MISMATCH
func TestReconcileTokenTransfer_IncorrectAmount_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
	fromAddr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	toAddr := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	onChainAmount, _ := new(big.Int).SetString("1000000000000000000000", 10)
	dbAmount, _ := new(big.Int).SetString("999000000000000000000", 10)

	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       1,
			BlockNumber: 11718060,
		},
	}
	decoderMock.transfers[eventKey(txHash, 1)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      onChainAmount,
		TxHash:      txHash,
		BlockNumber: 11718060,
		LogIndex:    1,
	}

	tStore.transfers = []*models.TokenTransfer{
		{
			ID:          1,
			ChainID:     11155111,
			Token:       testTokenAddress,
			FromAddress: fromAddr,
			ToAddress:   toAddr,
			Amount:      dbAmount,
			TxHash:      txHash,
			BlockNumber: 11718060,
			LogIndex:    1,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11717931, 11718100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 || res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 mismatch and 1 exception, got %d mismatches, %d exceptions",
			res.MismatchesDetected, res.ExceptionsCreated)
	}

	exc := rStore.exceptions[1]
	if exc.Type != TypeTokenTransferAmountMismatch {
		t.Errorf("expected type %s, got %s", TypeTokenTransferAmountMismatch, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// 4. EntityMismatch_CreatesException: from or to differs -> TOKEN_TRANSFER_ENTITY_MISMATCH with critical severity
func TestReconcileTokenTransfer_EntityMismatch_CreatesException(t *testing.T) {
	ctx := context.Background()

	t.Run("FromAddress Mismatch", func(t *testing.T) {
		rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)
		txHash := common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444")
		onChainFrom := common.HexToAddress("0x1111111111111111111111111111111111111111")
		dbFrom := common.HexToAddress("0x2222222222222222222222222222222222222222")
		toAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
		amount := big.NewInt(100)

		client.logs = []types.Log{{Address: testTokenAddress, Topics: []common.Hash{testTransferTopic}, TxHash: txHash, Index: 0, BlockNumber: 11718010}}
		decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
			ChainID: 11155111, Token: testTokenAddress, FromAddress: onChainFrom, ToAddress: toAddr, Amount: amount, TxHash: txHash, BlockNumber: 11718010, LogIndex: 0,
		}
		tStore.transfers = []*models.TokenTransfer{
			{ID: 1, ChainID: 11155111, Token: testTokenAddress, FromAddress: dbFrom, ToAddress: toAddr, Amount: amount, TxHash: txHash, BlockNumber: 11718010, LogIndex: 0},
		}

		res, err := rec.ReconcileRange(ctx, 11718000, 11718020)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.ExceptionsCreated != 1 {
			t.Fatalf("expected 1 exception, got %d", res.ExceptionsCreated)
		}
		exc := rStore.exceptions[1]
		if exc.Type != TypeTokenTransferEntityMismatch {
			t.Errorf("expected %s, got %s", TypeTokenTransferEntityMismatch, exc.Type)
		}
		if exc.Severity != SeverityCritical {
			t.Errorf("expected severity %s, got %s", SeverityCritical, exc.Severity)
		}
	})

	t.Run("ToAddress Mismatch", func(t *testing.T) {
		rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)
		txHash := common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555")
		fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
		onChainTo := common.HexToAddress("0x3333333333333333333333333333333333333333")
		dbTo := common.HexToAddress("0x4444444444444444444444444444444444444444")
		amount := big.NewInt(100)

		client.logs = []types.Log{{Address: testTokenAddress, Topics: []common.Hash{testTransferTopic}, TxHash: txHash, Index: 0, BlockNumber: 11718010}}
		decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
			ChainID: 11155111, Token: testTokenAddress, FromAddress: fromAddr, ToAddress: onChainTo, Amount: amount, TxHash: txHash, BlockNumber: 11718010, LogIndex: 0,
		}
		tStore.transfers = []*models.TokenTransfer{
			{ID: 1, ChainID: 11155111, Token: testTokenAddress, FromAddress: fromAddr, ToAddress: dbTo, Amount: amount, TxHash: txHash, BlockNumber: 11718010, LogIndex: 0},
		}

		res, err := rec.ReconcileRange(ctx, 11718000, 11718020)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.ExceptionsCreated != 1 {
			t.Fatalf("expected 1 exception, got %d", res.ExceptionsCreated)
		}
		exc := rStore.exceptions[1]
		if exc.Type != TypeTokenTransferEntityMismatch {
			t.Errorf("expected %s, got %s", TypeTokenTransferEntityMismatch, exc.Type)
		}
		if exc.Severity != SeverityCritical {
			t.Errorf("expected severity %s, got %s", SeverityCritical, exc.Severity)
		}
	})
}

// 5. DuplicateRecords_CreatesException: Multiple DB rows represent the same transfer -> TOKEN_TRANSFER_DUPLICATE
func TestReconcileTokenTransfer_DuplicateRecords_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0x6666666666666666666666666666666666666666666666666666666666666666")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	amount := big.NewInt(500)

	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11718020,
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: 11718020,
		LogIndex:    0,
	}

	// Two database records for the same event!
	tStore.transfers = []*models.TokenTransfer{
		{ID: 10, ChainID: 11155111, Token: testTokenAddress, FromAddress: fromAddr, ToAddress: toAddr, Amount: amount, TxHash: txHash, BlockNumber: 11718020, LogIndex: 0},
		{ID: 11, ChainID: 11155111, Token: testTokenAddress, FromAddress: fromAddr, ToAddress: toAddr, Amount: amount, TxHash: txHash, BlockNumber: 11718020, LogIndex: 0},
	}

	res, err := rec.ReconcileRange(ctx, 11718000, 11718050)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 || res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 duplicate mismatch and 1 exception, got %d / %d", res.MismatchesDetected, res.ExceptionsCreated)
	}

	exc := rStore.exceptions[1]
	if exc.Type != TypeTokenTransferDuplicate {
		t.Errorf("expected type %s, got %s", TypeTokenTransferDuplicate, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// 6. RunningTwice_Idempotent: Run reconciliation twice -> no duplicate exception rows
func TestReconcileTokenTransfer_RunningTwice_Idempotent(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, _, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0x7777777777777777777777777777777777777777777777777777777777777777")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	amount := big.NewInt(100)

	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11718030,
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: 11718030,
		LogIndex:    0,
	}

	// Pass 1: Creates exception
	res1, err := rec.ReconcileRange(ctx, 11718000, 11718050)
	if err != nil {
		t.Fatalf("unexpected error in pass 1: %v", err)
	}
	if res1.ExceptionsCreated != 1 || res1.ExceptionsSkipped != 0 {
		t.Fatalf("expected 1 created and 0 skipped, got created=%d, skipped=%d", res1.ExceptionsCreated, res1.ExceptionsSkipped)
	}

	// Pass 2: Identical run should skip already open exception
	res2, err := rec.ReconcileRange(ctx, 11718000, 11718050)
	if err != nil {
		t.Fatalf("unexpected error in pass 2: %v", err)
	}
	if res2.ExceptionsCreated != 0 || res2.ExceptionsSkipped != 1 {
		t.Fatalf("expected 0 created and 1 skipped, got created=%d, skipped=%d", res2.ExceptionsCreated, res2.ExceptionsSkipped)
	}

	if len(rStore.exceptions) != 1 {
		t.Errorf("expected 1 total exception in store, got %d", len(rStore.exceptions))
	}
}

// 7. MismatchCorrected_ExceptionResolved: First run creates exception, correct DB record, run again -> resolved
func TestReconcileTokenTransfer_MismatchCorrected_ExceptionResolved(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0x8888888888888888888888888888888888888888888888888888888888888888")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	amount := big.NewInt(100)

	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11718030,
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: 11718030,
		LogIndex:    0,
	}

	// Pass 1: Missing transfer creates exception
	res1, err := rec.ReconcileRange(ctx, 11718000, 11718050)
	if err != nil {
		t.Fatalf("unexpected error in pass 1: %v", err)
	}
	if res1.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 exception created, got %d", res1.ExceptionsCreated)
	}

	// Fix DB: Insert the missing record
	tStore.transfers = []*models.TokenTransfer{
		{
			ID:          1,
			ChainID:     11155111,
			Token:       testTokenAddress,
			FromAddress: fromAddr,
			ToAddress:   toAddr,
			Amount:      new(big.Int).Set(amount),
			TxHash:      txHash,
			BlockNumber: 11718030,
			LogIndex:    0,
		},
	}

	// Pass 2: Re-run should resolve the exception
	res2, err := rec.ReconcileRange(ctx, 11718000, 11718050)
	if err != nil {
		t.Fatalf("unexpected error in pass 2: %v", err)
	}
	if res2.ExceptionsResolved != 1 {
		t.Errorf("expected 1 exception resolved, got %d", res2.ExceptionsResolved)
	}

	exc := rStore.exceptions[1]
	if exc.Status != StatusResolved {
		t.Errorf("expected exception status %s, got %s", StatusResolved, exc.Status)
	}
	if exc.ResolvedAt == nil {
		t.Error("expected non-nil ResolvedAt timestamp")
	}
}

// 8. SystemFailure_ReturnsErrorWithoutExceptions: Simulate RPC/DB failures -> error returned, 0 exceptions, checkpoint unchanged
func TestReconcileTokenTransfer_SystemFailure_ReturnsErrorWithoutExceptions(t *testing.T) {
	ctx := context.Background()

	t.Run("RPC_GetLogs_Error", func(t *testing.T) {
		rec, client, _, _, rStore, cpStore := setupTestTokenTransferReconciler(11718500, 5, 11718495)
		client.getLogsErr = errors.New("simulated RPC timeout 504")

		_, err := rec.ReconcileRange(ctx, 11718000, 11718050)
		if err == nil {
			t.Fatal("expected error on RPC failure, got nil")
		}
		if len(rStore.exceptions) != 0 {
			t.Errorf("expected 0 exceptions on system error, got %d", len(rStore.exceptions))
		}
		_, ok, _ := cpStore.GetCheckpoint(ctx, 11155111, testReconStreamID)
		if ok {
			t.Error("expected checkpoint not to be created/advanced on error")
		}
	})

	t.Run("RPC_LatestBlock_Error", func(t *testing.T) {
		rec, client, _, _, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)
		client.latestErr = errors.New("simulated network connection reset")

		_, err := rec.ReconcileRange(ctx, 11718000, 11718050)
		if err == nil {
			t.Fatal("expected error on latest block RPC failure, got nil")
		}
		if len(rStore.exceptions) != 0 {
			t.Errorf("expected 0 exceptions on system error, got %d", len(rStore.exceptions))
		}
	})

	t.Run("Database_Query_Error", func(t *testing.T) {
		rec, _, _, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)
		tStore.queryErr = errors.New("simulated database connection dropped")

		_, err := rec.ReconcileRange(ctx, 11718000, 11718050)
		if err == nil {
			t.Fatal("expected error on database query failure, got nil")
		}
		if len(rStore.exceptions) != 0 {
			t.Errorf("expected 0 exceptions on system error, got %d", len(rStore.exceptions))
		}
	})
}

// 9. UnfinalizedBlock_Ignored: Transfer occurs above safeTarget -> ignored
func TestReconcileTokenTransfer_UnfinalizedBlock_Ignored(t *testing.T) {
	ctx := context.Background()
	// latestBlock = 11718000, confirmationDepth = 5 -> safeTarget = 11717995
	rec, client, decoderMock, _, rStore, _ := setupTestTokenTransferReconciler(11718000, 5, 11718000)

	txHash := common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999")
	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11717998, // Unfinalized block (> 11717995)
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		ToAddress:   common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Amount:      big.NewInt(100),
		TxHash:      txHash,
		BlockNumber: 11717998,
		LogIndex:    0,
	}

	res, err := rec.ReconcileRange(ctx, 11717990, 11718000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ToBlock > 11717995 {
		t.Errorf("expected effective toBlock <= safeTarget (11717995), got %d", res.ToBlock)
	}
	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions for unfinalized event, got %d", res.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions in store, got %d", len(rStore.exceptions))
	}
}

// 10. IndexingLag_NotReportedAsMissing: Transfer block > token indexer checkpoint -> no TOKEN_TRANSFER_MISSING
func TestReconcileTokenTransfer_IndexingLag_NotReportedAsMissing(t *testing.T) {
	ctx := context.Background()
	// safeTarget = 11718000, but token indexer checkpoint is only 11717950 (lagging behind)
	rec, client, decoderMock, _, rStore, _ := setupTestTokenTransferReconciler(11718005, 5, 11717950)

	txHash := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11717960, // Above indexer checkpoint (11717950), but below safeTarget (11718000)
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		ToAddress:   common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Amount:      big.NewInt(100),
		TxHash:      txHash,
		BlockNumber: 11717960,
		LogIndex:    0,
	}

	res, err := rec.ReconcileRange(ctx, 11717931, 11718000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions due to indexing lag protection, got %d", res.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 0 {
		t.Errorf("expected 0 stored exceptions, got %d", len(rStore.exceptions))
	}
}

// 11. TransactionInconsistency_CreatesException: Database block metadata differs -> TOKEN_TRANSFER_TX_INCONSISTENCY
func TestReconcileTokenTransfer_TransactionInconsistency_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	txHash := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	amount := big.NewInt(100)

	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11718020,
		},
	}
	decoderMock.transfers[eventKey(txHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      txHash,
		BlockNumber: 11718020,
		LogIndex:    0,
	}

	// DB record has incorrect block number!
	tStore.transfers = []*models.TokenTransfer{
		{
			ID:          1,
			ChainID:     11155111,
			Token:       testTokenAddress,
			FromAddress: fromAddr,
			ToAddress:   toAddr,
			Amount:      amount,
			TxHash:      txHash,
			BlockNumber: 11718021, // Inconsistent block number!
			LogIndex:    0,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11718000, 11718050)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 || res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 mismatch and 1 exception, got %d / %d", res.MismatchesDetected, res.ExceptionsCreated)
	}

	exc := rStore.exceptions[1]
	if exc.Type != TypeTokenTransferTxInconsistent {
		t.Errorf("expected type %s, got %s", TypeTokenTransferTxInconsistent, exc.Type)
	}
	if exc.Severity != SeverityMedium {
		t.Errorf("expected severity %s, got %s", SeverityMedium, exc.Severity)
	}
}

// 12. RecentWindowAndNextBatch: Verify safeTarget, window calculation, checkpoint advancement & stream isolation
func TestReconcileTokenTransfer_RecentWindowAndNextBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("ReconcileRecentWindow", func(t *testing.T) {
		rec, client, _, _, _, _ := setupTestTokenTransferReconciler(11719000, 5, 11718995)
		client.latestBlock = 11719000 // safeTarget = 11718995

		res, err := rec.ReconcileRecentWindow(ctx, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expectedFrom := uint64(11718995 - 100 + 1)
		if res.FromBlock != expectedFrom {
			t.Errorf("expected fromBlock %d, got %d", expectedFrom, res.FromBlock)
		}
		if res.ToBlock != 11718995 {
			t.Errorf("expected toBlock 11718995, got %d", res.ToBlock)
		}
	})

	t.Run("ReconcileNextBatch Advances Dedicated Checkpoint", func(t *testing.T) {
		rec, client, _, _, _, cpStore := setupTestTokenTransferReconciler(11719000, 5, 11718995)
		client.latestBlock = 11718500 // safeTarget = 11718495

		// First batch: from StartBlock 11717931, batchSize 50 -> toBlock 11717980
		res, err := rec.ReconcileNextBatch(ctx, 50)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.FromBlock != 11717931 {
			t.Errorf("expected fromBlock 11717931, got %d", res.FromBlock)
		}
		if res.ToBlock != 11717980 {
			t.Errorf("expected toBlock 11717980, got %d", res.ToBlock)
		}

		// Verify checkpoint was advanced in dedicated token stream
		cp, ok, err := cpStore.GetCheckpoint(ctx, 11155111, testReconStreamID)
		if err != nil || !ok {
			t.Fatalf("expected checkpoint to exist for %s, ok=%v, err=%v", testReconStreamID, ok, err)
		}
		if cp != 11717980 {
			t.Errorf("expected checkpoint 11717980, got %d", cp)
		}

		// Second batch resumes from checkpoint + 1 = 11717981
		res2, err := rec.ReconcileNextBatch(ctx, 50)
		if err != nil {
			t.Fatalf("unexpected error in second batch: %v", err)
		}
		if res2.FromBlock != 11717981 {
			t.Errorf("expected second batch fromBlock 11717981, got %d", res2.FromBlock)
		}
		if res2.ToBlock != 11718030 {
			t.Errorf("expected second batch toBlock 11718030, got %d", res2.ToBlock)
		}

		// Verify checkpoint is now 11718030
		cp2, _, _ := cpStore.GetCheckpoint(ctx, 11155111, testReconStreamID)
		if cp2 != 11718030 {
			t.Errorf("expected checkpoint 11718030, got %d", cp2)
		}
	})
}

// 13. TokenIsolation: Transfers from another ERC-20 contract are NOT considered part of WTF token reconciliation
func TestReconcileTokenTransfer_TokenIsolation(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, tStore, rStore, _ := setupTestTokenTransferReconciler(11718500, 5, 11718495)

	wtfTxHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	otherTxHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	otherTokenAddress := common.HexToAddress("0x9999999999999999999999999999999999999999")

	fromAddr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	toAddr := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	amount := big.NewInt(100)

	// Blockchain logs return 2 logs:
	// 1. One from the configured WTF token contract (matches DB)
	// 2. One from a DIFFERENT token contract (missing from DB)
	client.logs = []types.Log{
		{
			Address:     testTokenAddress,
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      wtfTxHash,
			Index:       0,
			BlockNumber: 11718010,
		},
		{
			Address:     otherTokenAddress, // Different token!
			Topics:      []common.Hash{testTransferTopic},
			TxHash:      otherTxHash,
			Index:       0,
			BlockNumber: 11718010,
		},
	}

	decoderMock.transfers[eventKey(wtfTxHash, 0)] = &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testTokenAddress,
		FromAddress: fromAddr,
		ToAddress:   toAddr,
		Amount:      amount,
		TxHash:      wtfTxHash,
		BlockNumber: 11718010,
		LogIndex:    0,
	}

	// DB only has the WTF token transfer
	tStore.transfers = []*models.TokenTransfer{
		{
			ID:          1,
			ChainID:     11155111,
			Token:       testTokenAddress,
			FromAddress: fromAddr,
			ToAddress:   toAddr,
			Amount:      new(big.Int).Set(amount),
			TxHash:      wtfTxHash,
			BlockNumber: 11718010,
			LogIndex:    0,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11718000, 11718020)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The log from the other token MUST be ignored and NOT reported as missing!
	if res.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches due to token isolation, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions created, got %d", res.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 0 {
		t.Errorf("expected 0 stored exceptions, got %d", len(rStore.exceptions))
	}
}
