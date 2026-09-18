package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// --- Mock for SalaryClaimStore ---

type mockSalaryClaimStore struct {
	mu       sync.Mutex
	claims   []*models.SalaryClaim
	queryErr error
}

func (m *mockSalaryClaimStore) GetClaimsByBlockRange(ctx context.Context, chainID int64, fromBlock, toBlock uint64) ([]*models.SalaryClaim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var res []*models.SalaryClaim
	for _, c := range m.claims {
		if c.ChainID == chainID && c.BlockNumber >= fromBlock && c.BlockNumber <= toBlock {
			res = append(res, c)
		}
	}
	return res, nil
}

// Helper to create test setup for salary claim reconciler
func setupTestSalaryClaimReconciler(
	latestBlock uint64,
	confirmationDepth uint64,
	indexerCheckpoint uint64,
) (*SalaryClaimReconciler, *mockBlockchainClient, *mockEventDecoder, *mockSalaryClaimStore, *mockReconciliationStore, *mockCheckpointStore) {
	client := &mockBlockchainClient{latestBlock: latestBlock}
	eventDecoder := &mockEventDecoder{events: make(map[string]*decoder.DecodedEvent)}
	claimStore := &mockSalaryClaimStore{}
	reconStore := newMockReconciliationStore()
	checkpointStore := newMockCheckpointStore()

	if indexerCheckpoint > 0 {
		_ = checkpointStore.SaveCheckpoint(context.Background(), 11155111, "monthly_payroll", indexerCheckpoint, "")
	}

	cfg := SalaryClaimReconcilerConfig{
		ChainID:           11155111,
		ContractAddress:   common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC"),
		PayrollStreamID:   "monthly_payroll",
		ReconStreamID:     "reconciliation_salary_claim",
		ConfirmationDepth: confirmationDepth,
		StartBlock:        11080692,
	}

	rec, _ := NewSalaryClaimReconciler(cfg, client, eventDecoder, claimStore, reconStore, checkpointStore)
	rec.SetRetryPolicy(indexer.RetryPolicy{
		MaxRetries:     1,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		BackoffFactor:  1.5,
		Sleeper: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	})

	return rec, client, eventDecoder, claimStore, reconStore, checkpointStore
}

// 1. MatchingSalaryClaim_NoException
func TestReconcileSalaryClaim_MatchingSalaryClaim_NoException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	// Blockchain log
	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080700,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(500),
		},
	}

	// Database record matching exactly
	cStore.claims = []*models.SalaryClaim{
		{
			ID:             1,
			ChainID:        11155111,
			Employee:       employee,
			Amount:         big.NewInt(500),
			TxHash:         txHash,
			BlockNumber:    11080700,
			LogIndex:       0,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11080692, 11080750)
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
		t.Errorf("expected empty reconciliation store, got %d records", len(rStore.exceptions))
	}
}

// 2. MissingSalaryClaim_CreatesException
func TestReconcileSalaryClaim_MissingSalaryClaim_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, _, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xaaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       2,
			BlockNumber: 11080750,
		},
	}
	decoderMock.events[eventKey(txHash, 2)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(750),
		},
	}

	// Database has NO records!
	res, err := rec.ReconcileRange(ctx, 11080700, 11080800)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Fatalf("expected 1 mismatch, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}

	expectedRef := fmt.Sprintf("11155111:%s:2:missing", txHash.Hex())
	exc, err := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if err != nil {
		t.Fatalf("error checking exception: %v", err)
	}
	if exc == nil {
		t.Fatalf("expected open exception for %s", expectedRef)
	}

	if exc.Type != TypeSalaryClaimMissing {
		t.Errorf("expected type %s, got %s", TypeSalaryClaimMissing, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
	if exc.Status != StatusOpen {
		t.Errorf("expected status %s, got %s", StatusOpen, exc.Status)
	}
}

// 3. IncorrectAmount_CreatesException
func TestReconcileSalaryClaim_IncorrectAmount_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xbbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       1,
			BlockNumber: 11080720,
		},
	}
	decoderMock.events[eventKey(txHash, 1)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(1200), // On chain: 1200
		},
	}

	// Database record has different amount!
	cStore.claims = []*models.SalaryClaim{
		{
			ID:             10,
			ChainID:        11155111,
			Employee:       employee,
			Amount:         big.NewInt(1000), // DB has 1000!
			TxHash:         txHash,
			BlockNumber:    11080720,
			LogIndex:       1,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Fatalf("expected 1 mismatch, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}

	expectedRef := fmt.Sprintf("11155111:%s:1:amount_mismatch", txHash.Hex())
	exc, err := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if err != nil || exc == nil {
		t.Fatalf("expected exception for %s, err: %v", expectedRef, err)
	}
	if exc.Type != TypeSalaryClaimAmountMismatch {
		t.Errorf("expected type %s, got %s", TypeSalaryClaimAmountMismatch, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// 4. EntityMismatch_CreatesException
func TestReconcileSalaryClaim_EntityMismatch_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0x7777888877778888777788887777888877778888777788887777888877778888")
	employeeOnChain := common.HexToAddress("0x1111111111111111111111111111111111111111")
	employeeWrong := common.HexToAddress("0x9999999999999999999999999999999999999999")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080715,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employeeOnChain,
			Amount:   big.NewInt(300),
		},
	}

	cStore.claims = []*models.SalaryClaim{
		{
			ID:          300,
			ChainID:     11155111,
			Employee:    employeeWrong, // Wrong employee address in DB!
			Amount:      big.NewInt(300),
			TxHash:      txHash,
			BlockNumber: 11080715,
			LogIndex:    0,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}

	expectedRef := fmt.Sprintf("11155111:%s:0:entity_mismatch", txHash.Hex())
	exc, _ := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if exc == nil {
		t.Fatalf("expected exception for %s", expectedRef)
	}
	if exc.Type != TypeSalaryClaimEntityMismatch {
		t.Errorf("expected type %s, got %s", TypeSalaryClaimEntityMismatch, exc.Type)
	}
	if exc.Severity != SeverityCritical {
		t.Errorf("expected severity %s, got %s", SeverityCritical, exc.Severity)
	}
}

// 5. DuplicateRecords_CreatesException
func TestReconcileSalaryClaim_DuplicateRecords_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xcccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080710,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(600),
		},
	}

	// 2 database records for the SAME (tx_hash, log_index)!
	cStore.claims = []*models.SalaryClaim{
		{
			ID:          101,
			ChainID:     11155111,
			Employee:    employee,
			Amount:      big.NewInt(600),
			TxHash:      txHash,
			BlockNumber: 11080710,
			LogIndex:    0,
		},
		{
			ID:          102,
			ChainID:     11155111,
			Employee:    employee,
			Amount:      big.NewInt(600),
			TxHash:      txHash,
			BlockNumber: 11080710,
			LogIndex:    0,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Fatalf("expected 1 mismatch detected, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}

	expectedRef := fmt.Sprintf("11155111:%s:0:duplicate", txHash.Hex())
	exc, err := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if err != nil || exc == nil {
		t.Fatalf("expected open exception for %s, err: %v", expectedRef, err)
	}
	if exc.Type != TypeSalaryClaimDuplicate {
		t.Errorf("expected type %s, got %s", TypeSalaryClaimDuplicate, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// 6. RunningTwice_Idempotent
func TestReconcileSalaryClaim_RunningTwice_Idempotent(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, _, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xdddd4444dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080730,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(900),
		},
	}

	// First Run: Discrepancy detected and exception created
	res1, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil {
		t.Fatalf("first run error: %v", err)
	}
	if res1.ExceptionsCreated != 1 {
		t.Fatalf("first run expected 1 exception created, got %d", res1.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 1 {
		t.Fatalf("store expected 1 exception, got %d", len(rStore.exceptions))
	}

	// Second Run: Discrepancy still present, but exception already open -> skipped!
	res2, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil {
		t.Fatalf("second run error: %v", err)
	}
	if res2.ExceptionsCreated != 0 {
		t.Errorf("second run expected 0 exceptions created, got %d", res2.ExceptionsCreated)
	}
	if res2.ExceptionsSkipped != 1 {
		t.Errorf("second run expected 1 exception skipped, got %d", res2.ExceptionsSkipped)
	}
	if len(rStore.exceptions) != 1 {
		t.Errorf("store expected still 1 exception, got %d", len(rStore.exceptions))
	}
}

// 7. MismatchCorrected_ExceptionResolved
func TestReconcileSalaryClaim_MismatchCorrected_ExceptionResolved(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xeeee5555eeee5555eeee5555eeee5555eeee5555eeee5555eeee5555eeee5555")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080740,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(450),
		},
	}

	// First Run: Missing claim in DB -> creates exception
	res1, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil || res1.ExceptionsCreated != 1 {
		t.Fatalf("first run failed, exc created: %d, err: %v", res1.ExceptionsCreated, err)
	}

	expectedRef := fmt.Sprintf("11155111:%s:0:missing", txHash.Hex())
	exc, _ := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if exc == nil || exc.Status != StatusOpen {
		t.Fatalf("expected open exception")
	}

	// Indexer now indexes or backfills the claim record in DB!
	cStore.claims = []*models.SalaryClaim{
		{
			ID:          200,
			ChainID:     11155111,
			Employee:    employee,
			Amount:      big.NewInt(450),
			TxHash:      txHash,
			BlockNumber: 11080740,
			LogIndex:    0,
		},
	}

	// Second Run: Discrepancy is now fixed -> exception resolved!
	res2, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil {
		t.Fatalf("second run error: %v", err)
	}

	if res2.ExceptionsResolved != 1 {
		t.Errorf("expected 1 exception resolved, got %d", res2.ExceptionsResolved)
	}

	// Check status in store
	updatedExc := rStore.exceptions[exc.ID]
	if updatedExc.Status != StatusResolved {
		t.Errorf("expected status %s, got %s", StatusResolved, updatedExc.Status)
	}
	if updatedExc.ResolvedAt == nil {
		t.Errorf("expected resolved_at to be populated")
	}
}

// 8. SystemFailure_ReturnsErrorWithoutExceptions
func TestReconcileSalaryClaim_SystemFailure_ReturnsErrorWithoutExceptions(t *testing.T) {
	ctx := context.Background()

	// Sub-test 1: RPC GetLogs failure
	t.Run("RPC_GetLogs_Error", func(t *testing.T) {
		rec, client, _, _, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)
		client.getLogsErr = errors.New("simulated RPC timeout 504")

		res, err := rec.ReconcileRange(ctx, 11080700, 11080750)
		if err == nil {
			t.Fatal("expected error on RPC failure, got nil")
		}
		if res != nil {
			t.Errorf("expected nil result on error")
		}
		if len(rStore.exceptions) != 0 {
			t.Errorf("expected 0 exceptions created during system failure, got %d", len(rStore.exceptions))
		}
	})

	// Sub-test 2: RPC LatestBlock failure
	t.Run("RPC_LatestBlock_Error", func(t *testing.T) {
		rec, client, _, _, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)
		client.latestErr = errors.New("simulated network connection reset")

		res, err := rec.ReconcileRange(ctx, 11080700, 11080750)
		if err == nil {
			t.Fatal("expected error on RPC failure, got nil")
		}
		if res != nil {
			t.Errorf("expected nil result on error")
		}
		if len(rStore.exceptions) != 0 {
			t.Errorf("expected 0 exceptions created, got %d", len(rStore.exceptions))
		}
	})

	// Sub-test 3: Database Query failure
	t.Run("Database_Query_Error", func(t *testing.T) {
		rec, _, _, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)
		cStore.queryErr = errors.New("simulated PostgreSQL connection dropped")

		res, err := rec.ReconcileRange(ctx, 11080700, 11080750)
		if err == nil {
			t.Fatal("expected error on DB failure, got nil")
		}
		if res != nil {
			t.Errorf("expected nil result on error")
		}
		if len(rStore.exceptions) != 0 {
			t.Errorf("expected 0 exceptions created on DB error, got %d", len(rStore.exceptions))
		}
	})
}

// 9. UnfinalizedBlock_Ignored
func TestReconcileSalaryClaim_UnfinalizedBlock_Ignored(t *testing.T) {
	ctx := context.Background()
	// latestBlock = 11081000, confirmationDepth = 10 -> safeTarget = 11080990
	rec, client, decoderMock, _, rStore, _ := setupTestSalaryClaimReconciler(11081000, 10, 11081000)

	// Block 11080995 is unfinalized!
	txHash := common.HexToHash("0xffff6666ffff6666ffff6666ffff6666ffff6666ffff6666ffff6666ffff6666")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080995,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(100),
		},
	}

	// Reconcile range spanning into unfinalized area
	res, err := rec.ReconcileRange(ctx, 11080980, 11081000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// effectiveToBlock should be capped at safeTarget (11080990)
	if res.ToBlock != 11080990 {
		t.Errorf("expected ToBlock capped at safeTarget 11080990, got %d", res.ToBlock)
	}

	// No exceptions should be created for block 11080995!
	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions created for unfinalized blocks, got %d", res.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 0 {
		t.Errorf("expected empty exception store, got %d records", len(rStore.exceptions))
	}
}

// 10. IndexingLag_NotReportedAsMissing
func TestReconcileSalaryClaim_IndexingLag_NotReportedAsMissing(t *testing.T) {
	ctx := context.Background()
	// latestBlock = 11081000, safeTarget = 11080995, but indexerCheckpoint is only 11080700
	rec, client, decoderMock, _, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080700)

	// Block 11080750 is safe (> confirmation depth), but > indexerCheckpoint (indexer is lagging)
	txHash := common.HexToHash("0x1234123412341234123412341234123412341234123412341234123412341234")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080750,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(250),
		},
	}

	res, err := rec.ReconcileRange(ctx, 11080720, 11080760)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions created due to normal indexing lag, got %d", res.ExceptionsCreated)
	}
	if len(rStore.exceptions) != 0 {
		t.Errorf("expected empty exception store, got %d records", len(rStore.exceptions))
	}
}

// 11. TransactionInconsistency_CreatesException
func TestReconcileSalaryClaim_TransactionInconsistency_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, cStore, rStore, _ := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0x8888999988889999888899998888999988889999888899998888999988889999")
	employee := common.HexToAddress("0x2222222222222222222222222222222222222222")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080725,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventSalaryClaimed,
		Data: &abi.ABISalaryClaimedEvent{
			Employee: employee,
			Amount:   big.NewInt(350),
		},
	}

	cStore.claims = []*models.SalaryClaim{
		{
			ID:          400,
			ChainID:     11155111,
			Employee:    employee,
			Amount:      big.NewInt(350),
			TxHash:      txHash,
			BlockNumber: 11080726, // Mismatched block number in DB
			LogIndex:    0,
		},
	}

	res, err := rec.ReconcileRange(ctx, 11080720, 11080730)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExceptionsCreated != 1 {
		t.Fatalf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}

	expectedRef := fmt.Sprintf("11155111:%s:0:tx_inconsistency", txHash.Hex())
	exc, _ := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if exc == nil {
		t.Fatalf("expected exception for %s", expectedRef)
	}
	if exc.Type != TypeSalaryClaimTxInconsistent {
		t.Errorf("expected type %s, got %s", TypeSalaryClaimTxInconsistent, exc.Type)
	}
	if exc.Severity != SeverityMedium {
		t.Errorf("expected severity %s, got %s", SeverityMedium, exc.Severity)
	}
}

// 12. RecentWindowAndNextBatch
func TestReconcileSalaryClaim_RecentWindowAndNextBatch(t *testing.T) {
	ctx := context.Background()
	rec, _, _, _, _, cpStore := setupTestSalaryClaimReconciler(11081000, 5, 11080995)

	// Recent Window
	resWindow, err := rec.ReconcileRecentWindow(ctx, 100)
	if err != nil {
		t.Fatalf("ReconcileRecentWindow failed: %v", err)
	}
	if resWindow.SafeTarget != 11080995 {
		t.Errorf("expected SafeTarget 11080995, got %d", resWindow.SafeTarget)
	}
	if resWindow.ToBlock != 11080995 {
		t.Errorf("expected ToBlock 11080995, got %d", resWindow.ToBlock)
	}

	// Next Batch with checkpoint advancement
	resBatch, err := rec.ReconcileNextBatch(ctx, 50)
	if err != nil {
		t.Fatalf("ReconcileNextBatch failed: %v", err)
	}
	if resBatch.FromBlock != 11080692 {
		t.Errorf("expected FromBlock 11080692, got %d", resBatch.FromBlock)
	}
	if resBatch.ToBlock != 11080741 {
		t.Errorf("expected ToBlock 11080741, got %d", resBatch.ToBlock)
	}

	// Checkpoint should now be 11080741 under reconciliation_salary_claim
	cp, ok, _ := cpStore.GetCheckpoint(ctx, 11155111, "reconciliation_salary_claim")
	if !ok || cp != 11080741 {
		t.Errorf("expected reconciliation checkpoint 11080741, got %d (ok=%v)", cp, ok)
	}
}
