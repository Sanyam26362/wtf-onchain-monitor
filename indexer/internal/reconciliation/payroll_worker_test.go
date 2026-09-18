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

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// --- Mocks for testing ---

type mockBlockchainClient struct {
	mu               sync.Mutex
	latestBlock      uint64
	latestErr        error
	logs             []types.Log
	getLogsErr       error
	getLogsCall      int
	callContractFunc func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	callContractErr  error
}

func (m *mockBlockchainClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.callContractErr != nil {
		return nil, m.callContractErr
	}
	if m.callContractFunc != nil {
		return m.callContractFunc(ctx, msg, blockNumber)
	}
	return nil, nil
}

func (m *mockBlockchainClient) LatestBlock(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latestErr != nil {
		return 0, m.latestErr
	}
	return m.latestBlock, nil
}

func (m *mockBlockchainClient) GetLogs(ctx context.Context, contractAddress common.Address, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getLogsCall++
	if m.getLogsErr != nil {
		return nil, m.getLogsErr
	}
	var filtered []types.Log
	for _, l := range m.logs {
		if l.BlockNumber >= fromBlock && l.BlockNumber <= toBlock {
			filtered = append(filtered, l)
		}
	}
	return filtered, nil
}

type mockEventDecoder struct {
	events map[string]*decoder.DecodedEvent
}

func (m *mockEventDecoder) Decode(log types.Log) (*decoder.DecodedEvent, error) {
	key := fmt.Sprintf("%s:%d", strings.ToLower(log.TxHash.Hex()), log.Index)
	if ev, ok := m.events[key]; ok {
		return ev, nil
	}
	return nil, errors.New("event not found")
}

type mockPayrollFundingStore struct {
	mu       sync.Mutex
	fundings []*models.PayrollFunding
	queryErr error
}

func (m *mockPayrollFundingStore) GetFundingsByBlockRange(ctx context.Context, chainID int64, fromBlock, toBlock uint64) ([]*models.PayrollFunding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var res []*models.PayrollFunding
	for _, f := range m.fundings {
		if f.ChainID == chainID && f.BlockNumber >= fromBlock && f.BlockNumber <= toBlock {
			res = append(res, f)
		}
	}
	return res, nil
}

type mockReconciliationStore struct {
	mu         sync.Mutex
	exceptions map[int64]*models.ReconciliationException
	nextID     int64
	createErr  error
	queryErr   error
	resolveErr error
}

func newMockReconciliationStore() *mockReconciliationStore {
	return &mockReconciliationStore{
		exceptions: make(map[int64]*models.ReconciliationException),
		nextID:     1,
	}
}

func (m *mockReconciliationStore) CreateException(ctx context.Context, exc *models.ReconciliationException) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return m.createErr
	}
	exc.ID = m.nextID
	m.nextID++
	copied := *exc
	m.exceptions[exc.ID] = &copied
	return nil
}

func (m *mockReconciliationStore) GetOpenExceptionByEntityRef(ctx context.Context, entityRef string) (*models.ReconciliationException, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	for _, exc := range m.exceptions {
		if exc.EntityRef == entityRef && exc.Status == StatusOpen {
			copied := *exc
			return &copied, nil
		}
	}
	return nil, nil
}

func (m *mockReconciliationStore) ResolveException(ctx context.Context, id int64, resolvedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resolveErr != nil {
		return m.resolveErr
	}
	exc, ok := m.exceptions[id]
	if !ok {
		return errors.New("exception not found")
	}
	exc.Status = StatusResolved
	t := resolvedAt.UTC()
	exc.ResolvedAt = &t
	return nil
}

func (m *mockReconciliationStore) GetOpenExceptionsByEntityRefPrefix(ctx context.Context, prefix string) ([]*models.ReconciliationException, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var res []*models.ReconciliationException
	for _, exc := range m.exceptions {
		if strings.HasPrefix(exc.EntityRef, prefix) && exc.Status == StatusOpen {
			copied := *exc
			res = append(res, &copied)
		}
	}
	return res, nil
}

type mockCheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]uint64
	queryErr    error
	saveErr     error
}

func newMockCheckpointStore() *mockCheckpointStore {
	return &mockCheckpointStore{
		checkpoints: make(map[string]uint64),
	}
}

func (m *mockCheckpointStore) GetCheckpoint(ctx context.Context, chainID int64, streamID string) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return 0, false, m.queryErr
	}
	key := fmt.Sprintf("%d:%s", chainID, streamID)
	val, ok := m.checkpoints[key]
	return val, ok, nil
}

func (m *mockCheckpointStore) SaveCheckpoint(ctx context.Context, chainID int64, streamID string, blockNumber uint64, blockHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	key := fmt.Sprintf("%d:%s", chainID, streamID)
	m.checkpoints[key] = blockNumber
	return nil
}

// Helper to create test setup
func setupTestReconciler(
	latestBlock uint64,
	confirmationDepth uint64,
	indexerCheckpoint uint64,
) (*PayrollReconciler, *mockBlockchainClient, *mockEventDecoder, *mockPayrollFundingStore, *mockReconciliationStore, *mockCheckpointStore) {
	client := &mockBlockchainClient{latestBlock: latestBlock}
	eventDecoder := &mockEventDecoder{events: make(map[string]*decoder.DecodedEvent)}
	payrollStore := &mockPayrollFundingStore{}
	reconStore := newMockReconciliationStore()
	checkpointStore := newMockCheckpointStore()

	if indexerCheckpoint > 0 {
		_ = checkpointStore.SaveCheckpoint(context.Background(), 11155111, "monthly_payroll", indexerCheckpoint, "")
	}

	cfg := PayrollReconcilerConfig{
		ChainID:           11155111,
		ContractAddress:   common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC"),
		PayrollStreamID:   "monthly_payroll",
		ReconStreamID:     "reconciliation_payroll_funding",
		ConfirmationDepth: confirmationDepth,
		StartBlock:        11080692,
	}

	rec, _ := NewPayrollReconciler(cfg, client, eventDecoder, payrollStore, reconStore, checkpointStore)
	rec.SetRetryPolicy(indexer.RetryPolicy{
		MaxRetries:     1,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		BackoffFactor:  1.5,
		Sleeper: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	})

	return rec, client, eventDecoder, payrollStore, reconStore, checkpointStore
}

// Requirement A: Matching funding -> no exception
func TestReconcile_MatchingFunding_NoException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
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
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(1000),
			Fee:            big.NewInt(50),
			AmountCredited: big.NewInt(950),
		},
	}

	// Database record matching exactly
	pStore.fundings = []*models.PayrollFunding{
		{
			ID:             1,
			ChainID:        11155111,
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(1000),
			Fee:            big.NewInt(50),
			AmountCredited: big.NewInt(950),
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

// Requirement B: Missing funding -> PAYROLL_FUNDING_MISSING (severity high)
func TestReconcile_MissingFunding_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, _, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xaaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       2,
			BlockNumber: 11080750,
		},
	}
	decoderMock.events[eventKey(txHash, 2)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(5000),
			Fee:            big.NewInt(250),
			AmountCredited: big.NewInt(4750),
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

	if exc.Type != TypePayrollFundingMissing {
		t.Errorf("expected type %s, got %s", TypePayrollFundingMissing, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
	if exc.Status != StatusOpen {
		t.Errorf("expected status %s, got %s", StatusOpen, exc.Status)
	}
}

// Requirement C: Incorrect amount -> PAYROLL_FUNDING_AMOUNT_MISMATCH (severity high)
func TestReconcile_IncorrectAmount_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xbbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       1,
			BlockNumber: 11080720,
		},
	}
	decoderMock.events[eventKey(txHash, 1)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(2000), // On chain: 2000
			Fee:            big.NewInt(100),
			AmountCredited: big.NewInt(1900),
		},
	}

	// Database record has different amount_paid!
	pStore.fundings = []*models.PayrollFunding{
		{
			ID:             10,
			ChainID:        11155111,
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(1500), // DB has 1500!
			Fee:            big.NewInt(100),
			AmountCredited: big.NewInt(1400),
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
	if exc.Type != TypePayrollFundingAmountMismatch {
		t.Errorf("expected type %s, got %s", TypePayrollFundingAmountMismatch, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// Requirement D: Duplicate records -> PAYROLL_FUNDING_DUPLICATE (severity high)
func TestReconcile_DuplicateRecords_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xcccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080710,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(1000),
			Fee:            big.NewInt(50),
			AmountCredited: big.NewInt(950),
		},
	}

	// 2 database records for the SAME (tx_hash, log_index)!
	pStore.fundings = []*models.PayrollFunding{
		{
			ID:             101,
			ChainID:        11155111,
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(1000),
			Fee:            big.NewInt(50),
			AmountCredited: big.NewInt(950),
			TxHash:         txHash,
			BlockNumber:    11080710,
			LogIndex:       0,
		},
		{
			ID:             102,
			ChainID:        11155111,
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(1000),
			Fee:            big.NewInt(50),
			AmountCredited: big.NewInt(950),
			TxHash:         txHash,
			BlockNumber:    11080710,
			LogIndex:       0,
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
	if exc.Type != TypePayrollFundingDuplicate {
		t.Errorf("expected type %s, got %s", TypePayrollFundingDuplicate, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// Requirement E: Running twice -> no duplicate exception (idempotent)
func TestReconcile_RunningTwice_Idempotent(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, _, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xdddd4444dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444dddd4444")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080730,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(3000),
			Fee:            big.NewInt(150),
			AmountCredited: big.NewInt(2850),
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

// Requirement F: Previously open mismatch corrected -> exception resolved
func TestReconcile_MismatchCorrected_ExceptionResolved(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0xeeee5555eeee5555eeee5555eeee5555eeee5555eeee5555eeee5555eeee5555")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080740,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(4000),
			Fee:            big.NewInt(200),
			AmountCredited: big.NewInt(3800),
		},
	}

	// First Run: Missing funding in DB -> creates exception
	res1, err := rec.ReconcileRange(ctx, 11080700, 11080750)
	if err != nil || res1.ExceptionsCreated != 1 {
		t.Fatalf("first run failed, exc created: %d, err: %v", res1.ExceptionsCreated, err)
	}

	expectedRef := fmt.Sprintf("11155111:%s:0:missing", txHash.Hex())
	exc, _ := rStore.GetOpenExceptionByEntityRef(ctx, expectedRef)
	if exc == nil || exc.Status != StatusOpen {
		t.Fatalf("expected open exception")
	}

	// Indexer now backfills or fixes the record in database!
	pStore.fundings = []*models.PayrollFunding{
		{
			ID:             200,
			ChainID:        11155111,
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(4000),
			Fee:            big.NewInt(200),
			AmountCredited: big.NewInt(3800),
			TxHash:         txHash,
			BlockNumber:    11080740,
			LogIndex:       0,
		},
	}

	// Second Run: Reconciliation discovers the funding is now matching!
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

// Requirement G: Database/RPC failure -> returned as system failure error without creating false mismatch
func TestReconcile_SystemFailure_ReturnsErrorWithoutExceptions(t *testing.T) {
	ctx := context.Background()

	// Sub-test 1: RPC GetLogs failure
	t.Run("RPC_GetLogs_Error", func(t *testing.T) {
		rec, client, _, _, rStore, _ := setupTestReconciler(11081000, 5, 11080995)
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
		rec, client, _, _, rStore, _ := setupTestReconciler(11081000, 5, 11080995)
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
		rec, _, _, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)
		pStore.queryErr = errors.New("simulated PostgreSQL connection dropped")

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

// Requirement H: Non-finalized block -> ignored (no mismatch reported)
func TestReconcile_UnfinalizedBlock_Ignored(t *testing.T) {
	ctx := context.Background()
	// latestBlock = 11081000, confirmationDepth = 10 -> safeTarget = 11080990
	rec, client, decoderMock, _, rStore, _ := setupTestReconciler(11081000, 10, 11081000)

	// Block 11080995 is within confirmation depth (unfinalized!)
	txHash := common.HexToHash("0xffff6666ffff6666ffff6666ffff6666ffff6666ffff6666ffff6666ffff6666")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080995,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(100),
			Fee:            big.NewInt(5),
			AmountCredited: big.NewInt(95),
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

// Indexing Lag Test: On-chain event exists in safe block, but indexer hasn't indexed up to that block yet -> Skipped, not an error!
func TestReconcile_IndexingLag_NotReportedAsMissing(t *testing.T) {
	ctx := context.Background()
	// latestBlock = 11081000, safeTarget = 11080995, but indexerCheckpoint is only 11080700
	rec, client, decoderMock, _, rStore, _ := setupTestReconciler(11081000, 5, 11080700)

	// Block 11080750 is safe (> confirmation depth), but > indexerCheckpoint (indexer is lagging)
	txHash := common.HexToHash("0x1234123412341234123412341234123412341234123412341234123412341234")
	employer := common.HexToAddress("0x2222222222222222222222222222222222222222")
	employee := common.HexToAddress("0x3333333333333333333333333333333333333333")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080750,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(100),
			Fee:            big.NewInt(5),
			AmountCredited: big.NewInt(95),
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

// Entity Address Mismatch Test: employer or employee differs -> PAYROLL_FUNDING_ENTITY_MISMATCH (severity critical)
func TestReconcile_EntityMismatch_CriticalSeverity(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0x7777888877778888777788887777888877778888777788887777888877778888")
	employerOnChain := common.HexToAddress("0x1111111111111111111111111111111111111111")
	employeeOnChain := common.HexToAddress("0x2222222222222222222222222222222222222222")

	employerWrong := common.HexToAddress("0x9999999999999999999999999999999999999999")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080715,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employerOnChain,
			Employee:       employeeOnChain,
			AmountPaid:     big.NewInt(100),
			Fee:            big.NewInt(5),
			AmountCredited: big.NewInt(95),
		},
	}

	pStore.fundings = []*models.PayrollFunding{
		{
			ID:             300,
			ChainID:        11155111,
			Employer:       employerWrong, // Corrupted employer address
			Employee:       employeeOnChain,
			AmountPaid:     big.NewInt(100),
			Fee:            big.NewInt(5),
			AmountCredited: big.NewInt(95),
			TxHash:         txHash,
			BlockNumber:    11080715,
			LogIndex:       0,
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
	if exc.Type != TypePayrollFundingEntityMismatch {
		t.Errorf("expected type %s, got %s", TypePayrollFundingEntityMismatch, exc.Type)
	}
	if exc.Severity != SeverityCritical {
		t.Errorf("expected severity %s, got %s", SeverityCritical, exc.Severity)
	}
}

// Transaction Inconsistency Test: block number mismatch -> PAYROLL_FUNDING_TX_INCONSISTENCY (severity medium)
func TestReconcile_TxInconsistency_MediumSeverity(t *testing.T) {
	ctx := context.Background()
	rec, client, decoderMock, pStore, rStore, _ := setupTestReconciler(11081000, 5, 11080995)

	txHash := common.HexToHash("0x8888999988889999888899998888999988889999888899998888999988889999")
	employer := common.HexToAddress("0x1111111111111111111111111111111111111111")
	employee := common.HexToAddress("0x2222222222222222222222222222222222222222")

	client.logs = []types.Log{
		{
			TxHash:      txHash,
			Index:       0,
			BlockNumber: 11080725,
		},
	}
	decoderMock.events[eventKey(txHash, 0)] = &decoder.DecodedEvent{
		Type: decoder.EventPayrollFunded,
		Data: &abi.ABIPayrollFundedEvent{
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(100),
			Fee:            big.NewInt(5),
			AmountCredited: big.NewInt(95),
		},
	}

	pStore.fundings = []*models.PayrollFunding{
		{
			ID:             400,
			ChainID:        11155111,
			Employer:       employer,
			Employee:       employee,
			AmountPaid:     big.NewInt(100),
			Fee:            big.NewInt(5),
			AmountCredited: big.NewInt(95),
			TxHash:         txHash,
			BlockNumber:    11080726, // Mismatched block number in DB
			LogIndex:       0,
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
	if exc.Type != TypePayrollFundingTxInconsistent {
		t.Errorf("expected type %s, got %s", TypePayrollFundingTxInconsistent, exc.Type)
	}
	if exc.Severity != SeverityMedium {
		t.Errorf("expected severity %s, got %s", SeverityMedium, exc.Severity)
	}
}

// ReconcileRecentWindow and ReconcileNextBatch tests
func TestReconcile_RecentWindowAndNextBatch(t *testing.T) {
	ctx := context.Background()
	rec, _, _, _, _, cpStore := setupTestReconciler(11081000, 5, 11080995)

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

	// Checkpoint should now be 11080741
	cp, ok, _ := cpStore.GetCheckpoint(ctx, 11155111, "reconciliation_payroll_funding")
	if !ok || cp != 11080741 {
		t.Errorf("expected reconciliation checkpoint 11080741, got %d (ok=%v)", cp, ok)
	}
}
