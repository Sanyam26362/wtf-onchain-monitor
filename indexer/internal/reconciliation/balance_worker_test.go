package reconciliation

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// --- Helpers for ABI encoding mock outputs ---

func packUint256(val *big.Int) []byte {
	if val == nil {
		return make([]byte, 32)
	}
	return common.LeftPadBytes(val.Bytes(), 32)
}

func packBool(val bool) []byte {
	b := byte(0)
	if val {
		b = 1
	}
	return common.LeftPadBytes([]byte{b}, 32)
}

// --- Mocks for Balance Reconciliation ---

type mockBalanceStore struct {
	mu                sync.Mutex
	transfers         []*models.TokenTransfer
	relevantAddresses []common.Address
	balances          map[common.Address]*big.Int
	getAddrsErr       error
	calcBalErr        error
	calcAllErr        error
}

func (m *mockBalanceStore) GetRelevantAddresses(
	ctx context.Context,
	chainID int64,
	token common.Address,
	payrollContract common.Address,
	upToBlock uint64,
) ([]common.Address, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getAddrsErr != nil {
		return nil, m.getAddrsErr
	}
	if len(m.relevantAddresses) > 0 {
		return m.relevantAddresses, nil
	}

	seen := make(map[common.Address]bool)
	var addrs []common.Address
	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")

	for _, tr := range m.transfers {
		if tr.ChainID == chainID && strings.EqualFold(tr.Token.Hex(), token.Hex()) && tr.BlockNumber <= upToBlock && !tr.Removed {
			if tr.FromAddress != zeroAddr && tr.FromAddress != (common.Address{}) && !seen[tr.FromAddress] {
				seen[tr.FromAddress] = true
				addrs = append(addrs, tr.FromAddress)
			}
			if tr.ToAddress != zeroAddr && tr.ToAddress != (common.Address{}) && !seen[tr.ToAddress] {
				seen[tr.ToAddress] = true
				addrs = append(addrs, tr.ToAddress)
			}
		}
	}

	if payrollContract != zeroAddr && payrollContract != (common.Address{}) && !seen[payrollContract] {
		seen[payrollContract] = true
		addrs = append(addrs, payrollContract)
	}

	sort.Slice(addrs, func(i, j int) bool {
		return strings.ToLower(addrs[i].Hex()) < strings.ToLower(addrs[j].Hex())
	})

	return addrs, nil
}

func (m *mockBalanceStore) CalculateTokenBalance(
	ctx context.Context,
	chainID int64,
	token common.Address,
	wallet common.Address,
	upToBlock uint64,
) (*big.Int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calcBalErr != nil {
		return nil, m.calcBalErr
	}
	if len(m.balances) > 0 {
		if b, ok := m.balances[wallet]; ok {
			return new(big.Int).Set(b), nil
		}
	}

	net := big.NewInt(0)
	for _, tr := range m.transfers {
		if tr.ChainID == chainID && strings.EqualFold(tr.Token.Hex(), token.Hex()) && tr.BlockNumber <= upToBlock && !tr.Removed {
			if tr.ToAddress == wallet {
				net.Add(net, tr.Amount)
			}
			if tr.FromAddress == wallet {
				net.Sub(net, tr.Amount)
			}
		}
	}
	return net, nil
}

func (m *mockBalanceStore) CalculateAllTokenBalances(
	ctx context.Context,
	chainID int64,
	token common.Address,
	upToBlock uint64,
) (map[common.Address]*big.Int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calcAllErr != nil {
		return nil, m.calcAllErr
	}
	if len(m.balances) > 0 {
		copyMap := make(map[common.Address]*big.Int)
		for k, v := range m.balances {
			copyMap[k] = new(big.Int).Set(v)
		}
		return copyMap, nil
	}

	result := make(map[common.Address]*big.Int)
	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")

	for _, tr := range m.transfers {
		if tr.ChainID == chainID && strings.EqualFold(tr.Token.Hex(), token.Hex()) && tr.BlockNumber <= upToBlock && !tr.Removed {
			if tr.ToAddress != zeroAddr && tr.ToAddress != (common.Address{}) {
				if _, ok := result[tr.ToAddress]; !ok {
					result[tr.ToAddress] = big.NewInt(0)
				}
				result[tr.ToAddress].Add(result[tr.ToAddress], tr.Amount)
			}
			if tr.FromAddress != zeroAddr && tr.FromAddress != (common.Address{}) {
				if _, ok := result[tr.FromAddress]; !ok {
					result[tr.FromAddress] = big.NewInt(0)
				}
				result[tr.FromAddress].Sub(result[tr.FromAddress], tr.Amount)
			}
		}
	}
	return result, nil
}

type mockPayrollStateStore struct {
	mu        sync.Mutex
	employers []*models.Employer
	employees []*models.Employee
	err       error
}

func (m *mockPayrollStateStore) ListEmployers(ctx context.Context, chainID int64) ([]*models.Employer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return m.employers, nil
}

func (m *mockPayrollStateStore) ListEmployees(ctx context.Context, chainID int64) ([]*models.Employee, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return m.employees, nil
}

var (
	testBalanceTokenAddr   = common.HexToAddress("0x378AFb93CaDd39AFF154704d2D90Af8c401137E7")
	testBalancePayrollAddr = common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	testBalanceStreamID    = "erc20_transfers_0x378afb93cadd39aff154704d2d90af8c401137e7"
	testBalanceReconID     = "reconciliation_token_balance_0x378afb93cadd39aff154704d2d90af8c401137e7"
)

const standardERC20BalanceABI = `[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]`

func setupTestBalanceReconciler(
	latestBlock uint64,
	confirmationDepth uint64,
	indexerCheckpoint uint64,
) (*BalanceReconciler, *mockBlockchainClient, *mockBalanceStore, *mockPayrollStateStore, *mockReconciliationStore, *mockCheckpointStore) {
	client := &mockBlockchainClient{latestBlock: latestBlock}
	tokenABI, _ := abi.JSON(strings.NewReader(standardERC20BalanceABI))
	payrollABI, _ := abi.JSON(strings.NewReader(indexerABI.MainABI))

	balanceStore := &mockBalanceStore{
		balances: make(map[common.Address]*big.Int),
	}
	stateStore := &mockPayrollStateStore{}
	reconStore := newMockReconciliationStore()
	checkpointStore := newMockCheckpointStore()

	if indexerCheckpoint > 0 {
		_ = checkpointStore.SaveCheckpoint(context.Background(), 11155111, testBalanceStreamID, indexerCheckpoint, "")
	}

	cfg := BalanceReconcilerConfig{
		ChainID:                11155111,
		TokenAddress:           testBalanceTokenAddr,
		PayrollContractAddress: testBalancePayrollAddr,
		TokenStreamID:          testBalanceStreamID,
		ReconStreamID:          testBalanceReconID,
		ConfirmationDepth:      confirmationDepth,
		StartBlock:             11717931,
	}

	rec, _ := NewBalanceReconciler(cfg, client, &tokenABI, &payrollABI, balanceStore, stateStore, reconStore, checkpointStore)
	rec.SetRetryPolicy(indexer.RetryPolicy{
		MaxRetries:     1,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		BackoffFactor:  1.5,
		Sleeper:        func(ctx context.Context, d time.Duration) error { return nil },
	})

	return rec, client, balanceStore, stateStore, reconStore, checkpointStore
}

// 1. TestReconcileBalance_MatchingBalance_NoException
func TestReconcileBalance_MatchingBalance_NoException(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	userBal := big.NewInt(500000)

	balanceStore.transfers = append(balanceStore.transfers, &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testBalanceTokenAddr,
		FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
		ToAddress:   userAddr,
		Amount:      userBal,
		BlockNumber: 11718000,
	})

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				return packUint256(userBal), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 0 {
		t.Errorf("expected 0 exceptions, got %d", res.ExceptionsCreated)
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions in store, got %d", len(reconStore.exceptions))
	}
}

// 2. TestReconcileBalance_MissingIndexedTransfer_CreatesMismatch
func TestReconcileBalance_MissingIndexedTransfer_CreatesMismatch(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	onChainBal := big.NewInt(1000000)

	balanceStore.relevantAddresses = []common.Address{userAddr}
	// DB has no transfers for userAddr (db balance = 0, but on-chain has 1,000,000)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				return packUint256(onChainBal), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Errorf("expected 1 mismatch, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 1 {
		t.Errorf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}
	if len(reconStore.exceptions) != 1 {
		t.Fatalf("expected 1 exception in store, got %d", len(reconStore.exceptions))
	}

	var exc *models.ReconciliationException
	for _, e := range reconStore.exceptions {
		exc = e
		break
	}
	if exc.Type != TypeTokenBalanceMismatch {
		t.Errorf("expected type %s, got %s", TypeTokenBalanceMismatch, exc.Type)
	}
	if exc.Severity != SeverityHigh {
		t.Errorf("expected severity %s, got %s", SeverityHigh, exc.Severity)
	}
}

// 3. TestReconcileBalance_ExtraIndexedTransfer_CreatesMismatch
func TestReconcileBalance_ExtraIndexedTransfer_CreatesMismatch(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	// DB has 500, but on-chain only has 0
	balanceStore.transfers = append(balanceStore.transfers, &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testBalanceTokenAddr,
		FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
		ToAddress:   userAddr,
		Amount:      big.NewInt(500),
		BlockNumber: 11718000,
	})

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		return packUint256(big.NewInt(0)), nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Errorf("expected 1 mismatch, got %d", res.MismatchesDetected)
	}
	if res.ExceptionsCreated != 1 {
		t.Errorf("expected 1 exception created, got %d", res.ExceptionsCreated)
	}
	if len(reconStore.exceptions) != 1 {
		t.Errorf("expected 1 exception in store, got %d", len(reconStore.exceptions))
	}
}

// 4. TestReconcileBalance_IncorrectCalculatedBalance_CreatesMismatch
func TestReconcileBalance_IncorrectCalculatedBalance_CreatesMismatch(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x4444444444444444444444444444444444444444")
	balanceStore.relevantAddresses = []common.Address{userAddr}
	balanceStore.balances[userAddr] = big.NewInt(500)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				// On chain returns 600
				return packUint256(big.NewInt(600)), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Errorf("expected 1 mismatch, got %d", res.MismatchesDetected)
	}
	if len(reconStore.exceptions) != 1 {
		t.Fatalf("expected 1 exception, got %d", len(reconStore.exceptions))
	}
}

// 5. TestReconcileBalance_ContractBalanceMismatch_CreatesException
func TestReconcileBalance_ContractBalanceMismatch_CreatesException(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	balanceStore.balances[testBalancePayrollAddr] = big.NewInt(1000)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == testBalancePayrollAddr {
				// On chain payroll contract token balance is 2000, DB is 1000
				return packUint256(big.NewInt(2000)), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundContractMismatch := false
	for _, exc := range reconStore.exceptions {
		if exc.Type == TypeContractBalanceMismatch {
			foundContractMismatch = true
			if exc.Severity != SeverityHigh {
				t.Errorf("expected severity high, got %s", exc.Severity)
			}
		}
	}
	if !foundContractMismatch {
		t.Errorf("expected CONTRACT_BALANCE_MISMATCH exception, got result: %+v", res)
	}
}

// 6. TestReconcileBalance_RunningTwice_Idempotent
func TestReconcileBalance_RunningTwice_Idempotent(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x5555555555555555555555555555555555555555")
	balanceStore.relevantAddresses = []common.Address{userAddr}
	balanceStore.balances[userAddr] = big.NewInt(100)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				return packUint256(big.NewInt(200)), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	// First pass
	res1, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("first pass error: %v", err)
	}
	if res1.ExceptionsCreated != 1 {
		t.Errorf("expected 1 created on first pass, got %d", res1.ExceptionsCreated)
	}

	// Second pass
	res2, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("second pass error: %v", err)
	}
	if res2.ExceptionsCreated != 0 {
		t.Errorf("expected 0 created on second pass, got %d", res2.ExceptionsCreated)
	}
	if res2.ExceptionsSkipped < 1 {
		t.Errorf("expected at least 1 skipped on second pass, got %d", res2.ExceptionsSkipped)
	}
	if len(reconStore.exceptions) != 1 {
		t.Errorf("expected exactly 1 exception in store, got %d", len(reconStore.exceptions))
	}
}

// 7. TestReconcileBalance_MismatchCorrected_ExceptionResolved
func TestReconcileBalance_MismatchCorrected_ExceptionResolved(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x6666666666666666666666666666666666666666")
	balanceStore.relevantAddresses = []common.Address{userAddr}
	balanceStore.balances[userAddr] = big.NewInt(100)

	onChainVal := big.NewInt(200)
	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				return packUint256(onChainVal), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	// Pass 1: Creates exception
	_, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("pass 1 error: %v", err)
	}
	if len(reconStore.exceptions) != 1 {
		t.Fatalf("expected 1 open exception, got %d", len(reconStore.exceptions))
	}
	var exc1 *models.ReconciliationException
	for _, e := range reconStore.exceptions {
		exc1 = e
		break
	}
	if exc1.Status != StatusOpen {
		t.Fatalf("expected exception status open, got %s", exc1.Status)
	}

	// Pass 2: DB balance now matches on-chain (backfilled)
	balanceStore.balances[userAddr] = big.NewInt(200)
	res2, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("pass 2 error: %v", err)
	}

	if res2.ExceptionsResolved < 1 {
		t.Errorf("expected at least 1 exception resolved, got %d", res2.ExceptionsResolved)
	}
	var exc2 *models.ReconciliationException
	for _, e := range reconStore.exceptions {
		exc2 = e
		break
	}
	if exc2.Status != StatusResolved {
		t.Errorf("expected exception status resolved, got %s", exc2.Status)
	}
}

// 8. TestReconcileBalance_RPCFailure_ReturnsErrorWithoutException
func TestReconcileBalance_RPCFailure_ReturnsErrorWithoutException(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0x7777777777777777777777777777777777777777")
	balanceStore.relevantAddresses = []common.Address{userAddr}
	client.callContractErr = errors.New("simulated RPC timeout 504")

	_, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err == nil {
		t.Fatalf("expected error on RPC failure, got nil")
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions created on RPC failure, got %d", len(reconStore.exceptions))
	}
}

// 9. TestReconcileBalance_DatabaseFailure_ReturnsErrorWithoutException
func TestReconcileBalance_DatabaseFailure_ReturnsErrorWithoutException(t *testing.T) {
	ctx := context.Background()
	rec, _, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	balanceStore.getAddrsErr = errors.New("simulated PostgreSQL connection pool error")

	_, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err == nil {
		t.Fatalf("expected error on DB failure, got nil")
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions on DB failure, got %d", len(reconStore.exceptions))
	}
}

// 10. TestReconcileBalance_IndexingLag_NotReportedAsMismatch
func TestReconcileBalance_IndexingLag_NotReportedAsMismatch(t *testing.T) {
	ctx := context.Background()
	// Safe target is 11725000, but indexer checkpoint is only at 11720000
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11725005, 5, 11720000)

	userAddr := common.HexToAddress("0x8888888888888888888888888888888888888888")
	balanceStore.relevantAddresses = []common.Address{userAddr}
	balanceStore.balances[userAddr] = big.NewInt(100)

	// On-chain call verifies the block tag requested:
	// If reconciler requests at safe target (11725000) it would see 500, but at 11720000 it sees 100!
	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				if blockNumber.Uint64() <= 11720000 {
					return packUint256(big.NewInt(100)), nil
				}
				return packUint256(big.NewInt(500)), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	// Reconcile without explicit target; should be bounded to indexer checkpoint 11720000
	res, err := rec.Reconcile(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.TargetBlock != 11720000 {
		t.Errorf("expected target block bounded to indexer checkpoint 11720000, got %d", res.TargetBlock)
	}
	if res.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches due to indexing lag protection, got %d", res.MismatchesDetected)
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions, got %d", len(reconStore.exceptions))
	}
}

// 11. TestReconcileBalance_FinalityProtection
func TestReconcileBalance_FinalityProtection(t *testing.T) {
	ctx := context.Background()
	// Latest is 11720010, confirmation depth is 5 -> safeTarget is 11720005
	rec, client, _, _, _, _ := setupTestBalanceReconciler(11720010, 5, 11725000)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		return packUint256(big.NewInt(0)), nil
	}

	// Requested block 11720009 is beyond safeTarget (11720005)
	res, err := rec.ReconcileTargetBlock(ctx, 11720009)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.TargetBlock > 11720005 {
		t.Errorf("expected target block capped at safe target 11720005, got %d", res.TargetBlock)
	}
}

// 12. TestReconcileBalance_MintHandling
func TestReconcileBalance_MintHandling(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	recipient := common.HexToAddress("0x9999999999999999999999999999999999999999")
	mintAmount := big.NewInt(750000)

	// Mint transfer from zero address
	balanceStore.transfers = append(balanceStore.transfers, &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testBalanceTokenAddr,
		FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
		ToAddress:   recipient,
		Amount:      mintAmount,
		BlockNumber: 11718000,
	})

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == recipient {
				return packUint256(mintAmount), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 0 {
		t.Errorf("expected mint to calculate positive recipient balance without mismatch, got %d", res.MismatchesDetected)
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions, got %d", len(reconStore.exceptions))
	}
}

// 13. TestReconcileBalance_BurnHandling
func TestReconcileBalance_BurnHandling(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	sender := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	initialMint := big.NewInt(1000)
	burned := big.NewInt(300)
	remaining := big.NewInt(700)

	// Mint 1000 to sender, then burn 300 to zero address
	balanceStore.transfers = append(balanceStore.transfers,
		&models.TokenTransfer{
			ChainID:     11155111,
			Token:       testBalanceTokenAddr,
			FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			ToAddress:   sender,
			Amount:      initialMint,
			BlockNumber: 11718000,
		},
		&models.TokenTransfer{
			ChainID:     11155111,
			Token:       testBalanceTokenAddr,
			FromAddress: sender,
			ToAddress:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
			Amount:      burned,
			BlockNumber: 11718010,
		},
	)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == sender {
				return packUint256(remaining), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 0 {
		t.Errorf("expected burn to calculate 700 without mismatch, got %d", res.MismatchesDetected)
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions, got %d", len(reconStore.exceptions))
	}
}

// 14. TestReconcileBalance_ZeroAddressExcludedFromNormalBalances
func TestReconcileBalance_ZeroAddressExcludedFromNormalBalances(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, _, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")
	userAddr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	balanceStore.transfers = append(balanceStore.transfers, &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testBalanceTokenAddr,
		FromAddress: zeroAddr,
		ToAddress:   userAddr,
		Amount:      big.NewInt(100),
		BlockNumber: 11718000,
	})

	calledAddresses := make(map[common.Address]bool)
	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if len(msg.Data) >= 36 {
			// Extract address from balanceOf(address)
			target := common.BytesToAddress(msg.Data[16:36])
			calledAddresses[target] = true
		}
		return packUint256(big.NewInt(100)), nil
	}

	_, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if calledAddresses[zeroAddr] {
		t.Errorf("zero address was unexpectedly queried for a balance")
	}
}

// 15. TestReconcileBalance_TokenIsolation
func TestReconcileBalance_TokenIsolation(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	otherToken := common.HexToAddress("0x9999999999999999999999999999999999999999")
	userAddr := common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

	// Transfer is for otherToken, NOT testBalanceTokenAddr
	balanceStore.transfers = append(balanceStore.transfers, &models.TokenTransfer{
		ChainID:     11155111,
		Token:       otherToken,
		FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
		ToAddress:   userAddr,
		Amount:      big.NewInt(10000),
		BlockNumber: 11718000,
	})

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		return packUint256(big.NewInt(0)), nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches for isolated token, got %d", res.MismatchesDetected)
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions, got %d", len(reconStore.exceptions))
	}
}

// 16. TestReconcileBalance_LargeAmountExactArithmetic
func TestReconcileBalance_LargeAmountExactArithmetic(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
	// 1,000,000,000 * 10^18 (10^27, exceeding 64-bit int and float64 exact range)
	largeAmount, _ := new(big.Int).SetString("1000000000000000000000000000", 10)

	balanceStore.transfers = append(balanceStore.transfers, &models.TokenTransfer{
		ChainID:     11155111,
		Token:       testBalanceTokenAddr,
		FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
		ToAddress:   userAddr,
		Amount:      largeAmount,
		BlockNumber: 11718000,
	})

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				return packUint256(largeAmount), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 0 {
		t.Errorf("expected exact big.Int parity for 10^27, got %d mismatches", res.MismatchesDetected)
	}
	if len(reconStore.exceptions) != 0 {
		t.Errorf("expected 0 exceptions, got %d", len(reconStore.exceptions))
	}
}

// 17. TestReconcileBalance_BlockTaggedBalanceMatchesIndexedRange
func TestReconcileBalance_BlockTaggedBalanceMatchesIndexedRange(t *testing.T) {
	ctx := context.Background()
	rec, client, balanceStore, _, _, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	userAddr := common.HexToAddress("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")

	// Transfer 1 at block 11718000
	// Transfer 2 at block 11719000
	balanceStore.transfers = append(balanceStore.transfers,
		&models.TokenTransfer{
			ChainID:     11155111,
			Token:       testBalanceTokenAddr,
			FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			ToAddress:   userAddr,
			Amount:      big.NewInt(100),
			BlockNumber: 11718000,
		},
		&models.TokenTransfer{
			ChainID:     11155111,
			Token:       testBalanceTokenAddr,
			FromAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			ToAddress:   userAddr,
			Amount:      big.NewInt(200),
			BlockNumber: 11719000,
		},
	)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr && len(msg.Data) >= 36 {
			addr := common.BytesToAddress(msg.Data[16:36])
			if addr == userAddr {
				if blockNumber.Uint64() < 11719000 {
					return packUint256(big.NewInt(100)), nil
				}
				return packUint256(big.NewInt(300)), nil
			}
			return packUint256(big.NewInt(0)), nil
		}
		return nil, nil
	}

	// At block 11718500, balance should be 100
	res1, err := rec.ReconcileTargetBlock(ctx, 11718500)
	if err != nil {
		t.Fatalf("res1 error: %v", err)
	}
	if res1.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches at block 11718500, got %d", res1.MismatchesDetected)
	}

	// At block 11719500, balance should be 300
	res2, err := rec.ReconcileTargetBlock(ctx, 11719500)
	if err != nil {
		t.Fatalf("res2 error: %v", err)
	}
	if res2.MismatchesDetected != 0 {
		t.Errorf("expected 0 mismatches at block 11719500, got %d", res2.MismatchesDetected)
	}
}

// 18. TestReconcileBalance_ContractState_EmployerActiveMismatch
func TestReconcileBalance_ContractState_EmployerActiveMismatch(t *testing.T) {
	ctx := context.Background()
	rec, client, _, stateStore, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	empAddr := common.HexToAddress("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
	stateStore.employers = []*models.Employer{
		{
			Wallet:  empAddr,
			Active:  true, // DB says true
			ChainID: 11155111,
		},
	}

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr {
			return packUint256(big.NewInt(0)), nil
		}
		if *msg.To == testBalancePayrollAddr {
			// On chain returns false for isEmployerActive
			return packBool(false), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Errorf("expected 1 state mismatch, got %d", res.MismatchesDetected)
	}

	foundStateMismatch := false
	for _, exc := range reconStore.exceptions {
		if exc.Type == TypeContractStateMismatch {
			foundStateMismatch = true
		}
	}
	if !foundStateMismatch {
		t.Errorf("expected CONTRACT_STATE_MISMATCH exception in store")
	}
}

// 19. TestReconcileBalance_ContractState_EmployeeActiveMismatch
func TestReconcileBalance_ContractState_EmployeeActiveMismatch(t *testing.T) {
	ctx := context.Background()
	rec, client, _, stateStore, reconStore, _ := setupTestBalanceReconciler(11720000, 5, 11720000)

	eeAddr := common.HexToAddress("0x1212121212121212121212121212121212121212")
	stateStore.employees = []*models.Employee{
		{
			Wallet:  eeAddr,
			Active:  false, // DB says false
			ChainID: 11155111,
		},
	}

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		if *msg.To == testBalanceTokenAddr {
			return packUint256(big.NewInt(0)), nil
		}
		if *msg.To == testBalancePayrollAddr {
			// On chain returns true for isEmployeeActive
			return packBool(true), nil
		}
		return nil, nil
	}

	res, err := rec.ReconcileTargetBlock(ctx, 11719000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.MismatchesDetected != 1 {
		t.Errorf("expected 1 state mismatch, got %d", res.MismatchesDetected)
	}

	foundStateMismatch := false
	for _, exc := range reconStore.exceptions {
		if exc.Type == TypeContractStateMismatch {
			foundStateMismatch = true
		}
	}
	if !foundStateMismatch {
		t.Errorf("expected CONTRACT_STATE_MISMATCH exception in store")
	}
}

// 20. TestReconcileBalance_NextBatchAdvancesDedicatedCheckpoint
func TestReconcileBalance_NextBatchAdvancesDedicatedCheckpoint(t *testing.T) {
	ctx := context.Background()
	rec, client, _, _, _, checkpointStore := setupTestBalanceReconciler(11720100, 5, 11720100)

	client.callContractFunc = func(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
		return packUint256(big.NewInt(0)), nil
	}

	// Initial checkpoint is 11717931
	res, err := rec.ReconcileNextBatch(ctx, 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedTarget := uint64(11717931 + 50)
	if res.TargetBlock != expectedTarget {
		t.Errorf("expected target block %d, got %d", expectedTarget, res.TargetBlock)
	}

	cp, ok, err := checkpointStore.GetCheckpoint(ctx, 11155111, testBalanceReconID)
	if err != nil || !ok {
		t.Fatalf("expected checkpoint to exist: %v", err)
	}
	if cp != expectedTarget {
		t.Errorf("expected advanced checkpoint %d, got %d", expectedTarget, cp)
	}
}
