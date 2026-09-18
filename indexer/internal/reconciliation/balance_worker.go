package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// Domain Note:
// The Contract & Token Balance Reconciler verifies that the authoritative on-chain state
// of the WTF ERC-20 token (via balanceOf queries) and the MonthlyPayroll contract balances/state
// are consistent with the indexed PostgreSQL data.
// The blockchain is the authoritative source. PostgreSQL is the observed/indexed source.
// The reconciler never modifies blockchain state.
// All amounts are compared using exact *big.Int multi-precision integer arithmetic.

const (
	// Mismatch types for balance and contract state reconciliation
	TypeTokenBalanceMismatch    = "TOKEN_BALANCE_MISMATCH"
	TypeContractBalanceMismatch = "CONTRACT_BALANCE_MISMATCH"
	TypeContractStateMismatch   = "CONTRACT_STATE_MISMATCH"
)

// BalanceStore defines PostgreSQL operations for token balance and address calculations.
type BalanceStore interface {
	GetRelevantAddresses(ctx context.Context, chainID int64, token common.Address, payrollContract common.Address, upToBlock uint64) ([]common.Address, error)
	CalculateTokenBalance(ctx context.Context, chainID int64, token common.Address, wallet common.Address, upToBlock uint64) (*big.Int, error)
	CalculateAllTokenBalances(ctx context.Context, chainID int64, token common.Address, upToBlock uint64) (map[common.Address]*big.Int, error)
}

// PayrollStateStore retrieves employers and employees for state reconciliation.
type PayrollStateStore interface {
	ListEmployers(ctx context.Context, chainID int64) ([]*models.Employer, error)
	ListEmployees(ctx context.Context, chainID int64) ([]*models.Employee, error)
}

// BalanceReconcilerConfig holds runtime configuration for the balance reconciler.
type BalanceReconcilerConfig struct {
	ChainID                int64
	TokenAddress           common.Address
	PayrollContractAddress common.Address
	TokenStreamID          string // token indexer cursor stream ID for lag checks
	ReconStreamID          string // durable balance reconciliation cursor stream ID
	ConfirmationDepth      uint64
	BlockWindow            uint64
	StartBlock             uint64
}

// BalanceReconciliationResult summarizes the outcome of a balance reconciliation pass.
type BalanceReconciliationResult struct {
	TargetBlock           uint64 `json:"target_block"`
	SafeTarget            uint64 `json:"safe_target"`
	IndexerCheckpoint     uint64 `json:"indexer_checkpoint"`
	AddressesCheckedCount int    `json:"addresses_checked_count"`
	BalancesMatchedCount  int    `json:"balances_matched_count"`
	ContractBalancesCount int    `json:"contract_balances_count"`
	StatesCheckedCount    int    `json:"states_checked_count"`
	StatesMatchedCount    int    `json:"states_matched_count"`
	MismatchesDetected    int    `json:"mismatches_detected"`
	ExceptionsCreated     int    `json:"exceptions_created"`
	ExceptionsResolved    int    `json:"exceptions_resolved"`
	ExceptionsSkipped     int    `json:"exceptions_skipped"`
}

// BalanceReconciler audits on-chain balances and contract states against indexed PostgreSQL data.
type BalanceReconciler struct {
	cfg             BalanceReconcilerConfig
	client          BlockchainClient
	tokenABI        *abi.ABI
	payrollABI      *abi.ABI
	balanceStore    BalanceStore
	stateStore      PayrollStateStore
	reconStore      ReconciliationStore
	checkpointStore CheckpointStore
	retryer         *indexer.Retryer
}

// NewBalanceReconciler constructs a new BalanceReconciler.
func NewBalanceReconciler(
	cfg BalanceReconcilerConfig,
	client BlockchainClient,
	tokenABI *abi.ABI,
	payrollABI *abi.ABI,
	balanceStore BalanceStore,
	stateStore PayrollStateStore,
	reconStore ReconciliationStore,
	checkpointStore CheckpointStore,
) (*BalanceReconciler, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client cannot be nil")
	}
	if tokenABI == nil {
		return nil, fmt.Errorf("token ABI cannot be nil")
	}
	if balanceStore == nil {
		return nil, fmt.Errorf("balance store cannot be nil")
	}
	if reconStore == nil {
		return nil, fmt.Errorf("reconciliation store cannot be nil")
	}
	if cfg.TokenStreamID == "" {
		cfg.TokenStreamID = fmt.Sprintf("erc20_transfers_%s", strings.ToLower(cfg.TokenAddress.Hex()))
	}
	if cfg.ReconStreamID == "" {
		cfg.ReconStreamID = fmt.Sprintf("reconciliation_token_balance_%s", strings.ToLower(cfg.TokenAddress.Hex()))
	}

	return &BalanceReconciler{
		cfg:             cfg,
		client:          client,
		tokenABI:        tokenABI,
		payrollABI:      payrollABI,
		balanceStore:    balanceStore,
		stateStore:      stateStore,
		reconStore:      reconStore,
		checkpointStore: checkpointStore,
		retryer:         indexer.NewRetryer(indexer.DefaultRetryPolicy()),
	}, nil
}

// SetRetryPolicy customizes the RPC retry policy for the reconciler.
func (r *BalanceReconciler) SetRetryPolicy(policy indexer.RetryPolicy) {
	r.retryer = indexer.NewRetryer(policy)
}

func (r *BalanceReconciler) callWithRetry(ctx context.Context, op func() error) error {
	if r.retryer != nil {
		return r.retryer.RetryRange(ctx, r.cfg.ReconStreamID, 0, 0, op)
	}
	return op()
}

// ReconcileTargetBlock reconciles balances and contract states at an explicit target block.
func (r *BalanceReconciler) ReconcileTargetBlock(ctx context.Context, requestedTargetBlock uint64) (*BalanceReconciliationResult, error) {
	result := &BalanceReconciliationResult{}

	// 1. Fetch latest finalized block from RPC
	var latestBlock uint64
	err := r.callWithRetry(ctx, func() error {
		var qErr error
		latestBlock, qErr = r.client.LatestBlock(ctx)
		return qErr
	})
	if err != nil {
		return nil, fmt.Errorf("balance reconciler: failed to fetch latest block: %w", err)
	}

	if latestBlock < r.cfg.ConfirmationDepth {
		return result, nil
	}
	safeTarget := latestBlock - r.cfg.ConfirmationDepth
	result.SafeTarget = safeTarget

	// 2. Query token indexer checkpoint for indexing lag protection
	var indexerCheckpoint uint64
	if r.checkpointStore != nil {
		cp, ok, cpErr := r.checkpointStore.GetCheckpoint(ctx, r.cfg.ChainID, r.cfg.TokenStreamID)
		if cpErr != nil {
			return nil, fmt.Errorf("balance reconciler: failed to get token indexer checkpoint: %w", cpErr)
		}
		if ok {
			indexerCheckpoint = cp
		} else {
			indexerCheckpoint = r.cfg.StartBlock
		}
	} else {
		indexerCheckpoint = safeTarget
	}
	result.IndexerCheckpoint = indexerCheckpoint

	// 3. Compute effective target block with indexing lag protection
	effectiveTarget := safeTarget
	if requestedTargetBlock > 0 && requestedTargetBlock < effectiveTarget {
		effectiveTarget = requestedTargetBlock
	}

	// Never evaluate ahead of what the token indexer has indexed
	if effectiveTarget > indexerCheckpoint {
		effectiveTarget = indexerCheckpoint
	}

	if effectiveTarget < r.cfg.StartBlock {
		slog.Info("balance reconciler: effective target block is below token start block, skipping",
			"effective_target", effectiveTarget,
			"start_block", r.cfg.StartBlock,
		)
		return result, nil
	}

	result.TargetBlock = effectiveTarget

	slog.Info("reconciling contract and token balances",
		"chain_id", r.cfg.ChainID,
		"token", r.cfg.TokenAddress.Hex(),
		"payroll_contract", r.cfg.PayrollContractAddress.Hex(),
		"target_block", effectiveTarget,
		"safe_target", safeTarget,
		"indexer_checkpoint", indexerCheckpoint,
	)

	// 4. Reconcile token balances for relevant addresses
	if err := r.reconcileBalancesAtBlock(ctx, effectiveTarget, result); err != nil {
		return nil, err
	}

	// 5. Reconcile MonthlyPayroll contract token balance
	if err := r.reconcileContractBalanceAtBlock(ctx, effectiveTarget, result); err != nil {
		return nil, err
	}

	// 6. Reconcile MonthlyPayroll contract states (employers/employees)
	if err := r.reconcileContractStatesAtBlock(ctx, effectiveTarget, result); err != nil {
		return nil, err
	}

	return result, nil
}

// ReconcileRecentWindow reconciles balances at the block corresponding to the recent window offset.
func (r *BalanceReconciler) ReconcileRecentWindow(ctx context.Context, window uint64) (*BalanceReconciliationResult, error) {
	var latestBlock uint64
	err := r.callWithRetry(ctx, func() error {
		var qErr error
		latestBlock, qErr = r.client.LatestBlock(ctx)
		return qErr
	})
	if err != nil {
		return nil, fmt.Errorf("balance reconciler: failed to fetch latest block: %w", err)
	}

	if latestBlock < r.cfg.ConfirmationDepth {
		return &BalanceReconciliationResult{}, nil
	}
	safeTarget := latestBlock - r.cfg.ConfirmationDepth

	var targetBlock uint64
	if safeTarget > window {
		targetBlock = safeTarget - window
	} else {
		targetBlock = r.cfg.StartBlock
	}

	return r.ReconcileTargetBlock(ctx, targetBlock)
}

// ReconcileNextBatch reconciles the next incremental batch and advances the dedicated balance checkpoint.
func (r *BalanceReconciler) ReconcileNextBatch(ctx context.Context, batchSize uint64) (*BalanceReconciliationResult, error) {
	var lastCheckpoint uint64
	if r.checkpointStore != nil {
		cp, ok, err := r.checkpointStore.GetCheckpoint(ctx, r.cfg.ChainID, r.cfg.ReconStreamID)
		if err != nil {
			return nil, fmt.Errorf("balance reconciler: failed to get checkpoint: %w", err)
		}
		if ok && cp > 0 {
			lastCheckpoint = cp
		} else {
			lastCheckpoint = r.cfg.StartBlock
		}
	} else {
		lastCheckpoint = r.cfg.StartBlock
	}

	var targetBlock uint64
	if batchSize > 0 {
		targetBlock = lastCheckpoint + batchSize
	} else {
		targetBlock = lastCheckpoint + 50
	}

	res, err := r.ReconcileTargetBlock(ctx, targetBlock)
	if err != nil {
		return nil, err
	}

	// Advance checkpoint on success if target block progressed
	if r.checkpointStore != nil && res.TargetBlock > lastCheckpoint {
		if err := r.checkpointStore.SaveCheckpoint(ctx, r.cfg.ChainID, r.cfg.ReconStreamID, res.TargetBlock, ""); err != nil {
			return nil, fmt.Errorf("balance reconciler: failed to advance checkpoint: %w", err)
		}
		slog.Info("balance reconciliation checkpoint advanced",
			"stream_id", r.cfg.ReconStreamID,
			"new_checkpoint", res.TargetBlock,
		)
	}

	return res, nil
}

// Reconcile executes a reconciliation pass targeting the latest safe finalized/indexed head.
func (r *BalanceReconciler) Reconcile(ctx context.Context) (*BalanceReconciliationResult, error) {
	return r.ReconcileTargetBlock(ctx, 0)
}

// reconcileBalancesAtBlock compares on-chain ERC20.balanceOf(address) against DB-derived balances.
func (r *BalanceReconciler) reconcileBalancesAtBlock(
	ctx context.Context,
	targetBlock uint64,
	result *BalanceReconciliationResult,
) error {
	// 1. Discover all unique addresses participating in token or payroll movements
	relevantAddrs, err := r.balanceStore.GetRelevantAddresses(
		ctx,
		r.cfg.ChainID,
		r.cfg.TokenAddress,
		r.cfg.PayrollContractAddress,
		targetBlock,
	)
	if err != nil {
		return fmt.Errorf("failed to discover relevant addresses: %w", err)
	}

	// 2. Fetch all aggregate database-side balances up to targetBlock
	dbBalances, err := r.balanceStore.CalculateAllTokenBalances(ctx, r.cfg.ChainID, r.cfg.TokenAddress, targetBlock)
	if err != nil {
		return fmt.Errorf("failed to calculate database token balances: %w", err)
	}

	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")

	// 3. Compare each address
	for _, addr := range relevantAddrs {
		// Never treat the zero address as a normal user balance
		if addr == zeroAddr || addr == (common.Address{}) {
			continue
		}

		result.AddressesCheckedCount++

		expectedDBBal, ok := dbBalances[addr]
		if !ok || expectedDBBal == nil {
			expectedDBBal = big.NewInt(0)
		}

		// Query on-chain balanceOf(addr) at targetBlock
		var onChainBal *big.Int
		callErr := r.callWithRetry(ctx, func() error {
			data, packErr := r.tokenABI.Pack("balanceOf", addr)
			if packErr != nil {
				return fmt.Errorf("failed to pack balanceOf for %s: %w", addr.Hex(), packErr)
			}

			resBytes, cErr := r.client.CallContract(ctx, ethereum.CallMsg{
				To:   &r.cfg.TokenAddress,
				Data: data,
			}, new(big.Int).SetUint64(targetBlock))
			if cErr != nil {
				return cErr
			}

			unpacked, unpErr := r.tokenABI.Unpack("balanceOf", resBytes)
			if unpErr != nil {
				return fmt.Errorf("failed to unpack balanceOf return for %s: %w", addr.Hex(), unpErr)
			}
			if len(unpacked) == 0 {
				return fmt.Errorf("empty balanceOf return for %s", addr.Hex())
			}

			b, ok := unpacked[0].(*big.Int)
			if !ok {
				return fmt.Errorf("unexpected balanceOf return type %T for %s", unpacked[0], addr.Hex())
			}
			onChainBal = b
			return nil
		})
		if callErr != nil {
			return fmt.Errorf("operational error querying balanceOf(%s) at block %d: %w", addr.Hex(), targetBlock, callErr)
		}

		// Exact big.Int comparison
		if expectedDBBal.Cmp(onChainBal) == 0 {
			result.BalancesMatchedCount++
			r.resolveTokenBalanceException(ctx, result, addr)
		} else {
			result.MismatchesDetected++
			if err := r.recordTokenBalanceMismatch(ctx, result, addr, onChainBal, expectedDBBal, targetBlock); err != nil {
				return err
			}
		}
	}

	return nil
}

// reconcileContractBalanceAtBlock reconciles the WTF ERC-20 token balance of the MonthlyPayroll contract.
func (r *BalanceReconciler) reconcileContractBalanceAtBlock(
	ctx context.Context,
	targetBlock uint64,
	result *BalanceReconciliationResult,
) error {
	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")
	if r.cfg.PayrollContractAddress == zeroAddr || r.cfg.PayrollContractAddress == (common.Address{}) {
		return nil
	}

	payrollAddr := r.cfg.PayrollContractAddress

	// 1. Calculate database-derived token balance for the contract
	dbBal, err := r.balanceStore.CalculateTokenBalance(ctx, r.cfg.ChainID, r.cfg.TokenAddress, payrollAddr, targetBlock)
	if err != nil {
		return fmt.Errorf("failed to calculate contract token balance in database: %w", err)
	}
	if dbBal == nil {
		dbBal = big.NewInt(0)
	}

	// 2. Query authoritative on-chain ERC20.balanceOf(payrollContract) at targetBlock
	var onChainBal *big.Int
	callErr := r.callWithRetry(ctx, func() error {
		data, packErr := r.tokenABI.Pack("balanceOf", payrollAddr)
		if packErr != nil {
			return fmt.Errorf("failed to pack balanceOf for payroll contract: %w", packErr)
		}

		resBytes, cErr := r.client.CallContract(ctx, ethereum.CallMsg{
			To:   &r.cfg.TokenAddress,
			Data: data,
		}, new(big.Int).SetUint64(targetBlock))
		if cErr != nil {
			return cErr
		}

		unpacked, unpErr := r.tokenABI.Unpack("balanceOf", resBytes)
		if unpErr != nil {
			return fmt.Errorf("failed to unpack contract balanceOf return: %w", unpErr)
		}
		if len(unpacked) == 0 {
			return fmt.Errorf("empty contract balanceOf return")
		}

		b, ok := unpacked[0].(*big.Int)
		if !ok {
			return fmt.Errorf("unexpected contract balanceOf return type %T", unpacked[0])
		}
		onChainBal = b
		return nil
	})
	if callErr != nil {
		return fmt.Errorf("operational error querying contract balanceOf at block %d: %w", targetBlock, callErr)
	}

	// 3. Exact comparison
	if dbBal.Cmp(onChainBal) == 0 {
		result.ContractBalancesCount++
		r.resolveContractBalanceException(ctx, result, payrollAddr)
	} else {
		result.MismatchesDetected++
		if err := r.recordContractBalanceMismatch(ctx, result, payrollAddr, onChainBal, dbBal, targetBlock); err != nil {
			return err
		}
	}

	return nil
}

// reconcileContractStatesAtBlock reconciles active/inactive status for employers and employees.
func (r *BalanceReconciler) reconcileContractStatesAtBlock(
	ctx context.Context,
	targetBlock uint64,
	result *BalanceReconciliationResult,
) error {
	if r.stateStore == nil || r.payrollABI == nil {
		return nil
	}
	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")
	if r.cfg.PayrollContractAddress == zeroAddr || r.cfg.PayrollContractAddress == (common.Address{}) {
		return nil
	}

	// 1. Reconcile Employer Active States
	employers, err := r.stateStore.ListEmployers(ctx, r.cfg.ChainID)
	if err != nil {
		return fmt.Errorf("failed to list employers for state reconciliation: %w", err)
	}

	for _, emp := range employers {
		result.StatesCheckedCount++

		var onChainActive bool
		callErr := r.callWithRetry(ctx, func() error {
			data, packErr := r.payrollABI.Pack("isEmployerActive", emp.Wallet)
			if packErr != nil {
				return packErr
			}

			resBytes, cErr := r.client.CallContract(ctx, ethereum.CallMsg{
				To:   &r.cfg.PayrollContractAddress,
				Data: data,
			}, new(big.Int).SetUint64(targetBlock))
			if cErr != nil {
				return cErr
			}

			unpacked, unpErr := r.payrollABI.Unpack("isEmployerActive", resBytes)
			if unpErr != nil {
				return unpErr
			}
			if len(unpacked) == 0 {
				return fmt.Errorf("empty isEmployerActive return")
			}
			onChainActive = unpacked[0].(bool)
			return nil
		})
		if callErr != nil {
			return fmt.Errorf("operational error querying isEmployerActive(%s) at block %d: %w", emp.Wallet.Hex(), targetBlock, callErr)
		}

		if onChainActive == emp.Active {
			result.StatesMatchedCount++
			r.resolveStateException(ctx, result, emp.Wallet, "is_employer_active")
		} else {
			result.MismatchesDetected++
			if err := r.recordStateMismatch(ctx, result, emp.Wallet, "is_employer_active", onChainActive, emp.Active, targetBlock); err != nil {
				return err
			}
		}
	}

	// 2. Reconcile Employee Active States
	employees, err := r.stateStore.ListEmployees(ctx, r.cfg.ChainID)
	if err != nil {
		return fmt.Errorf("failed to list employees for state reconciliation: %w", err)
	}

	for _, ee := range employees {
		result.StatesCheckedCount++

		var onChainActive bool
		callErr := r.callWithRetry(ctx, func() error {
			data, packErr := r.payrollABI.Pack("isEmployeeActive", ee.Wallet)
			if packErr != nil {
				return packErr
			}

			resBytes, cErr := r.client.CallContract(ctx, ethereum.CallMsg{
				To:   &r.cfg.PayrollContractAddress,
				Data: data,
			}, new(big.Int).SetUint64(targetBlock))
			if cErr != nil {
				return cErr
			}

			unpacked, unpErr := r.payrollABI.Unpack("isEmployeeActive", resBytes)
			if unpErr != nil {
				return unpErr
			}
			if len(unpacked) == 0 {
				return fmt.Errorf("empty isEmployeeActive return")
			}
			onChainActive = unpacked[0].(bool)
			return nil
		})
		if callErr != nil {
			return fmt.Errorf("operational error querying isEmployeeActive(%s) at block %d: %w", ee.Wallet.Hex(), targetBlock, callErr)
		}

		if onChainActive == ee.Active {
			result.StatesMatchedCount++
			r.resolveStateException(ctx, result, ee.Wallet, "is_employee_active")
		} else {
			result.MismatchesDetected++
			if err := r.recordStateMismatch(ctx, result, ee.Wallet, "is_employee_active", onChainActive, ee.Active, targetBlock); err != nil {
				return err
			}
		}
	}

	return nil
}

// recordTokenBalanceMismatch records an idempotent TOKEN_BALANCE_MISMATCH exception in reconciliation_exceptions.
func (r *BalanceReconciler) recordTokenBalanceMismatch(
	ctx context.Context,
	result *BalanceReconciliationResult,
	wallet common.Address,
	onChainBal *big.Int,
	dbBal *big.Int,
	targetBlock uint64,
) error {
	entityRef := fmt.Sprintf("%d:%s:%s:token_balance_mismatch",
		r.cfg.ChainID,
		strings.ToLower(r.cfg.TokenAddress.Hex()),
		strings.ToLower(wallet.Hex()),
	)

	// Idempotency: Skip if already open
	openEx, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
	if err != nil {
		return fmt.Errorf("failed checking open exception: %w", err)
	}
	if openEx != nil {
		result.ExceptionsSkipped++
		return nil
	}

	diff := new(big.Int).Sub(onChainBal, dbBal)

	expectedJSON, _ := json.Marshal(map[string]interface{}{
		"on_chain_balance": onChainBal.String(),
		"target_block":     targetBlock,
		"wallet":           wallet.Hex(),
		"token":            r.cfg.TokenAddress.Hex(),
	})
	observedJSON, _ := json.Marshal(map[string]interface{}{
		"db_balance":   dbBal.String(),
		"target_block": targetBlock,
		"wallet":       wallet.Hex(),
		"token":        r.cfg.TokenAddress.Hex(),
		"difference":   diff.String(),
	})

	exc := &models.ReconciliationException{
		Type:       TypeTokenBalanceMismatch,
		Severity:   SeverityHigh,
		EntityRef:  entityRef,
		Expected:   expectedJSON,
		Observed:   observedJSON,
		Status:     StatusOpen,
		DetectedAt: time.Now().UTC(),
	}

	if err := r.reconStore.CreateException(ctx, exc); err != nil {
		return fmt.Errorf("failed creating token balance mismatch exception: %w", err)
	}

	result.ExceptionsCreated++
	slog.Warn("reconciliation mismatch detected: token balance",
		"wallet", wallet.Hex(),
		"on_chain_balance", onChainBal.String(),
		"db_balance", dbBal.String(),
		"difference", diff.String(),
		"target_block", targetBlock,
	)

	return nil
}

// recordContractBalanceMismatch records an idempotent CONTRACT_BALANCE_MISMATCH exception.
func (r *BalanceReconciler) recordContractBalanceMismatch(
	ctx context.Context,
	result *BalanceReconciliationResult,
	contract common.Address,
	onChainBal *big.Int,
	dbBal *big.Int,
	targetBlock uint64,
) error {
	entityRef := fmt.Sprintf("%d:%s:%s:contract_balance_mismatch",
		r.cfg.ChainID,
		strings.ToLower(contract.Hex()),
		strings.ToLower(r.cfg.TokenAddress.Hex()),
	)

	openEx, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
	if err != nil {
		return fmt.Errorf("failed checking open exception: %w", err)
	}
	if openEx != nil {
		result.ExceptionsSkipped++
		return nil
	}

	diff := new(big.Int).Sub(onChainBal, dbBal)

	expectedJSON, _ := json.Marshal(map[string]interface{}{
		"on_chain_contract_balance": onChainBal.String(),
		"target_block":              targetBlock,
		"contract":                  contract.Hex(),
		"token":                     r.cfg.TokenAddress.Hex(),
	})
	observedJSON, _ := json.Marshal(map[string]interface{}{
		"db_contract_balance": dbBal.String(),
		"target_block":        targetBlock,
		"contract":            contract.Hex(),
		"token":               r.cfg.TokenAddress.Hex(),
		"difference":          diff.String(),
	})

	exc := &models.ReconciliationException{
		Type:       TypeContractBalanceMismatch,
		Severity:   SeverityHigh,
		EntityRef:  entityRef,
		Expected:   expectedJSON,
		Observed:   observedJSON,
		Status:     StatusOpen,
		DetectedAt: time.Now().UTC(),
	}

	if err := r.reconStore.CreateException(ctx, exc); err != nil {
		return fmt.Errorf("failed creating contract balance mismatch exception: %w", err)
	}

	result.ExceptionsCreated++
	slog.Warn("reconciliation mismatch detected: contract token balance",
		"contract", contract.Hex(),
		"on_chain_balance", onChainBal.String(),
		"db_balance", dbBal.String(),
		"difference", diff.String(),
		"target_block", targetBlock,
	)

	return nil
}

// recordStateMismatch records an idempotent CONTRACT_STATE_MISMATCH exception.
func (r *BalanceReconciler) recordStateMismatch(
	ctx context.Context,
	result *BalanceReconciliationResult,
	wallet common.Address,
	stateProperty string,
	onChainValue bool,
	dbValue bool,
	targetBlock uint64,
) error {
	entityRef := fmt.Sprintf("%d:%s:%s:%s:state_mismatch",
		r.cfg.ChainID,
		strings.ToLower(r.cfg.PayrollContractAddress.Hex()),
		strings.ToLower(wallet.Hex()),
		stateProperty,
	)

	openEx, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
	if err != nil {
		return fmt.Errorf("failed checking open exception: %w", err)
	}
	if openEx != nil {
		result.ExceptionsSkipped++
		return nil
	}

	expectedJSON, _ := json.Marshal(map[string]interface{}{
		"on_chain_value": onChainValue,
		"property":       stateProperty,
		"target_block":   targetBlock,
		"wallet":         wallet.Hex(),
	})
	observedJSON, _ := json.Marshal(map[string]interface{}{
		"db_value":     dbValue,
		"property":     stateProperty,
		"target_block": targetBlock,
		"wallet":       wallet.Hex(),
	})

	exc := &models.ReconciliationException{
		Type:       TypeContractStateMismatch,
		Severity:   SeverityMedium,
		EntityRef:  entityRef,
		Expected:   expectedJSON,
		Observed:   observedJSON,
		Status:     StatusOpen,
		DetectedAt: time.Now().UTC(),
	}

	if err := r.reconStore.CreateException(ctx, exc); err != nil {
		return fmt.Errorf("failed creating state mismatch exception: %w", err)
	}

	result.ExceptionsCreated++
	slog.Warn("reconciliation mismatch detected: contract state",
		"wallet", wallet.Hex(),
		"property", stateProperty,
		"on_chain_value", onChainValue,
		"db_value", dbValue,
		"target_block", targetBlock,
	)

	return nil
}

// resolveTokenBalanceException auto-resolves any open balance exception for wallet.
func (r *BalanceReconciler) resolveTokenBalanceException(
	ctx context.Context,
	result *BalanceReconciliationResult,
	wallet common.Address,
) {
	entityRef := fmt.Sprintf("%d:%s:%s:token_balance_mismatch",
		r.cfg.ChainID,
		strings.ToLower(r.cfg.TokenAddress.Hex()),
		strings.ToLower(wallet.Hex()),
	)

	openEx, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
	if err != nil || openEx == nil {
		return
	}

	if err := r.reconStore.ResolveException(ctx, openEx.ID, time.Now().UTC()); err == nil {
		result.ExceptionsResolved++
		slog.Info("token balance reconciliation exception resolved",
			"id", openEx.ID,
			"entity_ref", entityRef,
		)
	}
}

// resolveContractBalanceException auto-resolves any open contract balance exception.
func (r *BalanceReconciler) resolveContractBalanceException(
	ctx context.Context,
	result *BalanceReconciliationResult,
	contract common.Address,
) {
	entityRef := fmt.Sprintf("%d:%s:%s:contract_balance_mismatch",
		r.cfg.ChainID,
		strings.ToLower(contract.Hex()),
		strings.ToLower(r.cfg.TokenAddress.Hex()),
	)

	openEx, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
	if err != nil || openEx == nil {
		return
	}

	if err := r.reconStore.ResolveException(ctx, openEx.ID, time.Now().UTC()); err == nil {
		result.ExceptionsResolved++
		slog.Info("contract token balance reconciliation exception resolved",
			"id", openEx.ID,
			"entity_ref", entityRef,
		)
	}
}

// resolveStateException auto-resolves any open state mismatch exception.
func (r *BalanceReconciler) resolveStateException(
	ctx context.Context,
	result *BalanceReconciliationResult,
	wallet common.Address,
	stateProperty string,
) {
	entityRef := fmt.Sprintf("%d:%s:%s:%s:state_mismatch",
		r.cfg.ChainID,
		strings.ToLower(r.cfg.PayrollContractAddress.Hex()),
		strings.ToLower(wallet.Hex()),
		stateProperty,
	)

	openEx, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
	if err != nil || openEx == nil {
		return
	}

	if err := r.reconStore.ResolveException(ctx, openEx.ID, time.Now().UTC()); err == nil {
		result.ExceptionsResolved++
		slog.Info("contract state reconciliation exception resolved",
			"id", openEx.ID,
			"entity_ref", entityRef,
		)
	}
}
