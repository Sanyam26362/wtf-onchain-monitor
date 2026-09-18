package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// Domain Note:
// The MonthlyPayroll smart contract (0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC)
// emits the SalaryClaimed event whenever an employee claims their accumulated salary.
// The contract storage does not retain a historical transaction ledger of past claims.
// Therefore, the authoritative on-chain source for individual salary claims is the immutable
// SalaryClaimed event emitted in transaction receipts on the blockchain.
// The salary claim reconciler verifies these authoritative on-chain logs against the
// indexed PostgreSQL database records in salary_claims.

const (
	// Mismatch types for Salary Claim reconciliation
	TypeSalaryClaimMissing        = "SALARY_CLAIM_MISSING"
	TypeSalaryClaimAmountMismatch = "SALARY_CLAIM_AMOUNT_MISMATCH"
	TypeSalaryClaimEntityMismatch = "SALARY_CLAIM_ENTITY_MISMATCH"
	TypeSalaryClaimDuplicate      = "SALARY_CLAIM_DUPLICATE"
	TypeSalaryClaimTxInconsistent = "SALARY_CLAIM_TX_INCONSISTENCY"
)

// SalaryClaimStore retrieves indexed salary claim events from PostgreSQL.
type SalaryClaimStore interface {
	GetClaimsByBlockRange(ctx context.Context, chainID int64, fromBlock, toBlock uint64) ([]*models.SalaryClaim, error)
}

// SalaryClaimReconcilerConfig holds configuration for the salary claim reconciler.
type SalaryClaimReconcilerConfig struct {
	ChainID           int64
	ContractAddress   common.Address
	PayrollStreamID   string // stream ID for indexer lag checks (e.g. "monthly_payroll")
	ReconStreamID     string // stream ID for durable reconciliation cursor (e.g. "reconciliation_salary_claim")
	ConfirmationDepth uint64
	BlockWindow       uint64
	StartBlock        uint64
}

// SalaryClaimReconciler executes reconciliation checks comparing on-chain SalaryClaimed events
// against indexed database records in PostgreSQL.
type SalaryClaimReconciler struct {
	cfg             SalaryClaimReconcilerConfig
	client          BlockchainClient
	decoder         EventDecoder
	claimStore      SalaryClaimStore
	reconStore      ReconciliationStore
	checkpointStore CheckpointStore
	retryer         *indexer.Retryer
}

// NewSalaryClaimReconciler constructs a new SalaryClaimReconciler.
func NewSalaryClaimReconciler(
	cfg SalaryClaimReconcilerConfig,
	client BlockchainClient,
	eventDecoder EventDecoder,
	claimStore SalaryClaimStore,
	reconStore ReconciliationStore,
	checkpointStore CheckpointStore,
) (*SalaryClaimReconciler, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client cannot be nil")
	}
	if eventDecoder == nil {
		return nil, fmt.Errorf("event decoder cannot be nil")
	}
	if claimStore == nil {
		return nil, fmt.Errorf("salary claim store cannot be nil")
	}
	if reconStore == nil {
		return nil, fmt.Errorf("reconciliation store cannot be nil")
	}
	if cfg.PayrollStreamID == "" {
		cfg.PayrollStreamID = "monthly_payroll"
	}
	if cfg.ReconStreamID == "" {
		cfg.ReconStreamID = "reconciliation_salary_claim"
	}

	return &SalaryClaimReconciler{
		cfg:             cfg,
		client:          client,
		decoder:         eventDecoder,
		claimStore:      claimStore,
		reconStore:      reconStore,
		checkpointStore: checkpointStore,
		retryer:         indexer.NewRetryer(indexer.DefaultRetryPolicy()),
	}, nil
}

// SetRetryPolicy customizes the RPC retry policy for the reconciler.
func (r *SalaryClaimReconciler) SetRetryPolicy(policy indexer.RetryPolicy) {
	r.retryer = indexer.NewRetryer(policy)
}

type onChainClaimRecord struct {
	TxHash      common.Hash
	LogIndex    uint
	BlockNumber uint64
	Employee    common.Address
	Amount      *big.Int
}

// ReconcileRange performs salary claim reconciliation for an explicit block range [fromBlock, toBlock].
// It returns system errors directly without creating false reconciliation exceptions.
func (r *SalaryClaimReconciler) ReconcileRange(ctx context.Context, fromBlock, toBlock uint64) (*ReconciliationResult, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range: fromBlock (%d) > toBlock (%d)", fromBlock, toBlock)
	}

	// 1. Fetch latest block to determine safeTarget based on confirmation depth
	var latestBlock uint64
	var err error
	if r.retryer != nil {
		err = r.retryer.RetryRange(ctx, r.cfg.ReconStreamID, fromBlock, toBlock, func() error {
			var qErr error
			latestBlock, qErr = r.client.LatestBlock(ctx)
			return qErr
		})
	} else {
		latestBlock, err = r.client.LatestBlock(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to get latest block from RPC: %w", err)
	}

	var safeTarget uint64 = latestBlock
	if r.cfg.ConfirmationDepth > 0 && latestBlock >= r.cfg.ConfirmationDepth {
		safeTarget = latestBlock - r.cfg.ConfirmationDepth
	}

	// If the entire requested window is beyond the safe target, skip as unfinalized (Rule 12)
	if fromBlock > safeTarget {
		slog.Info("reconciliation skipped unfinalized block range for salary claims",
			"from_block", fromBlock,
			"to_block", toBlock,
			"latest_block", latestBlock,
			"safe_target", safeTarget,
		)
		return &ReconciliationResult{
			FromBlock:  fromBlock,
			ToBlock:    toBlock,
			SafeTarget: safeTarget,
		}, nil
	}

	effectiveToBlock := toBlock
	if effectiveToBlock > safeTarget {
		effectiveToBlock = safeTarget
	}

	// 2. Fetch indexer checkpoint to identify indexing lag (Rule 11)
	var indexerCheckpoint uint64
	var hasCheckpoint bool
	if r.checkpointStore != nil {
		var cpErr error
		indexerCheckpoint, hasCheckpoint, cpErr = r.checkpointStore.GetCheckpoint(ctx, r.cfg.ChainID, r.cfg.PayrollStreamID)
		if cpErr != nil {
			return nil, fmt.Errorf("reconciliation failed to retrieve indexer checkpoint: %w", cpErr)
		}
	}

	// 3. Fetch authoritative logs from blockchain
	var logs []types.Log
	if r.retryer != nil {
		err = r.retryer.RetryRange(ctx, r.cfg.ReconStreamID, fromBlock, effectiveToBlock, func() error {
			var qErr error
			logs, qErr = r.client.GetLogs(ctx, r.cfg.ContractAddress, fromBlock, effectiveToBlock)
			return qErr
		})
	} else {
		logs, err = r.client.GetLogs(ctx, r.cfg.ContractAddress, fromBlock, effectiveToBlock)
	}
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to fetch logs [%d, %d]: %w", fromBlock, effectiveToBlock, err)
	}

	// 4. Decode SalaryClaimed logs
	onChainMap := make(map[string]*onChainClaimRecord)
	var onChainList []*onChainClaimRecord

	for _, log := range logs {
		if log.Removed {
			continue // Skip reorganized logs
		}

		decoded, err := r.decoder.Decode(log)
		if err != nil {
			// Skip logs not matching the MonthlyPayroll ABI or not decodable
			continue
		}

		if decoded.Type != decoder.EventSalaryClaimed {
			continue
		}

		event, ok := decoded.Data.(*abi.ABISalaryClaimedEvent)
		if !ok || event == nil {
			continue
		}

		amount := new(big.Int)
		if event.Amount != nil {
			amount = new(big.Int).Set(event.Amount)
		}

		rec := &onChainClaimRecord{
			TxHash:      log.TxHash,
			LogIndex:    log.Index,
			BlockNumber: log.BlockNumber,
			Employee:    event.Employee,
			Amount:      amount,
		}

		key := eventKey(log.TxHash, log.Index)
		onChainMap[key] = rec
		onChainList = append(onChainList, rec)
	}

	// 5. Fetch observed database records for the block range
	dbRecords, err := r.claimStore.GetClaimsByBlockRange(ctx, r.cfg.ChainID, fromBlock, effectiveToBlock)
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to retrieve db claims [%d, %d]: %w", fromBlock, effectiveToBlock, err)
	}

	dbMap := make(map[string][]*models.SalaryClaim)
	for _, rec := range dbRecords {
		key := eventKey(rec.TxHash, rec.LogIndex)
		dbMap[key] = append(dbMap[key], rec)
	}

	result := &ReconciliationResult{
		FromBlock:          fromBlock,
		ToBlock:            effectiveToBlock,
		SafeTarget:         safeTarget,
		IndexerCheckpoint:  indexerCheckpoint,
		OnChainEventsCount: len(onChainList),
		DBRecordsCount:     len(dbRecords),
	}

	// Track all active mismatches detected in this pass
	activeMismatches := make(map[string]bool)

	recordMismatch := func(excType, severity, entityRef string, expected, observed any) error {
		activeMismatches[entityRef] = true
		result.MismatchesDetected++

		existing, err := r.reconStore.GetOpenExceptionByEntityRef(ctx, entityRef)
		if err != nil {
			return fmt.Errorf("failed to check existing exception for %s: %w", entityRef, err)
		}

		if existing != nil {
			result.ExceptionsSkipped++
			return nil
		}

		expBytes, err := json.Marshal(expected)
		if err != nil {
			return fmt.Errorf("failed to marshal expected data: %w", err)
		}
		obsBytes, err := json.Marshal(observed)
		if err != nil {
			return fmt.Errorf("failed to marshal observed data: %w", err)
		}

		exc := &models.ReconciliationException{
			Type:       excType,
			Severity:   severity,
			EntityRef:  entityRef,
			Expected:   expBytes,
			Observed:   obsBytes,
			Status:     StatusOpen,
			DetectedAt: time.Now().UTC(),
		}

		if err := r.reconStore.CreateException(ctx, exc); err != nil {
			return fmt.Errorf("failed to create reconciliation exception: %w", err)
		}

		result.ExceptionsCreated++
		slog.Warn("reconciliation mismatch detected and logged for salary claim",
			"type", excType,
			"severity", severity,
			"entity_ref", entityRef,
		)
		return nil
	}

	// 6. Check for duplicate database records (Rule 9)
	for key, recs := range dbMap {
		if len(recs) > 1 {
			first := recs[0]
			entityRef := fmt.Sprintf("%d:%s:%d:duplicate", r.cfg.ChainID, first.TxHash.Hex(), first.LogIndex)
			var ids []int64
			for _, r := range recs {
				ids = append(ids, r.ID)
			}

			expected := map[string]any{
				"count":        1,
				"chain_id":     r.cfg.ChainID,
				"tx_hash":      first.TxHash.Hex(),
				"log_index":    first.LogIndex,
				"block_number": first.BlockNumber,
			}
			observed := map[string]any{
				"count":      len(recs),
				"record_ids": ids,
			}

			if err := recordMismatch(TypeSalaryClaimDuplicate, SeverityHigh, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}
		_ = key
	}

	// 7. Compare authoritative On-Chain events with Database records
	for _, onChain := range onChainList {
		key := eventKey(onChain.TxHash, onChain.LogIndex)
		dbRecs := dbMap[key]

		// Case A: Missing in database
		if len(dbRecs) == 0 {
			// Lag Check: If block is unfinalized, skip
			if onChain.BlockNumber > safeTarget {
				continue
			}

			// Lag Check: If indexer hasn't reached this block yet, it is indexing lag, NOT an error
			if !hasCheckpoint || onChain.BlockNumber > indexerCheckpoint {
				slog.Debug("on-chain claim event not yet indexed due to indexing lag",
					"block", onChain.BlockNumber,
					"indexer_checkpoint", indexerCheckpoint,
					"tx_hash", onChain.TxHash.Hex(),
				)
				continue
			}

			// Genuine Missing Event: indexer checkpoint >= blockNumber, but row is missing!
			entityRef := fmt.Sprintf("%d:%s:%d:missing", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"employee":     onChain.Employee.Hex(),
				"amount":       onChain.Amount.String(),
			}
			observed := map[string]any{
				"status": "not_found",
			}

			if err := recordMismatch(TypeSalaryClaimMissing, SeverityHigh, entityRef, expected, observed); err != nil {
				return nil, err
			}
			continue
		}

		// Case B: Record exists in database, compare fields
		dbRec := dbRecs[0]

		// 1. Entity Mismatch: Employee address differs (Critical)
		if !strings.EqualFold(dbRec.Employee.Hex(), onChain.Employee.Hex()) {
			entityRef := fmt.Sprintf("%d:%s:%d:entity_mismatch", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"employee":     onChain.Employee.Hex(),
			}
			observed := map[string]any{
				"employee": dbRec.Employee.Hex(),
			}
			if err := recordMismatch(TypeSalaryClaimEntityMismatch, SeverityCritical, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}

		// 2. Amount Mismatch: amount differs (High)
		if dbRec.Amount == nil || dbRec.Amount.Cmp(onChain.Amount) != 0 {
			entityRef := fmt.Sprintf("%d:%s:%d:amount_mismatch", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"employee":     onChain.Employee.Hex(),
				"amount":       onChain.Amount.String(),
			}
			observed := map[string]any{
				"amount": safeBigIntString(dbRec.Amount),
			}
			if err := recordMismatch(TypeSalaryClaimAmountMismatch, SeverityHigh, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}

		// 3. Tx Inconsistency: Block number mismatch (Medium)
		if dbRec.BlockNumber != onChain.BlockNumber {
			entityRef := fmt.Sprintf("%d:%s:%d:tx_inconsistency", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
			}
			observed := map[string]any{
				"block_number": dbRec.BlockNumber,
			}
			if err := recordMismatch(TypeSalaryClaimTxInconsistent, SeverityMedium, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}
	}

	// 8. Lifecycle Resolution: Resolve open exceptions within this range that are now fixed (Rule 13)
	prefix := fmt.Sprintf("%d:", r.cfg.ChainID)
	openExceptions, err := r.reconStore.GetOpenExceptionsByEntityRefPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to load open exceptions: %w", err)
	}

	now := time.Now().UTC()
	for _, openExc := range openExceptions {
		if !strings.HasPrefix(openExc.Type, "SALARY_CLAIM_") {
			continue
		}

		var expMap map[string]any
		if err := json.Unmarshal(openExc.Expected, &expMap); err != nil {
			continue
		}

		bNumFloat, ok := expMap["block_number"].(float64)
		if !ok {
			continue
		}
		excBlock := uint64(bNumFloat)

		// If this exception belongs to the block range we just checked
		if excBlock >= fromBlock && excBlock <= effectiveToBlock {
			// If it is no longer an active mismatch, resolve it
			if !activeMismatches[openExc.EntityRef] {
				if err := r.reconStore.ResolveException(ctx, openExc.ID, now); err != nil {
					return nil, fmt.Errorf("failed to resolve exception %d: %w", openExc.ID, err)
				}
				result.ExceptionsResolved++
				slog.Info("salary claim reconciliation exception resolved",
					"id", openExc.ID,
					"type", openExc.Type,
					"entity_ref", openExc.EntityRef,
				)
			}
		}
	}

	return result, nil
}

// ReconcileRecentWindow reconciles the most recent window of finalized blocks.
func (r *SalaryClaimReconciler) ReconcileRecentWindow(ctx context.Context, windowSize uint64) (*ReconciliationResult, error) {
	latestBlock, err := r.client.LatestBlock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest block: %w", err)
	}

	var safeTarget uint64 = latestBlock
	if r.cfg.ConfirmationDepth > 0 && latestBlock >= r.cfg.ConfirmationDepth {
		safeTarget = latestBlock - r.cfg.ConfirmationDepth
	}

	if safeTarget < r.cfg.StartBlock {
		return &ReconciliationResult{
			SafeTarget: safeTarget,
		}, nil
	}

	fromBlock := r.cfg.StartBlock
	if windowSize > 0 && safeTarget > windowSize {
		calculated := safeTarget - windowSize + 1
		if calculated > fromBlock {
			fromBlock = calculated
		}
	}

	return r.ReconcileRange(ctx, fromBlock, safeTarget)
}

// ReconcileNextBatch reconciles the next sequential batch of blocks based on the durable reconciliation cursor.
func (r *SalaryClaimReconciler) ReconcileNextBatch(ctx context.Context, batchSize uint64) (*ReconciliationResult, error) {
	latestBlock, err := r.client.LatestBlock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest block: %w", err)
	}

	var safeTarget uint64 = latestBlock
	if r.cfg.ConfirmationDepth > 0 && latestBlock >= r.cfg.ConfirmationDepth {
		safeTarget = latestBlock - r.cfg.ConfirmationDepth
	}

	fromBlock := r.cfg.StartBlock
	if r.checkpointStore != nil && r.cfg.ReconStreamID != "" {
		lastCheckpoint, ok, err := r.checkpointStore.GetCheckpoint(ctx, r.cfg.ChainID, r.cfg.ReconStreamID)
		if err != nil {
			return nil, fmt.Errorf("failed to query reconciliation checkpoint: %w", err)
		}
		if ok && lastCheckpoint >= fromBlock {
			fromBlock = lastCheckpoint + 1
		}
	}

	if fromBlock > safeTarget {
		return &ReconciliationResult{
			FromBlock:  fromBlock,
			ToBlock:    safeTarget,
			SafeTarget: safeTarget,
		}, nil
	}

	if batchSize == 0 {
		batchSize = 100
	}

	toBlock := fromBlock + batchSize - 1
	if toBlock > safeTarget {
		toBlock = safeTarget
	}

	res, err := r.ReconcileRange(ctx, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	if r.checkpointStore != nil && r.cfg.ReconStreamID != "" && res.ToBlock >= fromBlock {
		if err := r.checkpointStore.SaveCheckpoint(ctx, r.cfg.ChainID, r.cfg.ReconStreamID, res.ToBlock, ""); err != nil {
			return nil, fmt.Errorf("reconciliation failed to advance checkpoint to %d: %w", res.ToBlock, err)
		}
	}

	return res, nil
}
