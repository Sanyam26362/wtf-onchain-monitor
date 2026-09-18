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
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// Domain Note:
// The MonthlyPayroll smart contract (0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC)
// does not expose an iterable historical funding array or transaction ledger in contract storage;
// it only maintains running balances and state mappings (employers, employees, payrolls).
// Therefore, the authoritative on-chain source for individual funding transactions is
// the immutable PayrollFunded event emitted in transaction receipts on the blockchain.
// The payroll reconciliation worker verifies these authoritative on-chain logs against the
// indexed PostgreSQL database records, distinguishing indexing lag from genuine discrepancies.

const (
	// Mismatch types for Payroll Funding reconciliation
	TypePayrollFundingMissing        = "PAYROLL_FUNDING_MISSING"
	TypePayrollFundingAmountMismatch = "PAYROLL_FUNDING_AMOUNT_MISMATCH"
	TypePayrollFundingEntityMismatch = "PAYROLL_FUNDING_ENTITY_MISMATCH"
	TypePayrollFundingDuplicate      = "PAYROLL_FUNDING_DUPLICATE"
	TypePayrollFundingTxInconsistent = "PAYROLL_FUNDING_TX_INCONSISTENCY"

	// Severities permitted by database check constraint
	SeverityLow      = "low"
	SeverityMedium   = "medium"
	SeverityHigh     = "high"
	SeverityCritical = "critical"

	// Exception lifecycle statuses
	StatusOpen     = "open"
	StatusResolved = "resolved"
)

// BlockchainClient defines methods required by the reconciler to fetch block and log data.
type BlockchainClient interface {
	LatestBlock(ctx context.Context) (uint64, error)
	GetLogs(ctx context.Context, contractAddress common.Address, fromBlock uint64, toBlock uint64) ([]types.Log, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
}

// EventDecoder parses EVM logs into structured decoded events.
type EventDecoder interface {
	Decode(log types.Log) (*decoder.DecodedEvent, error)
}

// PayrollFundingStore retrieves indexed funding events from PostgreSQL.
type PayrollFundingStore interface {
	GetFundingsByBlockRange(ctx context.Context, chainID int64, fromBlock, toBlock uint64) ([]*models.PayrollFunding, error)
}

// ReconciliationStore persists and manages reconciliation exceptions in PostgreSQL.
type ReconciliationStore interface {
	CreateException(ctx context.Context, exc *models.ReconciliationException) error
	GetOpenExceptionByEntityRef(ctx context.Context, entityRef string) (*models.ReconciliationException, error)
	ResolveException(ctx context.Context, id int64, resolvedAt time.Time) error
	GetOpenExceptionsByEntityRefPrefix(ctx context.Context, prefix string) ([]*models.ReconciliationException, error)
}

// CheckpointStore reads and updates durable checkpoints.
type CheckpointStore interface {
	GetCheckpoint(ctx context.Context, chainID int64, streamID string) (uint64, bool, error)
	SaveCheckpoint(ctx context.Context, chainID int64, streamID string, blockNumber uint64, blockHash string) error
}

// PayrollReconcilerConfig holds configuration for the payroll funding reconciler.
type PayrollReconcilerConfig struct {
	ChainID           int64
	ContractAddress   common.Address
	PayrollStreamID   string
	ReconStreamID     string
	ConfirmationDepth uint64
	BlockWindow       uint64
	StartBlock        uint64
}

// ReconciliationResult summarizes the outcome of a reconciliation pass.
type ReconciliationResult struct {
	FromBlock          uint64 `json:"from_block"`
	ToBlock            uint64 `json:"to_block"`
	SafeTarget         uint64 `json:"safe_target"`
	IndexerCheckpoint  uint64 `json:"indexer_checkpoint"`
	OnChainEventsCount int    `json:"on_chain_events_count"`
	DBRecordsCount     int    `json:"db_records_count"`
	MismatchesDetected int    `json:"mismatches_detected"`
	ExceptionsCreated  int    `json:"exceptions_created"`
	ExceptionsResolved int    `json:"exceptions_resolved"`
	ExceptionsSkipped  int    `json:"exceptions_skipped"`
}

// PayrollReconciler executes reconciliation checks comparing on-chain PayrollFunded events
// against database records in PostgreSQL.
type PayrollReconciler struct {
	cfg             PayrollReconcilerConfig
	client          BlockchainClient
	decoder         EventDecoder
	payrollStore    PayrollFundingStore
	reconStore      ReconciliationStore
	checkpointStore CheckpointStore
	retryer         *indexer.Retryer
}

// NewPayrollReconciler constructs a new PayrollReconciler.
func NewPayrollReconciler(
	cfg PayrollReconcilerConfig,
	client BlockchainClient,
	eventDecoder EventDecoder,
	payrollStore PayrollFundingStore,
	reconStore ReconciliationStore,
	checkpointStore CheckpointStore,
) (*PayrollReconciler, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client cannot be nil")
	}
	if eventDecoder == nil {
		return nil, fmt.Errorf("event decoder cannot be nil")
	}
	if payrollStore == nil {
		return nil, fmt.Errorf("payroll store cannot be nil")
	}
	if reconStore == nil {
		return nil, fmt.Errorf("reconciliation store cannot be nil")
	}
	if cfg.PayrollStreamID == "" {
		cfg.PayrollStreamID = "monthly_payroll"
	}
	if cfg.ReconStreamID == "" {
		cfg.ReconStreamID = "reconciliation_payroll_funding"
	}

	return &PayrollReconciler{
		cfg:             cfg,
		client:          client,
		decoder:         eventDecoder,
		payrollStore:    payrollStore,
		reconStore:      reconStore,
		checkpointStore: checkpointStore,
		retryer:         indexer.NewRetryer(indexer.DefaultRetryPolicy()),
	}, nil
}

// SetRetryPolicy customizes the RPC retry policy for the reconciler.
func (r *PayrollReconciler) SetRetryPolicy(policy indexer.RetryPolicy) {
	r.retryer = indexer.NewRetryer(policy)
}

type onChainFundingRecord struct {
	TxHash         common.Hash
	LogIndex       uint
	BlockNumber    uint64
	Employer       common.Address
	Employee       common.Address
	AmountPaid     *big.Int
	Fee            *big.Int
	AmountCredited *big.Int
}

// ReconcileRange performs payroll funding reconciliation for an explicit block range [fromBlock, toBlock].
// It returns system errors directly without creating false reconciliation exceptions.
func (r *PayrollReconciler) ReconcileRange(ctx context.Context, fromBlock, toBlock uint64) (*ReconciliationResult, error) {
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

	// If the entire requested window is beyond the safe target, skip as unfinalized (Rule H)
	if fromBlock > safeTarget {
		slog.Info("reconciliation skipped unfinalized block range",
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

	// 2. Fetch indexer checkpoint to identify indexing lag
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

	// 4. Decode PayrollFunded logs
	onChainMap := make(map[string]*onChainFundingRecord)
	var onChainList []*onChainFundingRecord

	for _, log := range logs {
		if log.Removed {
			continue // Skip reorganized logs
		}

		decoded, err := r.decoder.Decode(log)
		if err != nil {
			// Skip logs not matching the MonthlyPayroll ABI or not decodable
			continue
		}

		if decoded.Type != decoder.EventPayrollFunded {
			continue
		}

		event, ok := decoded.Data.(*abi.ABIPayrollFundedEvent)
		if !ok || event == nil {
			continue
		}

		amountPaid := new(big.Int)
		if event.AmountPaid != nil {
			amountPaid = new(big.Int).Set(event.AmountPaid)
		}
		fee := new(big.Int)
		if event.Fee != nil {
			fee = new(big.Int).Set(event.Fee)
		}
		amountCredited := new(big.Int)
		if event.AmountCredited != nil {
			amountCredited = new(big.Int).Set(event.AmountCredited)
		}

		rec := &onChainFundingRecord{
			TxHash:         log.TxHash,
			LogIndex:       log.Index,
			BlockNumber:    log.BlockNumber,
			Employer:       event.Employer,
			Employee:       event.Employee,
			AmountPaid:     amountPaid,
			Fee:            fee,
			AmountCredited: amountCredited,
		}

		key := eventKey(log.TxHash, log.Index)
		onChainMap[key] = rec
		onChainList = append(onChainList, rec)
	}

	// 5. Fetch observed database records for the block range
	dbRecords, err := r.payrollStore.GetFundingsByBlockRange(ctx, r.cfg.ChainID, fromBlock, effectiveToBlock)
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to retrieve db records [%d, %d]: %w", fromBlock, effectiveToBlock, err)
	}

	dbMap := make(map[string][]*models.PayrollFunding)
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
		slog.Warn("reconciliation mismatch detected and logged",
			"type", excType,
			"severity", severity,
			"entity_ref", entityRef,
		)
		return nil
	}

	// 6. Check for duplicate database records (Rule D)
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

			if err := recordMismatch(TypePayrollFundingDuplicate, SeverityHigh, entityRef, expected, observed); err != nil {
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
				slog.Debug("on-chain event not yet indexed due to indexing lag",
					"block", onChain.BlockNumber,
					"indexer_checkpoint", indexerCheckpoint,
					"tx_hash", onChain.TxHash.Hex(),
				)
				continue
			}

			// Genuine Missing Event: indexer checkpoint >= blockNumber, but row is missing!
			entityRef := fmt.Sprintf("%d:%s:%d:missing", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":        r.cfg.ChainID,
				"tx_hash":         onChain.TxHash.Hex(),
				"log_index":       onChain.LogIndex,
				"block_number":    onChain.BlockNumber,
				"employer":        onChain.Employer.Hex(),
				"employee":        onChain.Employee.Hex(),
				"amount_paid":     onChain.AmountPaid.String(),
				"fee":             onChain.Fee.String(),
				"amount_credited": onChain.AmountCredited.String(),
			}
			observed := map[string]any{
				"status": "not_found",
			}

			if err := recordMismatch(TypePayrollFundingMissing, SeverityHigh, entityRef, expected, observed); err != nil {
				return nil, err
			}
			continue
		}

		// Case B: Record exists in database, compare fields
		dbRec := dbRecs[0]

		// 1. Entity Mismatch: Employer or Employee addresses differ (Critical)
		if !strings.EqualFold(dbRec.Employer.Hex(), onChain.Employer.Hex()) ||
			!strings.EqualFold(dbRec.Employee.Hex(), onChain.Employee.Hex()) {
			entityRef := fmt.Sprintf("%d:%s:%d:entity_mismatch", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"employer":     onChain.Employer.Hex(),
				"employee":     onChain.Employee.Hex(),
			}
			observed := map[string]any{
				"employer": dbRec.Employer.Hex(),
				"employee": dbRec.Employee.Hex(),
			}
			if err := recordMismatch(TypePayrollFundingEntityMismatch, SeverityCritical, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}

		// 2. Amount Mismatch: amount_paid, fee, or amount_credited differ (High)
		amountMismatch := false
		if dbRec.AmountPaid == nil || dbRec.AmountPaid.Cmp(onChain.AmountPaid) != 0 {
			amountMismatch = true
		}
		if dbRec.Fee == nil || dbRec.Fee.Cmp(onChain.Fee) != 0 {
			amountMismatch = true
		}
		if dbRec.AmountCredited == nil || dbRec.AmountCredited.Cmp(onChain.AmountCredited) != 0 {
			amountMismatch = true
		}

		if amountMismatch {
			entityRef := fmt.Sprintf("%d:%s:%d:amount_mismatch", r.cfg.ChainID, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":        r.cfg.ChainID,
				"tx_hash":         onChain.TxHash.Hex(),
				"log_index":       onChain.LogIndex,
				"block_number":    onChain.BlockNumber,
				"amount_paid":     onChain.AmountPaid.String(),
				"fee":             onChain.Fee.String(),
				"amount_credited": onChain.AmountCredited.String(),
			}
			observed := map[string]any{
				"amount_paid":     safeBigIntString(dbRec.AmountPaid),
				"fee":             safeBigIntString(dbRec.Fee),
				"amount_credited": safeBigIntString(dbRec.AmountCredited),
			}
			if err := recordMismatch(TypePayrollFundingAmountMismatch, SeverityHigh, entityRef, expected, observed); err != nil {
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
			if err := recordMismatch(TypePayrollFundingTxInconsistent, SeverityMedium, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}
	}

	// 8. Lifecycle Resolution: Resolve open exceptions within this range that are now fixed (Rule F)
	prefix := fmt.Sprintf("%d:", r.cfg.ChainID)
	openExceptions, err := r.reconStore.GetOpenExceptionsByEntityRefPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to load open exceptions: %w", err)
	}

	now := time.Now().UTC()
	for _, openExc := range openExceptions {
		if !strings.HasPrefix(openExc.Type, "PAYROLL_FUNDING_") {
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
				slog.Info("reconciliation exception resolved",
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
func (r *PayrollReconciler) ReconcileRecentWindow(ctx context.Context, windowSize uint64) (*ReconciliationResult, error) {
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
func (r *PayrollReconciler) ReconcileNextBatch(ctx context.Context, batchSize uint64) (*ReconciliationResult, error) {
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

func eventKey(txHash common.Hash, logIndex uint) string {
	return fmt.Sprintf("%s:%d", strings.ToLower(txHash.Hex()), logIndex)
}

func safeBigIntString(val *big.Int) string {
	if val == nil {
		return "0"
	}
	return val.String()
}
