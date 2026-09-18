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

	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/models"
)

// Domain Note:
// The configured ERC-20 token contract (e.g. WTF Token: 0x378AFb93CaDd39AFF154704d2D90Af8c401137E7)
// emits the standard Transfer(address indexed from, address indexed to, uint256 value) event
// whenever tokens move between addresses.
// The authoritative on-chain source for token transfers is the immutable sequence of Transfer
// event logs emitted in transaction receipts on the blockchain.
// The token transfer reconciler verifies these authoritative on-chain logs against the
// indexed PostgreSQL database records in token_transfers.

const (
	// Mismatch types for Token Transfer reconciliation
	TypeTokenTransferMissing        = "TOKEN_TRANSFER_MISSING"
	TypeTokenTransferAmountMismatch = "TOKEN_TRANSFER_AMOUNT_MISMATCH"
	TypeTokenTransferEntityMismatch = "TOKEN_TRANSFER_ENTITY_MISMATCH"
	TypeTokenTransferDuplicate      = "TOKEN_TRANSFER_DUPLICATE"
	TypeTokenTransferTxInconsistent = "TOKEN_TRANSFER_TX_INCONSISTENCY"
)

// TokenTransferStore retrieves indexed token transfer events from PostgreSQL.
type TokenTransferStore interface {
	GetTransfersByBlockRange(ctx context.Context, chainID int64, token common.Address, fromBlock, toBlock uint64) ([]*models.TokenTransfer, error)
}

// ERC20TransferDecoder decodes ERC-20 logs into normalized TokenTransfer records.
type ERC20TransferDecoder interface {
	DecodeTransfer(chainID int64, log types.Log, blockTimestamp time.Time) (*models.TokenTransfer, error)
	TokenAddress() common.Address
	TransferTopic() common.Hash
}

// TokenTransferReconcilerConfig holds configuration for the token transfer reconciler.
type TokenTransferReconcilerConfig struct {
	ChainID           int64
	TokenAddress      common.Address
	TokenStreamID     string // token indexer cursor stream ID for lag checks (e.g. "erc20_transfers_0x378afb93cadd39aff154704d2d90af8c401137e7")
	ReconStreamID     string // durable reconciliation cursor stream ID (e.g. "reconciliation_erc20_transfers_0x378afb93cadd39aff154704d2d90af8c401137e7")
	ConfirmationDepth uint64
	BlockWindow       uint64
	StartBlock        uint64
}

// TokenTransferReconciler executes reconciliation checks comparing on-chain ERC-20 Transfer events
// against indexed database records in PostgreSQL.
type TokenTransferReconciler struct {
	cfg             TokenTransferReconcilerConfig
	client          BlockchainClient
	decoder         ERC20TransferDecoder
	transferStore   TokenTransferStore
	reconStore      ReconciliationStore
	checkpointStore CheckpointStore
	retryer         *indexer.Retryer
}

// NewTokenTransferReconciler constructs a new TokenTransferReconciler.
func NewTokenTransferReconciler(
	cfg TokenTransferReconcilerConfig,
	client BlockchainClient,
	eventDecoder ERC20TransferDecoder,
	transferStore TokenTransferStore,
	reconStore ReconciliationStore,
	checkpointStore CheckpointStore,
) (*TokenTransferReconciler, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client cannot be nil")
	}
	if eventDecoder == nil {
		return nil, fmt.Errorf("event decoder cannot be nil")
	}
	if transferStore == nil {
		return nil, fmt.Errorf("token transfer store cannot be nil")
	}
	if reconStore == nil {
		return nil, fmt.Errorf("reconciliation store cannot be nil")
	}
	if cfg.TokenStreamID == "" {
		cfg.TokenStreamID = fmt.Sprintf("erc20_transfers_%s", strings.ToLower(cfg.TokenAddress.Hex()))
	}
	if cfg.ReconStreamID == "" {
		cfg.ReconStreamID = fmt.Sprintf("reconciliation_erc20_transfers_%s", strings.ToLower(cfg.TokenAddress.Hex()))
	}

	return &TokenTransferReconciler{
		cfg:             cfg,
		client:          client,
		decoder:         eventDecoder,
		transferStore:   transferStore,
		reconStore:      reconStore,
		checkpointStore: checkpointStore,
		retryer:         indexer.NewRetryer(indexer.DefaultRetryPolicy()),
	}, nil
}

// SetRetryPolicy customizes the RPC retry policy for the reconciler.
func (r *TokenTransferReconciler) SetRetryPolicy(policy indexer.RetryPolicy) {
	r.retryer = indexer.NewRetryer(policy)
}

type onChainTransferRecord struct {
	TxHash      common.Hash
	LogIndex    uint
	BlockNumber uint64
	Token       common.Address
	FromAddress common.Address
	ToAddress   common.Address
	Amount      *big.Int
}

// ReconcileRange performs token transfer reconciliation for an explicit block range [fromBlock, toBlock].
// It returns system errors directly without creating false reconciliation exceptions.
func (r *TokenTransferReconciler) ReconcileRange(ctx context.Context, fromBlock, toBlock uint64) (*ReconciliationResult, error) {
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
		slog.Info("reconciliation skipped unfinalized block range for token transfers",
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
		indexerCheckpoint, hasCheckpoint, cpErr = r.checkpointStore.GetCheckpoint(ctx, r.cfg.ChainID, r.cfg.TokenStreamID)
		if cpErr != nil {
			return nil, fmt.Errorf("reconciliation failed to retrieve token indexer checkpoint: %w", cpErr)
		}
	}

	// 3. Fetch authoritative logs from blockchain
	var logs []types.Log
	if r.retryer != nil {
		err = r.retryer.RetryRange(ctx, r.cfg.ReconStreamID, fromBlock, effectiveToBlock, func() error {
			var qErr error
			logs, qErr = r.client.GetLogs(ctx, r.cfg.TokenAddress, fromBlock, effectiveToBlock)
			return qErr
		})
	} else {
		logs, err = r.client.GetLogs(ctx, r.cfg.TokenAddress, fromBlock, effectiveToBlock)
	}
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to fetch token logs [%d, %d]: %w", fromBlock, effectiveToBlock, err)
	}

	// 4. Decode ERC-20 Transfer logs
	onChainMap := make(map[string]*onChainTransferRecord)
	var onChainList []*onChainTransferRecord

	transferTopic := r.decoder.TransferTopic()

	for _, log := range logs {
		if log.Removed {
			continue // Skip reorganized logs
		}

		// Token isolation: only process logs from the configured token address
		if !strings.EqualFold(log.Address.Hex(), r.cfg.TokenAddress.Hex()) {
			continue
		}

		if len(log.Topics) == 0 || log.Topics[0] != transferTopic {
			continue
		}

		decoded, err := r.decoder.DecodeTransfer(r.cfg.ChainID, log, time.Time{})
		if err != nil {
			// Skip non-standard or non-decodable logs
			continue
		}

		amount := new(big.Int)
		if decoded.Amount != nil {
			amount = new(big.Int).Set(decoded.Amount)
		}

		rec := &onChainTransferRecord{
			TxHash:      log.TxHash,
			LogIndex:    log.Index,
			BlockNumber: log.BlockNumber,
			Token:       decoded.Token,
			FromAddress: decoded.FromAddress,
			ToAddress:   decoded.ToAddress,
			Amount:      amount,
		}

		key := eventKey(log.TxHash, log.Index)
		onChainMap[key] = rec
		onChainList = append(onChainList, rec)
	}

	// 5. Fetch observed database records for the block range
	dbRecords, err := r.transferStore.GetTransfersByBlockRange(ctx, r.cfg.ChainID, r.cfg.TokenAddress, fromBlock, effectiveToBlock)
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to retrieve db token transfers [%d, %d]: %w", fromBlock, effectiveToBlock, err)
	}

	dbMap := make(map[string][]*models.TokenTransfer)
	for _, rec := range dbRecords {
		if rec.Removed {
			// Skip removed records from active matching
			continue
		}
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

	tokenHexLower := strings.ToLower(r.cfg.TokenAddress.Hex())

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
		slog.Warn("reconciliation mismatch detected and logged for token transfer",
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
			entityRef := fmt.Sprintf("%d:%s:%s:%d:duplicate", r.cfg.ChainID, tokenHexLower, first.TxHash.Hex(), first.LogIndex)
			var ids []int64
			for _, r := range recs {
				ids = append(ids, r.ID)
			}

			expected := map[string]any{
				"count":        1,
				"chain_id":     r.cfg.ChainID,
				"token":        tokenHexLower,
				"tx_hash":      first.TxHash.Hex(),
				"log_index":    first.LogIndex,
				"block_number": first.BlockNumber,
			}
			observed := map[string]any{
				"count":      len(recs),
				"record_ids": ids,
			}

			if err := recordMismatch(TypeTokenTransferDuplicate, SeverityHigh, entityRef, expected, observed); err != nil {
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

			// Lag Check: If token indexer hasn't reached this block yet, it is indexing lag, NOT an error
			if !hasCheckpoint || onChain.BlockNumber > indexerCheckpoint {
				slog.Debug("on-chain token transfer event not yet indexed due to indexing lag",
					"block", onChain.BlockNumber,
					"indexer_checkpoint", indexerCheckpoint,
					"tx_hash", onChain.TxHash.Hex(),
				)
				continue
			}

			// Genuine Missing Event: indexer checkpoint >= blockNumber, but row is missing!
			entityRef := fmt.Sprintf("%d:%s:%s:%d:missing", r.cfg.ChainID, tokenHexLower, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"token":        tokenHexLower,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"from_address": onChain.FromAddress.Hex(),
				"to_address":   onChain.ToAddress.Hex(),
				"amount":       onChain.Amount.String(),
			}
			observed := map[string]any{
				"status": "not_found",
			}

			if err := recordMismatch(TypeTokenTransferMissing, SeverityHigh, entityRef, expected, observed); err != nil {
				return nil, err
			}
			continue
		}

		// Case B: Record exists in database, compare fields
		dbRec := dbRecs[0]

		// 1. Entity Mismatch: from or to address differs (Critical)
		fromDiffers := !strings.EqualFold(dbRec.FromAddress.Hex(), onChain.FromAddress.Hex())
		toDiffers := !strings.EqualFold(dbRec.ToAddress.Hex(), onChain.ToAddress.Hex())
		if fromDiffers || toDiffers {
			entityRef := fmt.Sprintf("%d:%s:%s:%d:entity_mismatch", r.cfg.ChainID, tokenHexLower, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"token":        tokenHexLower,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"from_address": onChain.FromAddress.Hex(),
				"to_address":   onChain.ToAddress.Hex(),
			}
			observed := map[string]any{
				"from_address": dbRec.FromAddress.Hex(),
				"to_address":   dbRec.ToAddress.Hex(),
			}
			if err := recordMismatch(TypeTokenTransferEntityMismatch, SeverityCritical, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}

		// 2. Amount Mismatch: amount differs (High)
		if dbRec.Amount == nil || dbRec.Amount.Cmp(onChain.Amount) != 0 {
			entityRef := fmt.Sprintf("%d:%s:%s:%d:amount_mismatch", r.cfg.ChainID, tokenHexLower, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"token":        tokenHexLower,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
				"from_address": onChain.FromAddress.Hex(),
				"to_address":   onChain.ToAddress.Hex(),
				"amount":       onChain.Amount.String(),
			}
			observed := map[string]any{
				"amount": safeBigIntString(dbRec.Amount),
			}
			if err := recordMismatch(TypeTokenTransferAmountMismatch, SeverityHigh, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}

		// 3. Tx Inconsistency: Block number mismatch or tx hash mismatch (Medium)
		if dbRec.BlockNumber != onChain.BlockNumber || !strings.EqualFold(dbRec.TxHash.Hex(), onChain.TxHash.Hex()) {
			entityRef := fmt.Sprintf("%d:%s:%s:%d:tx_inconsistency", r.cfg.ChainID, tokenHexLower, onChain.TxHash.Hex(), onChain.LogIndex)
			expected := map[string]any{
				"chain_id":     r.cfg.ChainID,
				"token":        tokenHexLower,
				"tx_hash":      onChain.TxHash.Hex(),
				"log_index":    onChain.LogIndex,
				"block_number": onChain.BlockNumber,
			}
			observed := map[string]any{
				"block_number": dbRec.BlockNumber,
				"tx_hash":      dbRec.TxHash.Hex(),
			}
			if err := recordMismatch(TypeTokenTransferTxInconsistent, SeverityMedium, entityRef, expected, observed); err != nil {
				return nil, err
			}
		}
	}

	// 8. Lifecycle Resolution: Resolve open exceptions within this range that are now fixed (Rule 13)
	prefix := fmt.Sprintf("%d:%s:", r.cfg.ChainID, tokenHexLower)
	openExceptions, err := r.reconStore.GetOpenExceptionsByEntityRefPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("reconciliation failed to load open exceptions: %w", err)
	}

	now := time.Now().UTC()
	for _, openExc := range openExceptions {
		if !strings.HasPrefix(openExc.Type, "TOKEN_TRANSFER_") {
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
				slog.Info("token transfer reconciliation exception resolved",
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
func (r *TokenTransferReconciler) ReconcileRecentWindow(ctx context.Context, windowSize uint64) (*ReconciliationResult, error) {
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
func (r *TokenTransferReconciler) ReconcileNextBatch(ctx context.Context, batchSize uint64) (*ReconciliationResult, error) {
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
