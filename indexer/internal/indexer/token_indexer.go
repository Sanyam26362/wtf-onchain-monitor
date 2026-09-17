package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
)

// TokenIndexer manages the ingestion of ERC-20 Transfer events from the blockchain into PostgreSQL.
type TokenIndexer struct {
	client            blockchain.BlockchainClient
	decoder           *decoder.ERC20Decoder
	persistence       *persistence.Postgres
	chainID           int64
	tokenAddress      common.Address
	streamID          string
	startBlock        uint64
	batchSize         uint64
	confirmationDepth uint64
}

// NewTokenIndexer creates a new generic ERC-20 token indexer.
func NewTokenIndexer(
	client blockchain.BlockchainClient,
	eventDecoder *decoder.ERC20Decoder,
	db *persistence.Postgres,
	chainID int64,
	startBlock uint64,
	batchSize uint64,
	confirmationDepth uint64,
	streamID string,
) (*TokenIndexer, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client is nil")
	}
	if eventDecoder == nil {
		return nil, fmt.Errorf("event decoder is nil")
	}
	if db == nil {
		return nil, fmt.Errorf("persistence layer is nil")
	}
	if batchSize == 0 {
		batchSize = 50
	}
	if streamID == "" || streamID == "erc20_transfers" {
		streamID = fmt.Sprintf("erc20_transfers_%s", strings.ToLower(eventDecoder.TokenAddress().Hex()))
	}

	return &TokenIndexer{
		client:            client,
		decoder:           eventDecoder,
		persistence:       db,
		chainID:           chainID,
		tokenAddress:      eventDecoder.TokenAddress(),
		streamID:          streamID,
		startBlock:        startBlock,
		batchSize:         batchSize,
		confirmationDepth: confirmationDepth,
	}, nil
}

// TokenAddress returns the configured token address.
func (ti *TokenIndexer) TokenAddress() common.Address {
	return ti.tokenAddress
}

// StreamID returns the durable checkpoint stream identifier.
func (ti *TokenIndexer) StreamID() string {
	return ti.streamID
}

// GetEffectiveStartBlock determines the block to resume from based on durable checkpoints.
func (ti *TokenIndexer) GetEffectiveStartBlock(ctx context.Context) (uint64, error) {
	lastBlock, found, err := ti.persistence.GetCheckpoint(ctx, ti.chainID, ti.streamID)
	if err != nil {
		return 0, fmt.Errorf("failed to get checkpoint for stream %s: %w", ti.streamID, err)
	}

	if found {
		resumeBlock := lastBlock + 1
		if resumeBlock < ti.startBlock {
			slog.Warn("checkpoint block is before configured token start block, starting from configured block",
				"stream_id", ti.streamID,
				"checkpoint_block", lastBlock,
				"start_block", ti.startBlock,
			)
			resumeBlock = ti.startBlock
		}
		slog.Info("resuming token indexer from durable checkpoint",
			"stream_id", ti.streamID,
			"checkpoint_block", lastBlock,
			"resume_block", resumeBlock,
		)
		return resumeBlock, nil
	}

	slog.Info("no existing checkpoint found, starting from configured block",
		"stream_id", ti.streamID,
		"start_block", ti.startBlock,
	)
	return ti.startBlock, nil
}

// IndexRange fetches and persists ERC-20 Transfer logs within [fromBlock, toBlock].
func (ti *TokenIndexer) IndexRange(
	ctx context.Context,
	fromBlock uint64,
	toBlock uint64,
) ([]*models.TokenTransfer, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range: from %d is greater than to %d", fromBlock, toBlock)
	}

	slog.Info("querying ERC-20 Transfer logs",
		"token", ti.tokenAddress.Hex(),
		"from_block", fromBlock,
		"to_block", toBlock,
		"batch_size", toBlock-fromBlock+1,
	)

	logs, err := ti.client.GetTokenLogs(
		ctx,
		ti.tokenAddress,
		ti.decoder.TransferTopic(),
		fromBlock,
		toBlock,
	)
	if err != nil {
		slog.Error("failed to fetch token logs from RPC",
			"token", ti.tokenAddress.Hex(),
			"from_block", fromBlock,
			"to_block", toBlock,
			"error", err,
		)
		return nil, err
	}

	if ti.decoder.HasApproval() {
		apprTopic, ok := ti.decoder.ApprovalTopic()
		if ok {
			apprLogs, err := ti.client.GetTokenLogs(
				ctx,
				ti.tokenAddress,
				apprTopic,
				fromBlock,
				toBlock,
			)
			if err != nil {
				slog.Error("failed to fetch token approval logs from RPC",
					"token", ti.tokenAddress.Hex(),
					"from_block", fromBlock,
					"to_block", toBlock,
					"error", err,
				)
				return nil, err
			}
			if len(apprLogs) > 0 {
				logs = append(logs, apprLogs...)
				sort.SliceStable(logs, func(i, j int) bool {
					if logs[i].BlockNumber == logs[j].BlockNumber {
						return logs[i].Index < logs[j].Index
					}
					return logs[i].BlockNumber < logs[j].BlockNumber
				})
			}
		}
	}

	slog.Info("fetched logs from blockchain",
		"token", ti.tokenAddress.Hex(),
		"from_block", fromBlock,
		"to_block", toBlock,
		"count", len(logs),
	)

	// 1. Resolve block timestamps with caching
	timestampCache := make(map[uint64]time.Time)
	for _, log := range logs {
		if _, ok := timestampCache[log.BlockNumber]; !ok {
			ts, err := ti.client.BlockTimestamp(ctx, log.BlockNumber)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch timestamp for block %d: %w", log.BlockNumber, err)
			}
			timestampCache[log.BlockNumber] = time.Unix(int64(ts), 0).UTC()
		}
	}

	// 2. Decode transfers and approvals, and collect unique transaction hashes
	transfers := make([]*models.TokenTransfer, 0, len(logs))
	rawEvents := make([]*persistence.RawChainEventInput, 0, len(logs))
	txHashMap := make(map[common.Hash]bool)

	for _, log := range logs {
		blockTime := timestampCache[log.BlockNumber]

		if len(log.Topics) > 0 && log.Topics[0] == ti.decoder.TransferTopic() {
			transfer, err := ti.decoder.DecodeTransfer(ti.chainID, log, blockTime)
			if err != nil {
				return nil, fmt.Errorf("decode transfer log at block %d tx %s log index %d: %w",
					log.BlockNumber, log.TxHash.Hex(), log.Index, err)
			}
			transfers = append(transfers, transfer)
			txHashMap[log.TxHash] = true

			rawEvents = append(rawEvents, &persistence.RawChainEventInput{
				ContractAddress: ti.tokenAddress,
				EventName:       "Transfer",
				TxHash:          log.TxHash,
				BlockNumber:     log.BlockNumber,
				BlockTimestamp:  blockTime,
				LogIndex:        log.Index,
				Removed:         log.Removed,
				RawData: map[string]interface{}{
					"from":   transfer.FromAddress.Hex(),
					"to":     transfer.ToAddress.Hex(),
					"amount": transfer.Amount.String(),
				},
			})
		} else if ti.decoder.HasApproval() {
			apprTopic, ok := ti.decoder.ApprovalTopic()
			if ok && len(log.Topics) > 0 && log.Topics[0] == apprTopic {
				approval, err := ti.decoder.DecodeApproval(ti.chainID, log, blockTime)
				if err != nil {
					return nil, fmt.Errorf("decode approval log at block %d tx %s log index %d: %w",
						log.BlockNumber, log.TxHash.Hex(), log.Index, err)
				}
				txHashMap[log.TxHash] = true

				rawEvents = append(rawEvents, &persistence.RawChainEventInput{
					ContractAddress: ti.tokenAddress,
					EventName:       "Approval",
					TxHash:          log.TxHash,
					BlockNumber:     log.BlockNumber,
					BlockTimestamp:  blockTime,
					LogIndex:        log.Index,
					Removed:         log.Removed,
					RawData: map[string]interface{}{
						"owner":   approval.Owner.Hex(),
						"spender": approval.Spender.Hex(),
						"value":   approval.Value.String(),
					},
				})
			}
		}
	}

	// 3. Identify which transactions already exist in the database
	txHashList := make([]string, 0, len(txHashMap))
	for h := range txHashMap {
		txHashList = append(txHashList, h.Hex())
	}
	existingTxs, err := ti.persistence.GetExistingTxHashes(ctx, ti.chainID, txHashList)
	if err != nil {
		slog.Warn("could not query existing tx hashes, will fetch all", "error", err)
		existingTxs = make(map[string]bool)
	}

	var neededHashes []common.Hash
	for h := range txHashMap {
		if !existingTxs[h.Hex()] {
			neededHashes = append(neededHashes, h)
		}
	}

	// 4. Fetch metadata for needed transactions using a controlled worker pool
	txMetaChan := make(chan *blockchain.TransactionMetadata, len(neededHashes))
	errChan := make(chan error, len(neededHashes))

	workerCount := 3
	if len(neededHashes) < workerCount {
		workerCount = len(neededHashes)
	}

	var uniqueTxs []*blockchain.TransactionMetadata
	if workerCount > 0 {
		hashChan := make(chan common.Hash, len(neededHashes))
		for _, h := range neededHashes {
			hashChan <- h
		}
		close(hashChan)

		for w := 0; w < workerCount; w++ {
			go func() {
				for h := range hashChan {
					time.Sleep(30 * time.Millisecond) // smooth out RPC bursts
					meta, metaErr := ti.client.TransactionMetadata(ctx, h)
					if metaErr != nil {
						errChan <- fmt.Errorf("fetch metadata for tx %s: %w", h.Hex(), metaErr)
						return
					}
					txMetaChan <- meta
				}
			}()
		}

		for i := 0; i < len(neededHashes); i++ {
			select {
			case err := <-errChan:
				return nil, err
			case meta := <-txMetaChan:
				uniqueTxs = append(uniqueTxs, meta)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// Persist transactions, raw events, normalized transfers, and checkpoint atomically
	err = ti.persistence.SaveTokenBatch(
		ctx,
		ti.chainID,
		ti.streamID,
		uniqueTxs,
		rawEvents,
		transfers,
		toBlock,
		"",
	)
	if err != nil {
		slog.Error("failed to persist token batch",
			"stream_id", ti.streamID,
			"from_block", fromBlock,
			"to_block", toBlock,
			"transfers_count", len(transfers),
			"error", err,
		)
		return nil, err
	}

	slog.Info("successfully processed and persisted token range",
		"token", ti.tokenAddress.Hex(),
		"stream_id", ti.streamID,
		"from_block", fromBlock,
		"to_block", toBlock,
		"transfers_indexed", len(transfers),
		"checkpoint_advanced_to", toBlock,
	)

	return transfers, nil
}

// ProcessNextBatch determines the next bounded chunk and indexes it.
// It respects confirmation depth and advances checkpoints only upon success.
func (ti *TokenIndexer) ProcessNextBatch(ctx context.Context) (bool, uint64, uint64, int, error) {
	fromBlock, err := ti.GetEffectiveStartBlock(ctx)
	if err != nil {
		return false, 0, 0, 0, err
	}

	latestBlock, err := ti.client.LatestBlock(ctx)
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("failed to fetch latest block: %w", err)
	}

	var safeBlock uint64 = latestBlock
	if ti.confirmationDepth > 0 && latestBlock >= ti.confirmationDepth {
		safeBlock = latestBlock - ti.confirmationDepth
	}

	if fromBlock > safeBlock {
		slog.Info("token indexer up to date, no new blocks to index",
			"from_block", fromBlock,
			"safe_block", safeBlock,
			"latest_block", latestBlock,
			"confirmation_depth", ti.confirmationDepth,
		)
		return false, fromBlock, safeBlock, 0, nil
	}

	toBlock := fromBlock + ti.batchSize - 1
	if toBlock > safeBlock {
		toBlock = safeBlock
	}

	transfers, err := ti.IndexRange(ctx, fromBlock, toBlock)
	if err != nil {
		return false, fromBlock, toBlock, 0, err
	}

	return true, fromBlock, toBlock, len(transfers), nil
}

// RunBackfill indexes bounded chunks from GetEffectiveStartBlock up to targetBlock.
// If targetBlock is 0, it computes and uses the latest safe block.
func (ti *TokenIndexer) RunBackfill(ctx context.Context, targetBlock uint64) (uint64, int, error) {
	fromBlock, err := ti.GetEffectiveStartBlock(ctx)
	if err != nil {
		return 0, 0, err
	}

	latestBlock, err := ti.client.LatestBlock(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch latest block: %w", err)
	}

	var safeBlock uint64 = latestBlock
	if ti.confirmationDepth > 0 && latestBlock >= ti.confirmationDepth {
		safeBlock = latestBlock - ti.confirmationDepth
	}

	effectiveTarget := safeBlock
	if targetBlock > 0 && targetBlock <= safeBlock {
		effectiveTarget = targetBlock
	}

	if fromBlock > effectiveTarget {
		slog.Info("token indexer up to date, no new blocks to index",
			"stream_id", ti.streamID,
			"from_block", fromBlock,
			"target_block", effectiveTarget,
		)
		return effectiveTarget, 0, nil
	}

	slog.Info("Starting token historical backfill",
		"token", ti.tokenAddress.Hex(),
		"stream_id", ti.streamID,
		"from_block", fromBlock,
		"target_block", effectiveTarget,
		"batch_size", ti.batchSize,
	)

	totalTransfers := 0
	lastIndexedBlock := fromBlock - 1

	for from := fromBlock; from <= effectiveTarget; {
		to := from + ti.batchSize - 1
		if to > effectiveTarget {
			to = effectiveTarget
		}

		slog.Info("Processing token block range",
			"stream_id", ti.streamID,
			"from", from,
			"to", to,
		)

		transfers, err := ti.IndexRange(ctx, from, to)
		if err != nil {
			slog.Error("Failed to index token block range",
				"stream_id", ti.streamID,
				"from", from,
				"to", to,
				"error", err,
			)
			return lastIndexedBlock, totalTransfers, fmt.Errorf("token index range [%d, %d]: %w", from, to, err)
		}

		totalTransfers += len(transfers)
		lastIndexedBlock = to
		from = to + 1
	}

	slog.Info("Token historical backfill completed",
		"stream_id", ti.streamID,
		"last_indexed_block", lastIndexedBlock,
		"total_transfers", totalTransfers,
	)

	return lastIndexedBlock, totalTransfers, nil
}
