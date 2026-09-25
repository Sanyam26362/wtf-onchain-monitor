package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/redis/go-redis/v9"

	wtfabi "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
)

// EscrowPersistence is the storage interface required by EscrowIndexer.
type EscrowPersistence interface {
	GetCheckpoint(ctx context.Context, chainID int64, streamID string) (uint64, bool, error)
	SaveEscrowBatch(ctx context.Context, chainID int64, streamID string, events []*models.EscrowEvent, checkpointBlock uint64, checkpointHash string) error
}

// EscrowIndexer indexes all events emitted by the WTFEscrow contract.
type EscrowIndexer struct {
	client   blockchain.BlockchainClient
	db       EscrowPersistence
	redis    *redis.Client
	abi      gethabi.ABI
	contract common.Address
	chainID  int64
	streamID string
	start    uint64
	batch    uint64
	retryer  *Retryer
}

// NewEscrowIndexer creates an EscrowIndexer bound to the given contract address.
func NewEscrowIndexer(
	client blockchain.BlockchainClient,
	db EscrowPersistence,
	redisClient *redis.Client,
	contract common.Address,
	chainID int64,
	streamID string,
	startBlock uint64,
	batchSize uint64,
) (*EscrowIndexer, error) {
	if client == nil {
		return nil, fmt.Errorf("escrow indexer: blockchain client is nil")
	}
	if db == nil {
		return nil, fmt.Errorf("escrow indexer: persistence layer is nil")
	}

	parsedABI, err := gethabi.JSON(strings.NewReader(wtfabi.WTFEscrowABI))
	if err != nil {
		return nil, fmt.Errorf("escrow indexer: parse ABI: %w", err)
	}

	if streamID == "" {
		streamID = "wtf_escrow"
	}
	if batchSize == 0 {
		batchSize = 50
	}

	return &EscrowIndexer{
		client:   client,
		db:       db,
		redis:    redisClient,
		abi:      parsedABI,
		contract: contract,
		chainID:  chainID,
		streamID: streamID,
		start:    startBlock,
		batch:    batchSize,
		retryer:  NewRetryer(DefaultRetryPolicy()),
	}, nil
}

// SetRetryPolicy replaces the retry policy on the indexer.
func (ei *EscrowIndexer) SetRetryPolicy(p RetryPolicy) {
	ei.retryer = NewRetryer(p)
}

// GetEffectiveStartBlock resolves the block to resume from.
func (ei *EscrowIndexer) GetEffectiveStartBlock(ctx context.Context) (uint64, error) {
	last, found, err := ei.db.GetCheckpoint(ctx, ei.chainID, ei.streamID)
	if err != nil {
		return 0, fmt.Errorf("escrow checkpoint: %w", err)
	}
	if found {
		resume := last + 1
		if resume < ei.start {
			resume = ei.start
		}
		slog.Info("resuming escrow indexer from checkpoint",
			"stream_id", ei.streamID,
			"checkpoint_block", last,
			"resume_block", resume,
		)
		return resume, nil
	}
	slog.Info("no escrow checkpoint found, starting from configured block",
		"stream_id", ei.streamID,
		"start_block", ei.start,
	)
	return ei.start, nil
}

// IndexRange fetches and processes logs for the specified block range [fromBlock, toBlock].
func (ei *EscrowIndexer) IndexRange(ctx context.Context, fromBlock, toBlock uint64) ([]*models.EscrowEvent, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("escrow index range: from %d > to %d", fromBlock, toBlock)
	}

	logs, err := ei.client.GetLogs(ctx, ei.contract, fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("escrow fetch logs [%d, %d]: %w", fromBlock, toBlock, err)
	}

	// Cache block timestamps
	tsCache := make(map[uint64]time.Time)
	getTimestamp := func(blockNum uint64) (time.Time, error) {
		if t, ok := tsCache[blockNum]; ok {
			return t, nil
		}
		ts, err := ei.client.BlockTimestamp(ctx, blockNum)
		if err != nil {
			return time.Time{}, fmt.Errorf("block timestamp for %d: %w", blockNum, err)
		}
		t := time.Unix(int64(ts), 0).UTC()
		tsCache[blockNum] = t
		return t, nil
	}

	events := make([]*models.EscrowEvent, 0, len(logs))
	for _, log := range logs {
		ev, decErr := ei.ProcessLog(log, getTimestamp)
		if decErr != nil {
			// Unknown event topic — skip gracefully
			slog.Warn("escrow indexer: unknown or undecodable log, skipping",
				"tx", log.TxHash.Hex(),
				"block", log.BlockNumber,
				"log_index", log.Index,
				"error", decErr,
			)
			continue
		}
		events = append(events, ev)
	}

	// Fetch block header for checkpoint hash
	header, err := ei.client.BlockHeader(ctx, toBlock)
	if err != nil {
		return nil, fmt.Errorf("escrow fetch block header %d: %w", toBlock, err)
	}

	if err := ei.db.SaveEscrowBatch(ctx, ei.chainID, ei.streamID, events, toBlock, header.Hash.Hex()); err != nil {
		return nil, fmt.Errorf("escrow save batch [%d, %d]: %w", fromBlock, toBlock, err)
	}

	// Redis Pub/Sub: broadcast event notification payload to wtf:chain:events and wtf:chain:settled
	if ei.redis != nil {
		for _, ev := range events {
			if ev == nil {
				continue
			}
			escrowID := ""
			if ev.EscrowID != nil {
				escrowID = *ev.EscrowID
			}
			amount := ""
			if ev.Amount != nil {
				amount = *ev.Amount
			}
			payload := map[string]any{
				"escrowId":  escrowID,
				"txHash":    ev.TxHash.Hex(),
				"eventType": ev.EventType,
				"amount":    amount,
			}
			if msgBytes, err := json.Marshal(payload); err == nil {
				if err := ei.redis.Publish(ctx, "wtf:chain:events", msgBytes).Err(); err != nil {
					slog.Warn("failed to publish escrow event to redis", "channel", "wtf:chain:events", "error", err)
				}
				if err := ei.redis.Publish(ctx, "wtf:chain:settled", msgBytes).Err(); err != nil {
					slog.Warn("failed to publish escrow event to redis", "channel", "wtf:chain:settled", "error", err)
				}
			}
		}
	}

	slog.Info("escrow range indexed",
		"stream_id", ei.streamID,
		"from", fromBlock,
		"to", toBlock,
		"events", len(events),
		"checkpoint", toBlock,
	)

	return events, nil
}

// IndexRangeWithRetry wraps IndexRange with the configured retry policy.
func (ei *EscrowIndexer) IndexRangeWithRetry(ctx context.Context, from, to uint64) ([]*models.EscrowEvent, error) {
	var events []*models.EscrowEvent
	err := ei.retryer.RetryRange(ctx, ei.streamID, from, to, func() error {
		var err error
		events, err = ei.IndexRange(ctx, from, to)
		return err
	})
	return events, err
}

// RunBackfill executes historical backfill from the last checkpoint up to targetBlock.
func (ei *EscrowIndexer) RunBackfill(ctx context.Context, targetBlock uint64) (uint64, int, error) {
	fromBlock, err := ei.GetEffectiveStartBlock(ctx)
	if err != nil {
		return 0, 0, err
	}

	if fromBlock > targetBlock {
		slog.Info("escrow stream already up to date",
			"stream_id", ei.streamID,
			"from_block", fromBlock,
			"target_block", targetBlock,
		)
		return targetBlock, 0, nil
	}

	slog.Info("escrow backfill starting",
		"stream_id", ei.streamID,
		"from_block", fromBlock,
		"target_block", targetBlock,
		"batch_size", ei.batch,
		"contract", ei.contract.Hex(),
	)

	totalEvents := 0
	lastBlock := fromBlock - 1

	for from := fromBlock; from <= targetBlock; {
		to := from + ei.batch - 1
		if to > targetBlock {
			to = targetBlock
		}

		evs, err := ei.IndexRangeWithRetry(ctx, from, to)
		if err != nil {
			return lastBlock, totalEvents, fmt.Errorf("escrow backfill [%d, %d]: %w", from, to, err)
		}

		totalEvents += len(evs)
		lastBlock = to
		from = to + 1
	}

	slog.Info("escrow backfill complete",
		"stream_id", ei.streamID,
		"last_block", lastBlock,
		"total_events", totalEvents,
	)

	return lastBlock, totalEvents, nil
}

// ProcessLog decodes a single ethereum log into an EscrowEvent.
func (ei *EscrowIndexer) ProcessLog(
	log types.Log,
	getTimestamp func(uint64) (time.Time, error),
) (*models.EscrowEvent, error) {
	return ei.decodeLog(log, getTimestamp)
}

// decodeLog decodes a single ethereum log into an EscrowEvent.
func (ei *EscrowIndexer) decodeLog(
	log types.Log,
	getTimestamp func(uint64) (time.Time, error),
) (*models.EscrowEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("log has no topics")
	}

	topic0 := log.Topics[0]
	event, err := ei.abi.EventByID(topic0)
	if err != nil {
		return nil, fmt.Errorf("unknown event topic %s: %w", topic0.Hex(), err)
	}

	ts, err := getTimestamp(log.BlockNumber)
	if err != nil {
		return nil, err
	}

	rawData := make(map[string]any)

	// Extract escrowId from Topics[1] as *big.Int and store as string
	var escrowID *string
	if len(log.Topics) > 1 {
		bi := new(big.Int).SetBytes(log.Topics[1].Bytes())
		s := bi.String()
		escrowID = &s
		rawData["escrowId"] = s
	}

	var amount *string

	switch topic0 {
	case wtfabi.TopicEscrowCreated:
		// buyer (Topics[2]), seller (Topics[3]), amount from log.Data[0:32]
		if len(log.Topics) > 2 {
			buyer := common.BytesToAddress(log.Topics[2].Bytes())
			rawData["buyer"] = buyer.Hex()
		}
		if len(log.Topics) > 3 {
			seller := common.BytesToAddress(log.Topics[3].Bytes())
			rawData["seller"] = seller.Hex()
		}
		if len(log.Data) >= 32 {
			amt := new(big.Int).SetBytes(log.Data[0:32])
			s := amt.String()
			amount = &s
			rawData["amount"] = s
		}

	case wtfabi.TopicEscrowReleased:
		// amount from log.Data[0:32]
		if len(log.Data) >= 32 {
			amt := new(big.Int).SetBytes(log.Data[0:32])
			s := amt.String()
			amount = &s
			rawData["amount"] = s
		}

	case wtfabi.TopicEscrowRefunded:
		// amount from log.Data[0:32]
		if len(log.Data) >= 32 {
			amt := new(big.Int).SetBytes(log.Data[0:32])
			s := amt.String()
			amount = &s
			rawData["amount"] = s
		}

	case wtfabi.TopicDisputeResolved:
		// winner (Topics[2]), amountReleased from log.Data[0:32]
		if len(log.Topics) > 2 {
			winner := common.BytesToAddress(log.Topics[2].Bytes())
			rawData["winner"] = winner.Hex()
		}
		if len(log.Data) >= 32 {
			amt := new(big.Int).SetBytes(log.Data[0:32])
			s := amt.String()
			amount = &s
			rawData["amountReleased"] = s
			rawData["amount"] = s
		}

	case wtfabi.TopicDeliveryAcknowledged:
		if len(log.Data) >= 32 {
			dt := new(big.Int).SetBytes(log.Data[0:32])
			s := dt.String()
			rawData["deliveryTime"] = s
		}

	case wtfabi.TopicDisputeRaised:
		if len(log.Topics) > 2 {
			raisedBy := common.BytesToAddress(log.Topics[2].Bytes())
			rawData["raisedBy"] = raisedBy.Hex()
		}
		if len(log.Data) >= 32 {
			fee := new(big.Int).SetBytes(log.Data[0:32])
			s := fee.String()
			rawData["disputeFee"] = s
		}
	}

	return persistence.EscrowEventFromLog(
		ei.chainID,
		ei.contract,
		event.Name,
		log.TxHash,
		log.BlockNumber,
		ts,
		log.Index,
		log.Removed,
		escrowID,
		amount,
		rawData,
	), nil
}
