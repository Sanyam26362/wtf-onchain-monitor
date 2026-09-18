package indexer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
)

// PayrollPersistence defines the storage operations required by the MonthlyPayroll indexer service.
type PayrollPersistence interface {
	GetCheckpoint(ctx context.Context, chainID int64, streamID string) (uint64, bool, error)
	SaveCheckpoint(ctx context.Context, chainID int64, streamID string, blockNumber uint64, blockHash string) error
	SaveTransaction(ctx context.Context, chainID int64, metadata *blockchain.TransactionMetadata) error
	SaveChainEvent(ctx context.Context, chainID int64, contractAddress common.Address, blockTimestamp uint64, event *decoder.DecodedEvent) error
	ProjectPayrollEvent(ctx context.Context, chainID int64, blockTimestamp uint64, event *decoder.DecodedEvent) error
}

// Service indexes events emitted by the MonthlyPayroll smart contract.
type Service struct {
	client      blockchain.BlockchainClient
	decoder     *decoder.Decoder
	persistence PayrollPersistence
	retryer     *Retryer
}

// BackfillOptions specifies parameters for running a historical backfill.
type BackfillOptions struct {
	ChainID         int64
	ContractAddress common.Address
	StartBlock      uint64
	TargetBlock     uint64
	BatchSize       uint64
	StreamID        string
}

// New creates a new MonthlyPayroll indexing service.
func New(
	client blockchain.BlockchainClient,
	contractAddress common.Address,
	db PayrollPersistence,
) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client is nil")
	}

	if db == nil {
		return nil, fmt.Errorf("persistence layer is nil")
	}

	eventDecoder, err := decoder.New(contractAddress)
	if err != nil {
		return nil, fmt.Errorf("create event decoder: %w", err)
	}

	return &Service{
		client:      client,
		decoder:     eventDecoder,
		persistence: db,
		retryer:     NewRetryer(DefaultRetryPolicy()),
	}, nil
}

// NewWithDecoder creates a service with a custom decoder (useful for testing).
func NewWithDecoder(
	client blockchain.BlockchainClient,
	eventDecoder *decoder.Decoder,
	db PayrollPersistence,
) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client is nil")
	}
	if eventDecoder == nil {
		return nil, fmt.Errorf("event decoder is nil")
	}
	if db == nil {
		return nil, fmt.Errorf("persistence layer is nil")
	}

	return &Service{
		client:      client,
		decoder:     eventDecoder,
		persistence: db,
		retryer:     NewRetryer(DefaultRetryPolicy()),
	}, nil
}

// SetRetryPolicy updates the service's RPC retry policy.
func (s *Service) SetRetryPolicy(policy RetryPolicy) {
	s.retryer = NewRetryer(policy)
}

// Retryer returns the configured retryer.
func (s *Service) Retryer() *Retryer {
	return s.retryer
}

// GetEffectiveStartBlock resolves the block number to start or resume from.
// If an existing checkpoint is found, it returns lastIndexedBlock + 1.
// Otherwise, it returns startBlock.
func (s *Service) GetEffectiveStartBlock(
	ctx context.Context,
	chainID int64,
	streamID string,
	startBlock uint64,
) (uint64, error) {
	lastBlock, found, err := s.persistence.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		return 0, fmt.Errorf("failed to query checkpoint for %s (chain %d): %w", streamID, chainID, err)
	}

	if found {
		resumeBlock := lastBlock + 1
		slog.Info("resuming MonthlyPayroll indexer from durable checkpoint",
			"stream_id", streamID,
			"checkpoint_block", lastBlock,
			"resume_block", resumeBlock,
		)
		return resumeBlock, nil
	}

	slog.Info("no existing checkpoint found, starting from configured block",
		"stream_id", streamID,
		"start_block", startBlock,
	)
	return startBlock, nil
}

// RunBackfill executes a bounded chunked historical backfill from the effective
// start block up to targetBlock. Each batch advances the checkpoint upon success.
func (s *Service) RunBackfill(ctx context.Context, opts BackfillOptions) (uint64, error) {
	if opts.BatchSize == 0 {
		opts.BatchSize = 50
	}
	if opts.StreamID == "" {
		opts.StreamID = "monthly_payroll"
	}

	fromBlock, err := s.GetEffectiveStartBlock(ctx, opts.ChainID, opts.StreamID, opts.StartBlock)
	if err != nil {
		return 0, err
	}

	if fromBlock > opts.TargetBlock {
		slog.Info("MonthlyPayroll backfill up to date, nothing to index",
			"stream_id", opts.StreamID,
			"from_block", fromBlock,
			"target_block", opts.TargetBlock,
		)
		return opts.TargetBlock, nil
	}

	slog.Info("Historical backfill starting",
		"start_block", opts.StartBlock,
		"from_block", fromBlock,
		"target_block", opts.TargetBlock,
		"batch_size", opts.BatchSize,
		"stream_id", opts.StreamID,
	)

	lastIndexedBlock := fromBlock - 1
	for from := fromBlock; from <= opts.TargetBlock; {
		to := from + opts.BatchSize - 1
		if to > opts.TargetBlock {
			to = opts.TargetBlock
		}

		slog.Info("Processing block range",
			"from", from,
			"to", to,
		)

		events, err := s.IndexRangeWithRetry(ctx, opts.ChainID, opts.ContractAddress, from, to, opts.StreamID)
		if err != nil {
			slog.Error("Failed to process block range",
				"from", from,
				"to", to,
				"error", err,
			)
			return lastIndexedBlock, fmt.Errorf("failed indexing block range [%d, %d]: %w", from, to, err)
		}

		slog.Info("Found events",
			"count", len(events),
		)

		if err := s.persistence.SaveCheckpoint(ctx, opts.ChainID, opts.StreamID, to, ""); err != nil {
			slog.Error("Failed to update checkpoint",
				"block", to,
				"error", err,
			)
			return lastIndexedBlock, fmt.Errorf("failed to save checkpoint for block %d: %w", to, err)
		}

		slog.Info("Checkpoint updated",
			"block", to,
		)

		lastIndexedBlock = to
		from = to + 1
	}

	slog.Info("Historical backfill completed",
		"last_indexed_block", lastIndexedBlock,
	)

	return lastIndexedBlock, nil
}

// IndexRangeWithRetry wraps IndexRange with retry logic for transient RPC and rate-limit errors.
func (s *Service) IndexRangeWithRetry(
	ctx context.Context,
	chainID int64,
	contractAddress common.Address,
	fromBlock uint64,
	toBlock uint64,
	streamID string,
) ([]*decoder.DecodedEvent, error) {
	var events []*decoder.DecodedEvent
	err := s.retryer.RetryRange(ctx, streamID, fromBlock, toBlock, func() error {
		var indexErr error
		events, indexErr = s.IndexRange(ctx, chainID, contractAddress, fromBlock, toBlock)
		return indexErr
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// IndexRange fetches, decodes, persists, and projects events within [fromBlock, toBlock].
func (s *Service) IndexRange(
	ctx context.Context,
	chainID int64,
	contractAddress common.Address,
	fromBlock uint64,
	toBlock uint64,
) ([]*decoder.DecodedEvent, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range: from %d is greater than %d", fromBlock, toBlock)
	}

	logs, err := s.client.GetLogs(ctx, contractAddress, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	events := make([]*decoder.DecodedEvent, 0, len(logs))
	timestampCache := make(map[uint64]uint64)

	for _, log := range logs {
		decoded, err := s.decoder.Decode(log)
		if err != nil {
			return nil, fmt.Errorf("decode log at block %d, tx %s, log index %d: %w",
				log.BlockNumber, log.TxHash.Hex(), log.Index, err)
		}

		events = append(events, decoded)

		metadata, err := s.client.TransactionMetadata(ctx, log.TxHash)
		if err != nil {
			return nil, err
		}

		blockTimestamp, ok := timestampCache[log.BlockNumber]
		if !ok {
			ts, err := s.client.BlockTimestamp(ctx, log.BlockNumber)
			if err != nil {
				return nil, err
			}
			blockTimestamp = ts
			timestampCache[log.BlockNumber] = ts
		}

		// 1. Persist transaction metadata
		if err := s.persistence.SaveTransaction(ctx, chainID, metadata); err != nil {
			return nil, err
		}

		// 2. Persist raw chain event
		if err := s.persistence.SaveChainEvent(ctx, chainID, contractAddress, blockTimestamp, decoded); err != nil {
			return nil, err
		}

		// 3. Project event into domain tables (employees, employers, payroll_fundings, salary_claims)
		if !log.Removed {
			if err := s.persistence.ProjectPayrollEvent(ctx, chainID, blockTimestamp, decoded); err != nil {
				return nil, fmt.Errorf("failed to project payroll event %s at block %d: %w", decoded.Type, log.BlockNumber, err)
			}
		} else {
			slog.Warn("skipping projection for removed log (reorg)",
				"event_type", decoded.Type,
				"tx_hash", log.TxHash.Hex(),
				"block_number", log.BlockNumber,
				"log_index", log.Index,
			)
		}

		// 4. Structured log for EmployeeAdded events
		if decoded.Type == decoder.EventEmployeeAdded {
			if eeAdded, ok := decoded.Data.(*abi.ABIEmployeeAddedEvent); ok {
				salaryStr := "0"
				if eeAdded.Salary != nil {
					salaryStr = eeAdded.Salary.String()
				}
				allocationStr := "0"
				if eeAdded.Allocation != nil {
					allocationStr = eeAdded.Allocation.String()
				}
				slog.Info("EmployeeAdded event indexed",
					"employee", eeAdded.Employee.Hex(),
					"employer", eeAdded.Employer.Hex(),
					"block_number", log.BlockNumber,
					"tx_hash", log.TxHash.Hex(),
					"log_index", log.Index,
					"salary", salaryStr,
					"allocation", allocationStr,
				)
			}
		}
	}

	return events, nil
}

// NextRange computes the next bounded block range [fromBlock, toBlock].
func NextRange(
	fromBlock uint64,
	latestBlock uint64,
	stepSize uint64,
) (uint64, uint64, bool) {
	if fromBlock > latestBlock || stepSize == 0 {
		return 0, 0, false
	}

	toBlock := fromBlock + stepSize - 1
	if toBlock < fromBlock || toBlock > latestBlock {
		toBlock = latestBlock
	}

	return fromBlock, toBlock, true
}
