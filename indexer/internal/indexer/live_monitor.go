package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/blockchain"
)

// CalculateSafeTarget computes the safe head block taking into account confirmation depth.
// Only blocks up to this target are indexed to protect against blockchain reorgs.
func CalculateSafeTarget(latestBlock uint64, confirmationDepth uint64) uint64 {
	if confirmationDepth == 0 {
		return latestBlock
	}
	if latestBlock < confirmationDepth {
		return 0
	}
	return latestBlock - confirmationDepth
}

// CalculateNextBlock determines the start block for indexing based on whether a durable
// checkpoint already exists for the stream.
func CalculateNextBlock(lastIndexedBlock uint64, hasCheckpoint bool, defaultStartBlock uint64) uint64 {
	if hasCheckpoint {
		return lastIndexedBlock + 1
	}
	return defaultStartBlock
}

// LiveMonitorConfig holds parameters for running the continuous live monitoring loop.
type LiveMonitorConfig struct {
	ChainID           int64
	ConfirmationDepth uint64
	BatchSize         uint64
	PollInterval      time.Duration

	// Stream 1: MonthlyPayroll
	PayrollContractAddress common.Address
	PayrollStreamID        string
	PayrollStartBlock      uint64

	// Stream 2: Generic ERC-20 Token (WTF Token)
	TokenAddress      common.Address
	TokenStreamID     string
	TokenStartBlock   uint64
	MaxBatchesPerPoll uint64
}

// LiveMonitor continuously monitors new safe blocks and indexes events into PostgreSQL.
type LiveMonitor struct {
	cfg            LiveMonitorConfig
	client         blockchain.BlockchainClient
	payrollService *Service
	tokenIndexer   *TokenIndexer
	persistence    PayrollPersistence
}

// NewLiveMonitor creates a new LiveMonitor instance.
func NewLiveMonitor(
	cfg LiveMonitorConfig,
	client blockchain.BlockchainClient,
	payrollService *Service,
	tokenIndexer *TokenIndexer,
	db PayrollPersistence,
) (*LiveMonitor, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client is nil")
	}
	if db == nil {
		return nil, fmt.Errorf("persistence layer is nil")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 50
	}
	if cfg.MaxBatchesPerPoll == 0 {
		cfg.MaxBatchesPerPoll = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.PayrollStreamID == "" {
		cfg.PayrollStreamID = "monthly_payroll"
	}

	return &LiveMonitor{
		cfg:            cfg,
		client:         client,
		payrollService: payrollService,
		tokenIndexer:   tokenIndexer,
		persistence:    db,
	}, nil
}

// Start launches the continuous live monitoring loop and runs until ctx is cancelled.
func (m *LiveMonitor) Start(ctx context.Context) error {
	slog.Info("LIVE MONITOR STARTED",
		"chain_id", m.cfg.ChainID,
		"poll_interval", m.cfg.PollInterval.String(),
		"confirmation_depth", m.cfg.ConfirmationDepth,
		"batch_size", m.cfg.BatchSize,
	)

	// Log initial checkpoints and safe targets
	m.logInitialStatus(ctx)

	// Run first poll cycle immediately
	if err := m.PollOnce(ctx); err != nil {
		slog.Error("error during initial live poll cycle", "error", err)
	}

	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown signal received, stopping live monitor")
			return nil
		case <-ticker.C:
			if err := m.PollOnce(ctx); err != nil {
				slog.Error("error during live poll cycle", "error", err)
			}
		}
	}
}

// PollOnce executes a single polling iteration across all configured streams.
func (m *LiveMonitor) PollOnce(ctx context.Context) error {
	latestBlock, err := m.client.LatestBlock(ctx)
	if err != nil {
		slog.Error("failed to fetch latest block from RPC", "error", err)
		return fmt.Errorf("fetch latest block: %w", err)
	}

	safeTarget := CalculateSafeTarget(latestBlock, m.cfg.ConfirmationDepth)

	// 1. Poll MonthlyPayroll stream
	if m.payrollService != nil && (m.cfg.PayrollContractAddress != common.Address{}) {
		if _, err := m.PollPayroll(ctx, safeTarget); err != nil {
			slog.Error("payroll live stream indexing error",
				"stream", m.cfg.PayrollStreamID,
				"error", err,
			)
			// Do not crash the entire monitor on single-stream RPC failure
		}
	}

	// 2. Poll Generic ERC-20 Token stream (WTF Token)
	if m.tokenIndexer != nil && (m.cfg.TokenAddress != common.Address{}) {
		if _, err := m.PollToken(ctx, safeTarget); err != nil {
			slog.Error("token live stream indexing error",
				"stream", m.cfg.TokenStreamID,
				"error", err,
			)
		}
	}

	return nil
}

// PollPayroll checks and indexes new safe blocks for the MonthlyPayroll stream.
func (m *LiveMonitor) PollPayroll(ctx context.Context, safeTarget uint64) (int, error) {
	fromBlock, err := m.payrollService.GetEffectiveStartBlock(
		ctx,
		m.cfg.ChainID,
		m.cfg.PayrollStreamID,
		m.cfg.PayrollStartBlock,
	)
	if err != nil {
		return 0, fmt.Errorf("resolve payroll start block: %w", err)
	}

	if fromBlock > safeTarget {
		slog.Info("payroll stream up to date",
			"stream", m.cfg.PayrollStreamID,
			"checkpoint", fromBlock-1,
			"safe_target", safeTarget,
		)
		return 0, nil
	}

	totalEvents := 0
	batchesProcessed := uint64(0)
	for from := fromBlock; from <= safeTarget && batchesProcessed < m.cfg.MaxBatchesPerPoll; {
		if ctx.Err() != nil {
			return totalEvents, ctx.Err()
		}

		to := from + m.cfg.BatchSize - 1
		if to > safeTarget {
			to = safeTarget
		}

		slog.Info("processing payroll block range",
			"stream", m.cfg.PayrollStreamID,
			"from_block", from,
			"to_block", to,
		)

		events, err := m.payrollService.IndexRange(ctx, m.cfg.ChainID, m.cfg.PayrollContractAddress, from, to)
		if err != nil {
			slog.Error("failed to process payroll block range",
				"stream", m.cfg.PayrollStreamID,
				"from_block", from,
				"to_block", to,
				"error", err,
			)
			return totalEvents, fmt.Errorf("index payroll range [%d, %d]: %w", from, to, err)
		}

		// Save checkpoint only after successful persistence of events and projections
		if err := m.persistence.SaveCheckpoint(ctx, m.cfg.ChainID, m.cfg.PayrollStreamID, to, ""); err != nil {
			slog.Error("failed to save payroll checkpoint",
				"stream", m.cfg.PayrollStreamID,
				"checkpoint", to,
				"error", err,
			)
			return totalEvents, fmt.Errorf("save payroll checkpoint for block %d: %w", to, err)
		}

		totalEvents += len(events)
		slog.Info("processed payroll block range successfully",
			"stream", m.cfg.PayrollStreamID,
			"events_found", len(events),
			"events_persisted", len(events),
			"checkpoint", to,
		)

		batchesProcessed++
		from = to + 1
	}

	return totalEvents, nil
}

// PollToken checks and indexes new safe blocks for the ERC-20 Token stream (WTF Token).
func (m *LiveMonitor) PollToken(ctx context.Context, safeTarget uint64) (int, error) {
	fromBlock, err := m.tokenIndexer.GetEffectiveStartBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve token start block: %w", err)
	}

	if fromBlock > safeTarget {
		slog.Info("token stream up to date",
			"stream", m.cfg.TokenStreamID,
			"checkpoint", fromBlock-1,
			"safe_target", safeTarget,
		)
		return 0, nil
	}

	totalTransfers := 0
	batchesProcessed := uint64(0)
	for from := fromBlock; from <= safeTarget && batchesProcessed < m.cfg.MaxBatchesPerPoll; {
		if ctx.Err() != nil {
			return totalTransfers, ctx.Err()
		}

		to := from + m.cfg.BatchSize - 1
		if to > safeTarget {
			to = safeTarget
		}

		slog.Info("processing token block range",
			"stream", m.cfg.TokenStreamID,
			"from_block", from,
			"to_block", to,
		)

		// IndexRange executes batch persistence and checkpoint update atomically
		transfers, err := m.tokenIndexer.IndexRange(ctx, from, to)
		if err != nil {
			slog.Error("failed to process token block range",
				"stream", m.cfg.TokenStreamID,
				"from_block", from,
				"to_block", to,
				"error", err,
			)
			return totalTransfers, fmt.Errorf("index token range [%d, %d]: %w", from, to, err)
		}

		totalTransfers += len(transfers)
		slog.Info("processed token block range successfully",
			"stream", m.cfg.TokenStreamID,
			"events_found", len(transfers),
			"checkpoint", to,
		)

		batchesProcessed++
		from = to + 1
	}

	return totalTransfers, nil
}

// logInitialStatus logs the current persisted checkpoints and targets at monitor startup.
func (m *LiveMonitor) logInitialStatus(ctx context.Context) {
	latestBlock, err := m.client.LatestBlock(ctx)
	if err != nil {
		slog.Warn("could not fetch latest block for initial status", "error", err)
		return
	}

	safeTarget := CalculateSafeTarget(latestBlock, m.cfg.ConfirmationDepth)

	if m.payrollService != nil && (m.cfg.PayrollContractAddress != common.Address{}) {
		cp, found, _ := m.persistence.GetCheckpoint(ctx, m.cfg.ChainID, m.cfg.PayrollStreamID)
		cpStr := "none"
		if found {
			cpStr = fmt.Sprintf("%d", cp)
		}
		slog.Info("PAYROLL stream status",
			"stream", m.cfg.PayrollStreamID,
			"checkpoint", cpStr,
			"safe_target", safeTarget,
		)
	}

	if m.tokenIndexer != nil && (m.cfg.TokenAddress != common.Address{}) {
		cp, found, _ := m.persistence.GetCheckpoint(ctx, m.cfg.ChainID, m.cfg.TokenStreamID)
		cpStr := "none"
		if found {
			cpStr = fmt.Sprintf("%d", cp)
		}
		slog.Info("TOKEN stream status",
			"stream", m.cfg.TokenStreamID,
			"checkpoint", cpStr,
			"safe_target", safeTarget,
		)
	}
}
