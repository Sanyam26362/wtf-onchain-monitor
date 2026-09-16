package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

type SyncRepository struct {
	pool *pgxpool.Pool
}

func NewSyncRepository(pool *pgxpool.Pool) *SyncRepository {
	return &SyncRepository{pool: pool}
}

// CheckpointRecord holds sync checkpoint data from PostgreSQL.
type CheckpointRecord struct {
	ChainID          int64
	StreamID         string
	LastIndexedBlock uint64
	LastBlockHash    string
	UpdatedAt        time.Time
}

// GetAllCheckpoints retrieves all stream checkpoints for a given chain ID.
func (r *SyncRepository) GetAllCheckpoints(ctx context.Context, chainID int64) ([]*CheckpointRecord, error) {
	const query = `
		SELECT
			chain_id,
			stream_id,
			last_indexed_block,
			COALESCE(last_block_hash, ''),
			updated_at
		FROM sync_checkpoints
		WHERE chain_id = $1
		ORDER BY stream_id ASC
	`

	rows, err := r.pool.Query(ctx, query, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to query sync checkpoints: %w", err)
	}
	defer rows.Close()

	var records []*CheckpointRecord
	for rows.Next() {
		var (
			cid         int64
			streamID    string
			blockNumber int64
			blockHash   string
			updatedAt   time.Time
		)

		if err := rows.Scan(&cid, &streamID, &blockNumber, &blockHash, &updatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint row: %w", err)
		}

		records = append(records, &CheckpointRecord{
			ChainID:          cid,
			StreamID:         streamID,
			LastIndexedBlock: uint64(blockNumber),
			LastBlockHash:    blockHash,
			UpdatedAt:        updatedAt.UTC(),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading checkpoint rows: %w", err)
	}

	return records, nil
}

// GetLatestChainBlock retrieves the highest indexed block or safe block recorded across streams.
func (r *SyncRepository) GetLatestChainBlock(ctx context.Context, chainID int64) (uint64, error) {
	const query = `
		SELECT COALESCE(MAX(last_indexed_block), 0)
		FROM sync_checkpoints
		WHERE chain_id = $1
	`
	var maxBlock sql.NullInt64
	if err := r.pool.QueryRow(ctx, query, chainID).Scan(&maxBlock); err != nil {
		return 0, fmt.Errorf("failed to query max indexed block: %w", err)
	}
	if maxBlock.Valid && maxBlock.Int64 > 0 {
		return uint64(maxBlock.Int64), nil
	}
	return 0, nil
}

// ExtractTokenFromStreamID extracts the token address if embedded in the stream ID (e.g. erc20_transfers_0x...).
func ExtractTokenFromStreamID(streamID string) *string {
	if strings.HasPrefix(streamID, "erc20_transfers_") {
		addr := strings.TrimPrefix(streamID, "erc20_transfers_")
		if strings.HasPrefix(addr, "0x") || strings.HasPrefix(addr, "0X") {
			return &addr
		}
	}
	return nil
}

// BuildStreamStatus builds status representation for a stream.
func BuildStreamStatus(rec *CheckpointRecord, latestBlock uint64, tokenAddress string) *models.StreamStatus {
	var lag int64
	if latestBlock >= rec.LastIndexedBlock {
		lag = int64(latestBlock - rec.LastIndexedBlock)
	}

	status := "synced"
	if lag > 10 {
		status = "catching_up"
	} else if lag > 0 {
		status = "syncing"
	}

	var token *string
	if rec.StreamID == "erc20_transfers" && tokenAddress != "" {
		token = &tokenAddress
	} else {
		token = ExtractTokenFromStreamID(rec.StreamID)
		if token == nil && tokenAddress != "" {
			token = &tokenAddress
		}
	}

	return &models.StreamStatus{
		StreamID:         rec.StreamID,
		Token:            token,
		LastIndexedBlock: rec.LastIndexedBlock,
		Lag:              lag,
		Status:           status,
	}
}
