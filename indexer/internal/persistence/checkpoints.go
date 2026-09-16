package persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// GetCheckpoint retrieves the last indexed block for a stream on a chain.
// If no checkpoint exists, it returns ok = false and block = 0.
func (p *Postgres) GetCheckpoint(
	ctx context.Context,
	chainID int64,
	streamID string,
) (uint64, bool, error) {
	const query = `
		SELECT last_indexed_block
		FROM sync_checkpoints
		WHERE chain_id = $1 AND stream_id = $2
	`

	var lastIndexedBlock int64
	err := p.pool.QueryRow(ctx, query, chainID, streamID).Scan(&lastIndexedBlock)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("failed to query checkpoint for %s (chain %d): %w", streamID, chainID, err)
	}

	return uint64(lastIndexedBlock), true, nil
}

// SaveCheckpoint updates or creates the checkpoint record for a stream.
func (p *Postgres) SaveCheckpoint(
	ctx context.Context,
	chainID int64,
	streamID string,
	blockNumber uint64,
	blockHash string,
) error {
	const query = `
		INSERT INTO sync_checkpoints (
			chain_id,
			stream_id,
			last_indexed_block,
			last_block_hash,
			updated_at
		)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (chain_id, stream_id)
		DO UPDATE SET
			last_indexed_block = EXCLUDED.last_indexed_block,
			last_block_hash = EXCLUDED.last_block_hash,
			updated_at = NOW()
	`

	_, err := p.pool.Exec(
		ctx,
		query,
		chainID,
		streamID,
		int64(blockNumber),
		blockHash,
	)
	if err != nil {
		return fmt.Errorf("failed to save checkpoint for %s at block %d: %w", streamID, blockNumber, err)
	}

	return nil
}
