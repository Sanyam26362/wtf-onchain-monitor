package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

// EscrowEventsRepository handles database persistence and queries for the escrow_events table.
type EscrowEventsRepository struct {
	pool *pgxpool.Pool
}

// NewEscrowEventsRepository creates a new EscrowEventsRepository instance.
func NewEscrowEventsRepository(pool *pgxpool.Pool) *EscrowEventsRepository {
	return &EscrowEventsRepository{pool: pool}
}

// SaveEscrowEvent persists a single escrow event idempotently with reorg handling.
func (r *EscrowEventsRepository) SaveEscrowEvent(ctx context.Context, ev *models.EscrowEvent) error {
	if ev == nil {
		return fmt.Errorf("escrow event is nil")
	}

	rawBytes, err := json.Marshal(ev.RawData)
	if err != nil {
		return fmt.Errorf("failed to marshal escrow event raw_data: %w", err)
	}

	const query = `
		INSERT INTO escrow_events (
			chain_id,
			contract_address,
			event_type,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed,
			escrow_id,
			amount,
			raw_data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (chain_id, contract_address, tx_hash, log_index)
		DO UPDATE SET
			removed         = EXCLUDED.removed,
			block_timestamp = EXCLUDED.block_timestamp,
			amount          = EXCLUDED.amount,
			raw_data        = EXCLUDED.raw_data
		RETURNING id;
	`

	var id int64
	err = r.pool.QueryRow(
		ctx,
		query,
		ev.ChainID,
		ev.ContractAddress.Hex(),
		ev.EventType,
		ev.TxHash.Hex(),
		int64(ev.BlockNumber),
		ev.BlockTimestamp.UTC(),
		int(ev.LogIndex),
		ev.Removed,
		ev.EscrowID,
		ev.Amount,
		rawBytes,
	).Scan(&id)

	if err != nil {
		return fmt.Errorf("failed to save escrow event: %w", err)
	}

	ev.ID = id
	return nil
}

// GetEscrowEventsByEscrowID retrieves all escrow events for a given escrow ID ordered by block_number ASC, log_index ASC.
func (r *EscrowEventsRepository) GetEscrowEventsByEscrowID(ctx context.Context, escrowID string) ([]*models.EscrowEvent, error) {
	const query = `
		SELECT
			id,
			chain_id,
			contract_address,
			event_type,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed,
			escrow_id,
			amount,
			raw_data,
			created_at
		FROM escrow_events
		WHERE escrow_id = $1
		ORDER BY block_number ASC, log_index ASC
	`
	rows, err := r.pool.Query(ctx, query, escrowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query escrow events by escrow_id: %w", err)
	}
	defer rows.Close()

	var events []*models.EscrowEvent
	for rows.Next() {
		var (
			e        models.EscrowEvent
			contract string
			txHash   string
			rawBytes []byte
			blockNum int64
			logIdx   int
		)
		if err := rows.Scan(
			&e.ID,
			&e.ChainID,
			&contract,
			&e.EventType,
			&txHash,
			&blockNum,
			&e.BlockTimestamp,
			&logIdx,
			&e.Removed,
			&e.EscrowID,
			&e.Amount,
			&rawBytes,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan escrow event: %w", err)
		}
		e.ContractAddress = common.HexToAddress(contract)
		e.TxHash = common.HexToHash(txHash)
		e.BlockNumber = uint64(blockNum)
		e.LogIndex = uint(logIdx)
		if len(rawBytes) > 0 {
			_ = json.Unmarshal(rawBytes, &e.RawData)
		}
		events = append(events, &e)
	}
	return events, rows.Err()
}

// GetEscrowEventByTxAndLogIndex retrieves a single escrow event by transaction hash and log index.
func (r *EscrowEventsRepository) GetEscrowEventByTxAndLogIndex(ctx context.Context, txHash string, logIndex int) (*models.EscrowEvent, error) {
	const query = `
		SELECT
			id,
			chain_id,
			contract_address,
			event_type,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed,
			escrow_id,
			amount,
			raw_data,
			created_at
		FROM escrow_events
		WHERE LOWER(tx_hash) = LOWER($1) AND log_index = $2
		LIMIT 1
	`
	var (
		e        models.EscrowEvent
		contract string
		txH      string
		rawBytes []byte
		blockNum int64
		logIdx   int
	)
	err := r.pool.QueryRow(ctx, query, txHash, logIndex).Scan(
		&e.ID,
		&e.ChainID,
		&contract,
		&e.EventType,
		&txH,
		&blockNum,
		&e.BlockTimestamp,
		&logIdx,
		&e.Removed,
		&e.EscrowID,
		&e.Amount,
		&rawBytes,
		&e.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query escrow event by tx and log index: %w", err)
	}
	e.ContractAddress = common.HexToAddress(contract)
	e.TxHash = common.HexToHash(txH)
	e.BlockNumber = uint64(blockNum)
	e.LogIndex = uint(logIdx)
	if len(rawBytes) > 0 {
		_ = json.Unmarshal(rawBytes, &e.RawData)
	}
	return &e, nil
}

// GetEscrowEventCount returns the total number of escrow events for a chain.
func (r *EscrowEventsRepository) GetEscrowEventCount(ctx context.Context, chainID int64) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM escrow_events WHERE chain_id = $1`, chainID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count escrow events: %w", err)
	}
	return count, nil
}
