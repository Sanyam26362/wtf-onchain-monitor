package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"worldtradefuture/indexer/internal/models"
)

// SaveEscrowEvent persists a single escrow event idempotently.
// The unique constraint is (chain_id, contract_address, tx_hash, log_index) — the blockchain log identity.
// On conflict the removed flag, block_timestamp, amount, and raw_data are updated (reorg handling).
func (p *Postgres) SaveEscrowEvent(ctx context.Context, ev *models.EscrowEvent) error {
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
	`

	_, err = p.pool.Exec(
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
	)
	if err != nil {
		return fmt.Errorf("failed to save escrow event %s tx %s log %d: %w",
			ev.EventType, ev.TxHash.Hex(), ev.LogIndex, err)
	}

	return nil
}

// SaveEscrowBatch persists escrow events and the checkpoint atomically.
// This is the primary write path for EscrowIndexer.
func (p *Postgres) SaveEscrowBatch(
	ctx context.Context,
	chainID int64,
	streamID string,
	events []*models.EscrowEvent,
	checkpointBlock uint64,
	checkpointHash string,
) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("escrow batch: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const evQuery = `
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
			event_type      = EXCLUDED.event_type,
			removed         = EXCLUDED.removed,
			block_timestamp = EXCLUDED.block_timestamp,
			escrow_id       = EXCLUDED.escrow_id,
			amount          = EXCLUDED.amount,
			raw_data        = EXCLUDED.raw_data
	`

	for _, ev := range events {
		if ev == nil {
			continue
		}
		rawBytes, mErr := json.Marshal(ev.RawData)
		if mErr != nil {
			return fmt.Errorf("escrow batch: marshal raw_data for tx %s: %w", ev.TxHash.Hex(), mErr)
		}
		if _, err := tx.Exec(
			ctx, evQuery,
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
		); err != nil {
			return fmt.Errorf("escrow batch: insert event %s tx %s log %d: %w",
				ev.EventType, ev.TxHash.Hex(), ev.LogIndex, err)
		}
	}

	if streamID != "" && checkpointBlock > 0 {
		const cpQuery = `
			INSERT INTO sync_checkpoints (
				chain_id, stream_id, last_indexed_block, last_block_hash, updated_at
			)
			VALUES ($1, $2, $3, $4, NOW())
			ON CONFLICT (chain_id, stream_id)
			DO UPDATE SET
				last_indexed_block = EXCLUDED.last_indexed_block,
				last_block_hash    = EXCLUDED.last_block_hash,
				updated_at         = NOW()
		`
		if _, err := tx.Exec(ctx, cpQuery, chainID, streamID, int64(checkpointBlock), checkpointHash); err != nil {
			return fmt.Errorf("escrow batch: checkpoint update to block %d: %w", checkpointBlock, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("escrow batch: commit: %w", err)
	}

	return nil
}

// GetEscrowEventCount returns the total number of escrow events for a chain.
func (p *Postgres) GetEscrowEventCount(ctx context.Context, chainID int64) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM escrow_events WHERE chain_id = $1`, chainID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count escrow events: %w", err)
	}
	return count, nil
}

// EscrowEventFromLog is a helper to build an EscrowEvent from a decoded log.
// escrowID and amount are nullable: pass nil when the event type does not provide them.
func EscrowEventFromLog(
	chainID int64,
	contractAddress common.Address,
	eventType string,
	txHash common.Hash,
	blockNumber uint64,
	blockTimestamp time.Time,
	logIndex uint,
	removed bool,
	escrowID *string,
	amount *string,
	rawData map[string]any,
) *models.EscrowEvent {
	return &models.EscrowEvent{
		ChainID:         chainID,
		ContractAddress: contractAddress,
		EventType:       eventType,
		TxHash:          txHash,
		BlockNumber:     blockNumber,
		BlockTimestamp:  blockTimestamp,
		LogIndex:        logIndex,
		Removed:         removed,
		EscrowID:        escrowID,
		Amount:          amount,
		RawData:         rawData,
	}
}

// GetEscrowEventsByEscrowID retrieves all escrow events for a given escrow ID ordered by block_number ASC, log_index ASC.
func (p *Postgres) GetEscrowEventsByEscrowID(ctx context.Context, escrowID string) ([]*models.EscrowEvent, error) {
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
		WHERE escrow_id = CAST($1 AS NUMERIC)
		ORDER BY block_number ASC, log_index ASC
	`
	rows, err := p.pool.Query(ctx, query, escrowID)
	if err != nil {
		return nil, fmt.Errorf("query escrow events by escrow_id: %w", err)
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
			return nil, fmt.Errorf("scan escrow event: %w", err)
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
func (p *Postgres) GetEscrowEventByTxAndLogIndex(ctx context.Context, txHash string, logIndex int) (*models.EscrowEvent, error) {
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
	err := p.pool.QueryRow(ctx, query, txHash, logIndex).Scan(
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
		return nil, fmt.Errorf("query escrow event by tx and log index: %w", err)
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
