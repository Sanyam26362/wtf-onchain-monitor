package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

// ChainEventsRepository handles persistence and queries for the chain_events table.
type ChainEventsRepository struct {
	pool *pgxpool.Pool
}

// NewChainEventsRepository creates a new ChainEventsRepository instance.
func NewChainEventsRepository(pool *pgxpool.Pool) *ChainEventsRepository {
	return &ChainEventsRepository{pool: pool}
}

// InsertChainEvent persists a chain event into indexer.chain_events.
// It is idempotent and performs an ON CONFLICT (tx_hash, log_index) DO NOTHING.
func (r *ChainEventsRepository) InsertChainEvent(ctx context.Context, event *models.ChainEvent) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	const query = `
		INSERT INTO indexer.chain_events (
			chain_id,
			contract_address,
			event_name,
			tx_hash,
			block_number,
			block_hash,
			log_index,
			tx_index,
			block_timestamp,
			escrow_id,
			buyer,
			seller,
			amount,
			alchemy_webhook_id,
			raw_payload,
			indexed_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
		)
		ON CONFLICT (tx_hash, log_index) DO NOTHING
		RETURNING event_id;
	`

	blockTime := time.Unix(event.BlockTimestamp, 0).UTC()
	if event.BlockTimestamp == 0 {
		blockTime = time.Now().UTC()
	}

	indexedAt := event.IndexedAt
	if indexedAt.IsZero() {
		indexedAt = time.Now().UTC()
	}

	var amountVal any
	if event.Amount != "" {
		amountVal = event.Amount
	}

	var escrowIDVal any
	if event.EscrowID != "" {
		escrowIDVal = event.EscrowID
	}

	var buyerVal any
	if event.Buyer != "" {
		buyerVal = event.Buyer
	}

	var sellerVal any
	if event.Seller != "" {
		sellerVal = event.Seller
	}

	var webhookIDVal any
	if event.AlchemyWebhookID != "" {
		webhookIDVal = event.AlchemyWebhookID
	}

	var payloadVal any
	if event.RawPayload != "" {
		payloadVal = event.RawPayload
	}

	var eventID int64
	err := r.pool.QueryRow(
		ctx,
		query,
		event.ChainID,
		event.ContractAddress,
		event.EventName,
		event.TxHash,
		event.BlockNumber,
		event.BlockHash,
		event.LogIndex,
		event.TxIndex,
		blockTime,
		escrowIDVal,
		buyerVal,
		sellerVal,
		amountVal,
		webhookIDVal,
		payloadVal,
		indexedAt,
	).Scan(&eventID)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Already exists due to conflict on (tx_hash, log_index)
			return nil
		}
		return fmt.Errorf("failed to insert chain event: %w", err)
	}

	event.EventID = eventID
	return nil
}

// GetEventsByEscrowID retrieves all chain events for a given escrow ID ordered by block_number ASC, log_index ASC.
func (r *ChainEventsRepository) GetEventsByEscrowID(ctx context.Context, escrowID string) ([]models.ChainEvent, error) {
	const query = `
		SELECT
			event_id,
			chain_id,
			contract_address,
			event_name,
			tx_hash,
			block_number,
			COALESCE(block_hash, ''),
			log_index,
			COALESCE(tx_index, 0),
			block_timestamp,
			COALESCE(escrow_id, ''),
			COALESCE(buyer, ''),
			COALESCE(seller, ''),
			COALESCE(amount::text, ''),
			COALESCE(alchemy_webhook_id, ''),
			COALESCE(raw_payload, ''),
			COALESCE(indexed_at, created_at)
		FROM indexer.chain_events
		WHERE escrow_id = $1
		ORDER BY block_number ASC, log_index ASC;
	`

	rows, err := r.pool.Query(ctx, query, escrowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query chain events by escrow_id: %w", err)
	}
	defer rows.Close()

	var events []models.ChainEvent
	for rows.Next() {
		var (
			e         models.ChainEvent
			blockTime time.Time
		)
		err := rows.Scan(
			&e.EventID,
			&e.ChainID,
			&e.ContractAddress,
			&e.EventName,
			&e.TxHash,
			&e.BlockNumber,
			&e.BlockHash,
			&e.LogIndex,
			&e.TxIndex,
			&blockTime,
			&e.EscrowID,
			&e.Buyer,
			&e.Seller,
			&e.Amount,
			&e.AlchemyWebhookID,
			&e.RawPayload,
			&e.IndexedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan chain event: %w", err)
		}
		e.BlockTimestamp = blockTime.Unix()
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating chain events: %w", err)
	}

	return events, nil
}

// GetEventByTxAndLogIndex retrieves a single chain event by transaction hash and log index.
func (r *ChainEventsRepository) GetEventByTxAndLogIndex(ctx context.Context, txHash string, logIndex int) (*models.ChainEvent, error) {
	const query = `
		SELECT
			event_id,
			chain_id,
			contract_address,
			event_name,
			tx_hash,
			block_number,
			COALESCE(block_hash, ''),
			log_index,
			COALESCE(tx_index, 0),
			block_timestamp,
			COALESCE(escrow_id, ''),
			COALESCE(buyer, ''),
			COALESCE(seller, ''),
			COALESCE(amount::text, ''),
			COALESCE(alchemy_webhook_id, ''),
			COALESCE(raw_payload, ''),
			COALESCE(indexed_at, created_at)
		FROM indexer.chain_events
		WHERE LOWER(tx_hash) = LOWER($1) AND log_index = $2
		LIMIT 1;
	`

	var (
		e         models.ChainEvent
		blockTime time.Time
	)

	err := r.pool.QueryRow(ctx, query, txHash, logIndex).Scan(
		&e.EventID,
		&e.ChainID,
		&e.ContractAddress,
		&e.EventName,
		&e.TxHash,
		&e.BlockNumber,
		&e.BlockHash,
		&e.LogIndex,
		&e.TxIndex,
		&blockTime,
		&e.EscrowID,
		&e.Buyer,
		&e.Seller,
		&e.Amount,
		&e.AlchemyWebhookID,
		&e.RawPayload,
		&e.IndexedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query chain event by tx and log index: %w", err)
	}

	e.BlockTimestamp = blockTime.Unix()
	return &e, nil
}
