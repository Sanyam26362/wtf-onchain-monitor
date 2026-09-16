package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

type TransactionsRepository struct {
	pool *pgxpool.Pool
}

func NewTransactionsRepository(pool *pgxpool.Pool) *TransactionsRepository {
	return &TransactionsRepository{pool: pool}
}

// GetByHash finds a transaction by chain ID and transaction hash, along with its decoded events.
func (r *TransactionsRepository) GetByHash(ctx context.Context, chainID int64, txHash common.Hash) (*models.Transaction, []*models.TransactionEvent, error) {
	const txQuery = `
		SELECT
			tx_hash,
			chain_id,
			block_number,
			sender,
			recipient,
			gas_used,
			status,
			confirmations,
			error_data,
			first_seen_at,
			finalized_at
		FROM transactions
		WHERE chain_id = $1 AND LOWER(tx_hash) = LOWER($2)
	`

	var (
		hashStr       string
		cid           int64
		blockNumber   sql.NullInt64
		senderStr     string
		recipientStr  sql.NullString
		gasUsed       sql.NullInt64
		status        string
		confirmations int64
		errorData     []byte
		firstSeenAt   sql.NullTime
		finalizedAt   sql.NullTime
	)

	err := r.pool.QueryRow(ctx, txQuery, chainID, txHash.Hex()).Scan(
		&hashStr,
		&cid,
		&blockNumber,
		&senderStr,
		&recipientStr,
		&gasUsed,
		&status,
		&confirmations,
		&errorData,
		&firstSeenAt,
		&finalizedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to query transaction: %w", err)
	}

	tx := &models.Transaction{
		TxHash:        common.HexToHash(hashStr),
		ChainID:       cid,
		Sender:        common.HexToAddress(senderStr),
		Status:        status,
		Confirmations: uint64(confirmations),
	}

	if blockNumber.Valid {
		bn := uint64(blockNumber.Int64)
		tx.BlockNumber = &bn
	}
	if recipientStr.Valid && recipientStr.String != "" {
		rcp := common.HexToAddress(recipientStr.String)
		tx.Recipient = &rcp
	}
	if gasUsed.Valid {
		gu := uint64(gasUsed.Int64)
		tx.GasUsed = &gu
	}
	if len(errorData) > 0 {
		tx.ErrorData = json.RawMessage(errorData)
	}
	if firstSeenAt.Valid {
		tx.FirstSeenAt = firstSeenAt.Time
	}
	if finalizedAt.Valid {
		tx.FinalizedAt = &finalizedAt.Time
	}

	// Fetch decoded events associated with this transaction
	const eventsQuery = `
		SELECT
			event_name,
			contract_address,
			log_index,
			raw_data
		FROM chain_events
		WHERE chain_id = $1 AND LOWER(tx_hash) = LOWER($2)
		ORDER BY log_index ASC
	`

	rows, err := r.pool.Query(ctx, eventsQuery, chainID, txHash.Hex())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query events for tx %s: %w", txHash.Hex(), err)
	}
	defer rows.Close()

	var events []*models.TransactionEvent
	for rows.Next() {
		var (
			eventName       string
			contractAddrStr string
			logIndex        int
			rawData         []byte
		)

		if err := rows.Scan(&eventName, &contractAddrStr, &logIndex, &rawData); err != nil {
			return nil, nil, fmt.Errorf("failed to scan chain event row: %w", err)
		}

		event := &models.TransactionEvent{
			EventName:       eventName,
			ContractAddress: common.HexToAddress(contractAddrStr),
			LogIndex:        uint(logIndex),
		}
		if len(rawData) > 0 {
			event.Data = json.RawMessage(rawData)
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error reading chain events: %w", err)
	}

	return tx, events, nil
}

// GetLatestBlockNumber retrieves the highest block number observed in transactions or checkpoints.
func (r *TransactionsRepository) GetLatestBlockNumber(ctx context.Context, chainID int64) (uint64, error) {
	const query = `
		SELECT COALESCE(MAX(b), 0) FROM (
			SELECT MAX(block_number) AS b FROM transactions WHERE chain_id = $1
			UNION ALL
			SELECT MAX(last_indexed_block) AS b FROM sync_checkpoints WHERE chain_id = $1
		) AS latest
	`
	var latest sql.NullInt64
	err := r.pool.QueryRow(ctx, query, chainID).Scan(&latest)
	if err != nil {
		return 0, fmt.Errorf("failed to query latest block number: %w", err)
	}
	if latest.Valid && latest.Int64 > 0 {
		return uint64(latest.Int64), nil
	}
	return 0, nil
}
