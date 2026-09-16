package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/models"
)

// RawChainEventInput holds data for raw chain event persistence.
type RawChainEventInput struct {
	ContractAddress common.Address
	EventName       string
	TxHash          common.Hash
	BlockNumber     uint64
	BlockTimestamp  time.Time
	LogIndex        uint
	Removed         bool
	RawData         interface{}
}

// SaveTokenTransfer persists a single token transfer record idempotently.
func (p *Postgres) SaveTokenTransfer(
	ctx context.Context,
	transfer *models.TokenTransfer,
) error {
	if transfer == nil {
		return fmt.Errorf("transfer is nil")
	}

	const query = `
		INSERT INTO token_transfers (
			chain_id,
			token,
			from_address,
			to_address,
			amount,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (chain_id, token, tx_hash, log_index)
		DO UPDATE SET
			removed = EXCLUDED.removed,
			block_timestamp = EXCLUDED.block_timestamp
	`

	_, err := p.pool.Exec(
		ctx,
		query,
		transfer.ChainID,
		transfer.Token.Hex(),
		transfer.FromAddress.Hex(),
		transfer.ToAddress.Hex(),
		transfer.Amount.String(),
		transfer.TxHash.Hex(),
		int64(transfer.BlockNumber),
		transfer.BlockTimestamp.UTC(),
		int(transfer.LogIndex),
		transfer.Removed,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to save token transfer tx %s log %d: %w",
			transfer.TxHash.Hex(),
			transfer.LogIndex,
			err,
		)
	}

	return nil
}

// SaveTokenTransfers persists multiple token transfers idempotently.
func (p *Postgres) SaveTokenTransfers(
	ctx context.Context,
	transfers []*models.TokenTransfer,
) error {
	for _, t := range transfers {
		if err := p.SaveTokenTransfer(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

// SaveTokenBatch persists transactions, raw events, normalized transfers, and the checkpoint
// inside a single database transaction for atomic consistency.
func (p *Postgres) SaveTokenBatch(
	ctx context.Context,
	chainID int64,
	streamID string,
	txs []*blockchain.TransactionMetadata,
	rawEvents []*RawChainEventInput,
	transfers []*models.TokenTransfer,
	checkpointBlock uint64,
	checkpointHash string,
) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Save transactions
	const txQuery = `
		INSERT INTO transactions (
			tx_hash,
			chain_id,
			block_number,
			sender,
			recipient,
			gas_used,
			status,
			confirmations,
			first_seen_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (chain_id, tx_hash)
		DO UPDATE SET
			block_number = EXCLUDED.block_number,
			sender = EXCLUDED.sender,
			recipient = EXCLUDED.recipient,
			gas_used = EXCLUDED.gas_used,
			status = EXCLUDED.status
	`
	for _, meta := range txs {
		if meta == nil {
			continue
		}
		status := "confirmed"
		if meta.Status == 0 {
			status = "failed"
		}
		var recipient string
		if meta.Recipient != nil {
			recipient = meta.Recipient.Hex()
		}

		_, err := tx.Exec(
			ctx,
			txQuery,
			meta.Hash.Hex(),
			chainID,
			meta.BlockNumber,
			meta.Sender.Hex(),
			recipient,
			meta.GasUsed,
			status,
			1,
			time.Now(),
		)
		if err != nil {
			return fmt.Errorf("failed to save transaction %s in batch: %w", meta.Hash.Hex(), err)
		}
	}

	// 2. Save raw chain events
	const eventQuery = `
		INSERT INTO chain_events (
			chain_id,
			contract_address,
			event_name,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed,
			raw_data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (chain_id, contract_address, tx_hash, log_index)
		DO UPDATE SET
			removed = EXCLUDED.removed,
			block_timestamp = EXCLUDED.block_timestamp
	`
	for _, re := range rawEvents {
		if re == nil {
			continue
		}
		rawBytes, err := json.Marshal(re.RawData)
		if err != nil {
			return fmt.Errorf("failed to marshal raw event data: %w", err)
		}

		_, err = tx.Exec(
			ctx,
			eventQuery,
			chainID,
			re.ContractAddress.Hex(),
			re.EventName,
			re.TxHash.Hex(),
			int64(re.BlockNumber),
			re.BlockTimestamp.UTC(),
			int(re.LogIndex),
			re.Removed,
			rawBytes,
		)
		if err != nil {
			return fmt.Errorf("failed to save raw chain event %s in batch: %w", re.EventName, err)
		}
	}

	// 3. Save normalized token transfers
	const transferQuery = `
		INSERT INTO token_transfers (
			chain_id,
			token,
			from_address,
			to_address,
			amount,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (chain_id, token, tx_hash, log_index)
		DO UPDATE SET
			removed = EXCLUDED.removed,
			block_timestamp = EXCLUDED.block_timestamp
	`
	for _, t := range transfers {
		if t == nil {
			continue
		}
		_, err := tx.Exec(
			ctx,
			transferQuery,
			t.ChainID,
			t.Token.Hex(),
			t.FromAddress.Hex(),
			t.ToAddress.Hex(),
			t.Amount.String(),
			t.TxHash.Hex(),
			int64(t.BlockNumber),
			t.BlockTimestamp.UTC(),
			int(t.LogIndex),
			t.Removed,
		)
		if err != nil {
			return fmt.Errorf("failed to save transfer tx %s log %d in batch: %w", t.TxHash.Hex(), t.LogIndex, err)
		}
	}

	// 4. Update checkpoint
	if streamID != "" && checkpointBlock > 0 {
		const checkpointQuery = `
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
		_, err := tx.Exec(
			ctx,
			checkpointQuery,
			chainID,
			streamID,
			int64(checkpointBlock),
			checkpointHash,
		)
		if err != nil {
			return fmt.Errorf("failed to update checkpoint for %s to block %d in batch: %w", streamID, checkpointBlock, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	return nil
}

// GetTokenTransfers fetches token transfers for a given token and chain, ordered by block_number and log_index.
func (p *Postgres) GetTokenTransfers(
	ctx context.Context,
	chainID int64,
	token common.Address,
	limit int,
) ([]*models.TokenTransfer, error) {
	if limit <= 0 {
		limit = 100
	}

	const query = `
		SELECT
			id,
			chain_id,
			token,
			from_address,
			to_address,
			amount,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			removed,
			created_at
		FROM token_transfers
		WHERE chain_id = $1 AND LOWER(token) = LOWER($2)
		ORDER BY block_number ASC, log_index ASC
		LIMIT $3
	`

	rows, err := p.pool.Query(ctx, query, chainID, token.Hex(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query token transfers: %w", err)
	}
	defer rows.Close()

	var result []*models.TokenTransfer
	for rows.Next() {
		var (
			id             int64
			cid            int64
			tokenStr       string
			fromStr        string
			toStr          string
			amountStr      string
			txHashStr      string
			blockNumber    int64
			blockTimestamp time.Time
			logIndex       int
			removed        bool
			createdAt      time.Time
		)

		err := rows.Scan(
			&id,
			&cid,
			&tokenStr,
			&fromStr,
			&toStr,
			&amountStr,
			&txHashStr,
			&blockNumber,
			&blockTimestamp,
			&logIndex,
			&removed,
			&createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan token transfer row: %w", err)
		}

		amount, ok := new(big.Int).SetString(amountStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to parse amount string: %s", amountStr)
		}

		result = append(result, &models.TokenTransfer{
			ID:             id,
			ChainID:        cid,
			Token:          common.HexToAddress(tokenStr),
			FromAddress:    common.HexToAddress(fromStr),
			ToAddress:      common.HexToAddress(toStr),
			Amount:         amount,
			TxHash:         common.HexToHash(txHashStr),
			BlockNumber:    uint64(blockNumber),
			BlockTimestamp: blockTimestamp,
			LogIndex:       uint(logIndex),
			Removed:        removed,
			CreatedAt:      createdAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return result, nil
}
