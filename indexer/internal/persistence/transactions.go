package persistence

import (
	"context"
	"fmt"
	"time"

	"worldtradefuture/indexer/internal/blockchain"
)

func (p *Postgres) SaveTransaction(
	ctx context.Context,
	chainID int64,
	metadata *blockchain.TransactionMetadata,
) error {
	if metadata == nil {
		return fmt.Errorf("transaction metadata is nil")
	}

	const query = `
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
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		ON CONFLICT (chain_id, tx_hash)
		DO UPDATE SET
			block_number = EXCLUDED.block_number,
			sender = EXCLUDED.sender,
			recipient = EXCLUDED.recipient,
			gas_used = EXCLUDED.gas_used,
			status = EXCLUDED.status
	`

	status := "confirmed"

	if metadata.Status == 0 {
		status = "failed"
	}

	var recipient string
	if metadata.Recipient != nil {
		recipient = metadata.Recipient.Hex()
	}

	_, err := p.pool.Exec(
		ctx,
		query,
		metadata.Hash.Hex(),
		chainID,
		metadata.BlockNumber,
		metadata.Sender.Hex(),
		recipient,
		metadata.GasUsed,
		status,
		1,
		time.Now(),
	)

	if err != nil {
		return fmt.Errorf(
			"failed to save transaction %s: %w",
			metadata.Hash.Hex(),
			err,
		)
	}

	return nil
}

// GetExistingTxHashes checks which transaction hashes are already stored in the transactions table.
func (p *Postgres) GetExistingTxHashes(
	ctx context.Context,
	chainID int64,
	hashes []string,
) (map[string]bool, error) {
	result := make(map[string]bool)
	if len(hashes) == 0 {
		return result, nil
	}

	const query = `
		SELECT tx_hash
		FROM transactions
		WHERE chain_id = $1 AND tx_hash = ANY($2)
	`
	rows, err := p.pool.Query(ctx, query, chainID, hashes)
	if err != nil {
		return nil, fmt.Errorf("failed to query existing tx hashes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		result[h] = true
	}
	return result, rows.Err()
}
