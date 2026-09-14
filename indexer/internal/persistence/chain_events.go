package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/decoder"
)

func (p *Postgres) SaveChainEvent(
	ctx context.Context,
	chainID int64,
	contractAddress common.Address,
	blockTimestamp uint64,
	event *decoder.DecodedEvent,
) error {
	if event == nil {
		return fmt.Errorf("decoded event is nil")
	}

	rawData, err := json.Marshal(event.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	const query = `
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
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9
		)
		ON CONFLICT (
			chain_id,
			contract_address,
			tx_hash,
			log_index
		)
		DO NOTHING
	`

	blockTime := time.Unix(int64(blockTimestamp), 0).UTC()

	_, err = p.pool.Exec(
		ctx,
		query,
		chainID,
		contractAddress.Hex(),
		string(event.Type),
		event.Log.TxHash.Hex(),
		event.Log.BlockNumber,
		blockTime,
		event.Log.Index,
		event.Log.Removed,
		rawData,
	)

	if err != nil {
		return fmt.Errorf(
			"failed to save chain event %s at block %d: %w",
			event.Type,
			event.Log.BlockNumber,
			err,
		)
	}

	return nil
}
