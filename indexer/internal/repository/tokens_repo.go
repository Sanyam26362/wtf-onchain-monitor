package repository

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

type TokenTransferFilter struct {
	ChainID       int64
	Token         common.Address
	Wallet        *common.Address
	Direction     string // "in", "out", "all"
	FromBlock     *uint64
	ToBlock       *uint64
	FromDate      *time.Time
	ToDate        *time.Time
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
}

type TokensRepository struct {
	pool *pgxpool.Pool
}

func NewTokensRepository(pool *pgxpool.Pool) *TokensRepository {
	return &TokensRepository{pool: pool}
}

// ListTransfers queries ERC-20 token transfers for a specific token contract with directional and range filters.
func (r *TokensRepository) ListTransfers(ctx context.Context, filter TokenTransferFilter) ([]*models.TokenTransfer, int64, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	whereClauses = append(whereClauses, fmt.Sprintf("chain_id = $%d", argIdx))
	args = append(args, filter.ChainID)
	argIdx++

	whereClauses = append(whereClauses, fmt.Sprintf("LOWER(token) = LOWER($%d)", argIdx))
	args = append(args, filter.Token.Hex())
	argIdx++

	if filter.Wallet != nil {
		dir := strings.ToLower(strings.TrimSpace(filter.Direction))
		switch dir {
		case "in":
			whereClauses = append(whereClauses, fmt.Sprintf("LOWER(to_address) = LOWER($%d)", argIdx))
			args = append(args, filter.Wallet.Hex())
			argIdx++
		case "out":
			whereClauses = append(whereClauses, fmt.Sprintf("LOWER(from_address) = LOWER($%d)", argIdx))
			args = append(args, filter.Wallet.Hex())
			argIdx++
		default: // "all" or unspecified
			whereClauses = append(whereClauses, fmt.Sprintf("(LOWER(from_address) = LOWER($%d) OR LOWER(to_address) = LOWER($%d))", argIdx, argIdx))
			args = append(args, filter.Wallet.Hex())
			argIdx++
		}
	}

	if filter.FromBlock != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("block_number >= $%d", argIdx))
		args = append(args, int64(*filter.FromBlock))
		argIdx++
	}

	if filter.ToBlock != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("block_number <= $%d", argIdx))
		args = append(args, int64(*filter.ToBlock))
		argIdx++
	}

	if filter.FromDate != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("block_timestamp >= $%d", argIdx))
		args = append(args, filter.FromDate.UTC())
		argIdx++
	}

	if filter.ToDate != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("block_timestamp <= $%d", argIdx))
		args = append(args, filter.ToDate.UTC())
		argIdx++
	}

	whereSQL := strings.Join(whereClauses, " AND ")

	// 1. Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM token_transfers WHERE %s", whereSQL)
	var total int64
	err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count token transfers: %w", err)
	}

	// 2. Fetch data rows
	sortBy := "block_number"
	switch filter.SortBy {
	case "block_number", "block_timestamp", "amount":
		sortBy = filter.SortBy
	}

	sortDir := "DESC"
	if strings.EqualFold(filter.SortDirection, "ASC") {
		sortDir = "ASC"
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	dataQuery := fmt.Sprintf(`
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
		WHERE %s
		ORDER BY %s %s, log_index DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, sortBy, sortDir, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query token transfers: %w", err)
	}
	defer rows.Close()

	transfers := make([]*models.TokenTransfer, 0)
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

		if err := rows.Scan(&id, &cid, &tokenStr, &fromStr, &toStr, &amountStr, &txHashStr, &blockNumber, &blockTimestamp, &logIndex, &removed, &createdAt); err != nil {
			return nil, 0, fmt.Errorf("failed to scan token transfer row: %w", err)
		}

		amount, _ := new(big.Int).SetString(amountStr, 10)

		transfers = append(transfers, &models.TokenTransfer{
			ID:             id,
			ChainID:        cid,
			Token:          common.HexToAddress(tokenStr),
			FromAddress:    common.HexToAddress(fromStr),
			ToAddress:      common.HexToAddress(toStr),
			Amount:         amount,
			TxHash:         common.HexToHash(txHashStr),
			BlockNumber:    uint64(blockNumber),
			BlockTimestamp: blockTimestamp.UTC(),
			LogIndex:       uint(logIndex),
			Removed:        removed,
			CreatedAt:      createdAt.UTC(),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading transfer rows: %w", err)
	}

	return transfers, total, nil
}
