package repository

import (
	"context"
	"fmt"
	"math/big"
	"sort"
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

// GetTransfersByBlockRange retrieves all token transfer records for a specific chain, token, and block range [fromBlock, toBlock].
func (r *TokensRepository) GetTransfersByBlockRange(ctx context.Context, chainID int64, token common.Address, fromBlock, toBlock uint64) ([]*models.TokenTransfer, error) {
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
		WHERE chain_id = $1 
		  AND LOWER(token) = LOWER($2) 
		  AND block_number >= $3 
		  AND block_number <= $4
		ORDER BY block_number ASC, log_index ASC, id ASC
	`
	rows, err := r.pool.Query(ctx, query, chainID, token.Hex(), int64(fromBlock), int64(toBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to query token transfers by block range: %w", err)
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
			return nil, fmt.Errorf("failed to scan token transfer row: %w", err)
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
		return nil, fmt.Errorf("error reading token transfer rows: %w", err)
	}

	return transfers, nil
}

// GetRelevantAddresses discovers all unique non-zero addresses involved in token transfers,
// payroll fundings, and salary claims up to upToBlock, plus includes payrollContract.
func (r *TokensRepository) GetRelevantAddresses(
	ctx context.Context,
	chainID int64,
	token common.Address,
	payrollContract common.Address,
	upToBlock uint64,
) ([]common.Address, error) {
	const query = `
		SELECT DISTINCT address FROM (
			SELECT LOWER(from_address) AS address FROM token_transfers WHERE chain_id = $1 AND LOWER(token) = LOWER($2) AND block_number <= $3
			UNION
			SELECT LOWER(to_address) AS address FROM token_transfers WHERE chain_id = $1 AND LOWER(token) = LOWER($2) AND block_number <= $3
			UNION
			SELECT LOWER(employer) AS address FROM payroll_fundings WHERE chain_id = $1 AND block_number <= $3
			UNION
			SELECT LOWER(employee) AS address FROM payroll_fundings WHERE chain_id = $1 AND block_number <= $3
			UNION
			SELECT LOWER(employee) AS address FROM salary_claims WHERE chain_id = $1 AND block_number <= $3
		) addr_sub
		WHERE address != '0x0000000000000000000000000000000000000000'
		ORDER BY address ASC
	`
	rows, err := r.pool.Query(ctx, query, chainID, token.Hex(), int64(upToBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to query relevant addresses: %w", err)
	}
	defer rows.Close()

	seen := make(map[common.Address]bool)
	var addrs []common.Address

	for rows.Next() {
		var addrStr string
		if err := rows.Scan(&addrStr); err != nil {
			return nil, fmt.Errorf("failed to scan relevant address: %w", err)
		}
		a := common.HexToAddress(addrStr)
		if a != (common.Address{}) && !seen[a] {
			seen[a] = true
			addrs = append(addrs, a)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating relevant addresses: %w", err)
	}

	// Ensure payroll contract is included if provided and non-zero
	zeroAddr := common.HexToAddress("0x0000000000000000000000000000000000000000")
	if payrollContract != zeroAddr && payrollContract != (common.Address{}) && !seen[payrollContract] {
		seen[payrollContract] = true
		addrs = append(addrs, payrollContract)
	}

	// Deterministic sort by lowercase hex
	sort.Slice(addrs, func(i, j int) bool {
		return strings.ToLower(addrs[i].Hex()) < strings.ToLower(addrs[j].Hex())
	})

	return addrs, nil
}

// CalculateTokenBalance calculates the net balance (incoming - outgoing) for a specific address up to upToBlock.
func (r *TokensRepository) CalculateTokenBalance(
	ctx context.Context,
	chainID int64,
	token common.Address,
	wallet common.Address,
	upToBlock uint64,
) (*big.Int, error) {
	const query = `
		SELECT 
			COALESCE(
				(SELECT SUM(amount) FROM token_transfers WHERE chain_id = $1 AND LOWER(token) = LOWER($2) AND LOWER(to_address) = LOWER($3) AND block_number <= $4 AND removed = false),
				0
			) - COALESCE(
				(SELECT SUM(amount) FROM token_transfers WHERE chain_id = $1 AND LOWER(token) = LOWER($2) AND LOWER(from_address) = LOWER($3) AND block_number <= $4 AND removed = false),
				0
			) AS balance
	`
	var balStr string
	err := r.pool.QueryRow(ctx, query, chainID, token.Hex(), wallet.Hex(), int64(upToBlock)).Scan(&balStr)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate token balance for %s: %w", wallet.Hex(), err)
	}

	bal, ok := new(big.Int).SetString(balStr, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse balance string %q for %s", balStr, wallet.Hex())
	}

	return bal, nil
}

// CalculateAllTokenBalances calculates net balances for all addresses with transfers up to upToBlock.
func (r *TokensRepository) CalculateAllTokenBalances(
	ctx context.Context,
	chainID int64,
	token common.Address,
	upToBlock uint64,
) (map[common.Address]*big.Int, error) {
	const query = `
		SELECT
			addr,
			COALESCE(SUM(incoming), 0) - COALESCE(SUM(outgoing), 0) AS balance
		FROM (
			SELECT LOWER(to_address) AS addr, amount AS incoming, 0::numeric AS outgoing
			FROM token_transfers
			WHERE chain_id = $1 AND LOWER(token) = LOWER($2) AND block_number <= $3 AND removed = false
			UNION ALL
			SELECT LOWER(from_address) AS addr, 0::numeric AS incoming, amount AS outgoing
			FROM token_transfers
			WHERE chain_id = $1 AND LOWER(token) = LOWER($2) AND block_number <= $3 AND removed = false
		) movements
		WHERE addr != '0x0000000000000000000000000000000000000000'
		GROUP BY addr
	`
	rows, err := r.pool.Query(ctx, query, chainID, token.Hex(), int64(upToBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to calculate all token balances: %w", err)
	}
	defer rows.Close()

	balances := make(map[common.Address]*big.Int)
	for rows.Next() {
		var addrStr, balStr string
		if err := rows.Scan(&addrStr, &balStr); err != nil {
			return nil, fmt.Errorf("failed to scan balance row: %w", err)
		}
		bal, ok := new(big.Int).SetString(balStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to parse balance string %q for %s", balStr, addrStr)
		}
		balances[common.HexToAddress(addrStr)] = bal
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading balance rows: %w", err)
	}

	return balances, nil
}
