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

type PayrollFundingFilter struct {
	ChainID       int64
	Employer      *common.Address
	Employee      *common.Address
	FromBlock     *uint64
	ToBlock       *uint64
	FromDate      *time.Time
	ToDate        *time.Time
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
}

type SalaryClaimFilter struct {
	ChainID       int64
	Employee      *common.Address
	FromBlock     *uint64
	ToBlock       *uint64
	FromDate      *time.Time
	ToDate        *time.Time
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
}

type PayrollRepository struct {
	pool *pgxpool.Pool
}

func NewPayrollRepository(pool *pgxpool.Pool) *PayrollRepository {
	return &PayrollRepository{pool: pool}
}

// ListFundings queries payroll funding events with dynamic filtering, total count, and pagination.
func (r *PayrollRepository) ListFundings(ctx context.Context, filter PayrollFundingFilter) ([]*models.PayrollFunding, int64, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	whereClauses = append(whereClauses, fmt.Sprintf("chain_id = $%d", argIdx))
	args = append(args, filter.ChainID)
	argIdx++

	if filter.Employer != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(employer) = LOWER($%d)", argIdx))
		args = append(args, filter.Employer.Hex())
		argIdx++
	}

	if filter.Employee != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(employee) = LOWER($%d)", argIdx))
		args = append(args, filter.Employee.Hex())
		argIdx++
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
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM payroll_fundings WHERE %s", whereSQL)
	var total int64
	err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count payroll fundings: %w", err)
	}

	// 2. Fetch page rows
	sortBy := "block_number"
	switch filter.SortBy {
	case "block_number", "block_timestamp", "amount_paid", "amount_credited":
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
			employer,
			employee,
			amount_paid,
			fee,
			amount_credited,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			chain_id
		FROM payroll_fundings
		WHERE %s
		ORDER BY %s %s, log_index DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, sortBy, sortDir, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query payroll fundings: %w", err)
	}
	defer rows.Close()

	fundings := make([]*models.PayrollFunding, 0)
	for rows.Next() {
		var (
			id                int64
			employerStr       string
			employeeStr       string
			amountPaidStr     string
			feeStr            string
			amountCreditedStr string
			txHashStr         string
			blockNumber       int64
			blockTimestamp    time.Time
			logIndex          int
			cid               int64
		)

		if err := rows.Scan(&id, &employerStr, &employeeStr, &amountPaidStr, &feeStr, &amountCreditedStr, &txHashStr, &blockNumber, &blockTimestamp, &logIndex, &cid); err != nil {
			return nil, 0, fmt.Errorf("failed to scan funding row: %w", err)
		}

		paid, _ := new(big.Int).SetString(amountPaidStr, 10)
		fee, _ := new(big.Int).SetString(feeStr, 10)
		credited, _ := new(big.Int).SetString(amountCreditedStr, 10)

		fundings = append(fundings, &models.PayrollFunding{
			ID:             id,
			Employer:       common.HexToAddress(employerStr),
			Employee:       common.HexToAddress(employeeStr),
			AmountPaid:     paid,
			Fee:            fee,
			AmountCredited: credited,
			TxHash:         common.HexToHash(txHashStr),
			BlockNumber:    uint64(blockNumber),
			BlockTimestamp: blockTimestamp.UTC(),
			LogIndex:       uint(logIndex),
			ChainID:        cid,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading funding rows: %w", err)
	}

	return fundings, total, nil
}

// ListClaims queries salary claim events with dynamic filtering, total count, and pagination.
func (r *PayrollRepository) ListClaims(ctx context.Context, filter SalaryClaimFilter) ([]*models.SalaryClaim, int64, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	whereClauses = append(whereClauses, fmt.Sprintf("chain_id = $%d", argIdx))
	args = append(args, filter.ChainID)
	argIdx++

	if filter.Employee != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(employee) = LOWER($%d)", argIdx))
		args = append(args, filter.Employee.Hex())
		argIdx++
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

	// 1. Count query
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM salary_claims WHERE %s", whereSQL)
	var total int64
	err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count salary claims: %w", err)
	}

	// 2. Data query
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
			employee,
			amount,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			chain_id
		FROM salary_claims
		WHERE %s
		ORDER BY %s %s, log_index DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, sortBy, sortDir, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query salary claims: %w", err)
	}
	defer rows.Close()

	claims := make([]*models.SalaryClaim, 0)
	for rows.Next() {
		var (
			id             int64
			employeeStr    string
			amountStr      string
			txHashStr      string
			blockNumber    int64
			blockTimestamp time.Time
			logIndex       int
			cid            int64
		)

		if err := rows.Scan(&id, &employeeStr, &amountStr, &txHashStr, &blockNumber, &blockTimestamp, &logIndex, &cid); err != nil {
			return nil, 0, fmt.Errorf("failed to scan claim row: %w", err)
		}

		amount, _ := new(big.Int).SetString(amountStr, 10)

		claims = append(claims, &models.SalaryClaim{
			ID:             id,
			Employee:       common.HexToAddress(employeeStr),
			Amount:         amount,
			TxHash:         common.HexToHash(txHashStr),
			BlockNumber:    uint64(blockNumber),
			BlockTimestamp: blockTimestamp.UTC(),
			LogIndex:       uint(logIndex),
			ChainID:        cid,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading claim rows: %w", err)
	}

	return claims, total, nil
}

// GetFundingsByBlockRange retrieves all payroll funding records within a block range [fromBlock, toBlock].
func (r *PayrollRepository) GetFundingsByBlockRange(ctx context.Context, chainID int64, fromBlock, toBlock uint64) ([]*models.PayrollFunding, error) {
	const query = `
		SELECT
			id,
			employer,
			employee,
			amount_paid,
			fee,
			amount_credited,
			tx_hash,
			block_number,
			block_timestamp,
			log_index,
			chain_id
		FROM payroll_fundings
		WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3
		ORDER BY block_number ASC, log_index ASC, id ASC
	`
	rows, err := r.pool.Query(ctx, query, chainID, int64(fromBlock), int64(toBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to query payroll fundings by block range: %w", err)
	}
	defer rows.Close()

	fundings := make([]*models.PayrollFunding, 0)
	for rows.Next() {
		var (
			id                int64
			employerStr       string
			employeeStr       string
			amountPaidStr     string
			feeStr            string
			amountCreditedStr string
			txHashStr         string
			blockNumber       int64
			blockTimestamp    time.Time
			logIndex          int
			cid               int64
		)

		if err := rows.Scan(&id, &employerStr, &employeeStr, &amountPaidStr, &feeStr, &amountCreditedStr, &txHashStr, &blockNumber, &blockTimestamp, &logIndex, &cid); err != nil {
			return nil, fmt.Errorf("failed to scan funding row: %w", err)
		}

		paid, _ := new(big.Int).SetString(amountPaidStr, 10)
		fee, _ := new(big.Int).SetString(feeStr, 10)
		credited, _ := new(big.Int).SetString(amountCreditedStr, 10)

		fundings = append(fundings, &models.PayrollFunding{
			ID:             id,
			Employer:       common.HexToAddress(employerStr),
			Employee:       common.HexToAddress(employeeStr),
			AmountPaid:     paid,
			Fee:            fee,
			AmountCredited: credited,
			TxHash:         common.HexToHash(txHashStr),
			BlockNumber:    uint64(blockNumber),
			BlockTimestamp: blockTimestamp.UTC(),
			LogIndex:       uint(logIndex),
			ChainID:        cid,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading funding rows: %w", err)
	}

	return fundings, nil
}
