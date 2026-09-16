package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

type EmployeesRepository struct {
	pool *pgxpool.Pool
}

func NewEmployeesRepository(pool *pgxpool.Pool) *EmployeesRepository {
	return &EmployeesRepository{pool: pool}
}

// GetByAddress retrieves an employee profile and their recent funding and claim history.
func (r *EmployeesRepository) GetByAddress(ctx context.Context, chainID int64, address common.Address) (*models.EmployeeResponse, error) {
	const empQuery = `
		SELECT
			wallet,
			employer,
			salary_per_second,
			last_withdraw,
			active,
			total_leaves,
			allocation,
			added_at,
			removed_at,
			latest_tx_hash
		FROM employees
		WHERE chain_id = $1 AND LOWER(wallet) = LOWER($2)
	`

	var (
		walletStr       string
		employerStr     string
		salaryPerSecStr string
		lastWithdraw    sql.NullInt64
		active          bool
		totalLeavesStr  string
		allocationStr   sql.NullString
		addedAt         sql.NullTime
		removedAt       sql.NullTime
		latestTxHashStr sql.NullString
	)

	err := r.pool.QueryRow(ctx, empQuery, chainID, address.Hex()).Scan(
		&walletStr,
		&employerStr,
		&salaryPerSecStr,
		&lastWithdraw,
		&active,
		&totalLeavesStr,
		&allocationStr,
		&addedAt,
		&removedAt,
		&latestTxHashStr,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query employee: %w", err)
	}

	resp := &models.EmployeeResponse{
		Address:         common.HexToAddress(walletStr).Hex(),
		Employer:        common.HexToAddress(employerStr).Hex(),
		SalaryPerSecond: salaryPerSecStr,
		Active:          active,
		TotalLeaves:     totalLeavesStr,
		FundingHistory:  make([]*models.PayrollFunding, 0),
		ClaimHistory:    make([]*models.SalaryClaim, 0),
	}

	if lastWithdraw.Valid {
		lw := lastWithdraw.Int64
		resp.LastWithdraw = &lw
	}
	if allocationStr.Valid && allocationStr.String != "" {
		al := allocationStr.String
		resp.Allocation = &al
	}
	if addedAt.Valid {
		t := addedAt.Time.UTC()
		resp.AddedAt = &t
	}
	if removedAt.Valid {
		t := removedAt.Time.UTC()
		resp.RemovedAt = &t
	}
	if latestTxHashStr.Valid && latestTxHashStr.String != "" {
		h := common.HexToHash(latestTxHashStr.String).Hex()
		resp.LatestTxHash = &h
	}

	// Fetch recent funding history
	const fundingQuery = `
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
		WHERE chain_id = $1 AND LOWER(employee) = LOWER($2)
		ORDER BY block_number DESC, log_index DESC
		LIMIT 50
	`

	fRows, err := r.pool.Query(ctx, fundingQuery, chainID, address.Hex())
	if err != nil {
		return nil, fmt.Errorf("failed to query fundings for employee %s: %w", address.Hex(), err)
	}
	defer fRows.Close()

	for fRows.Next() {
		var (
			id                int64
			fEmployer         string
			fEmployee         string
			amountPaidStr     string
			feeStr            string
			amountCreditedStr string
			txHashStr         string
			blockNumber       int64
			blockTimestamp    time.Time
			logIndex          int
			cid               int64
		)

		if err := fRows.Scan(&id, &fEmployer, &fEmployee, &amountPaidStr, &feeStr, &amountCreditedStr, &txHashStr, &blockNumber, &blockTimestamp, &logIndex, &cid); err != nil {
			return nil, fmt.Errorf("failed to scan funding row: %w", err)
		}

		paid, _ := new(big.Int).SetString(amountPaidStr, 10)
		fee, _ := new(big.Int).SetString(feeStr, 10)
		credited, _ := new(big.Int).SetString(amountCreditedStr, 10)

		resp.FundingHistory = append(resp.FundingHistory, &models.PayrollFunding{
			ID:             id,
			Employer:       common.HexToAddress(fEmployer),
			Employee:       common.HexToAddress(fEmployee),
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

	if err := fRows.Err(); err != nil {
		return nil, fmt.Errorf("error reading funding rows: %w", err)
	}

	// Fetch recent claim history
	const claimQuery = `
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
		WHERE chain_id = $1 AND LOWER(employee) = LOWER($2)
		ORDER BY block_number DESC, log_index DESC
		LIMIT 50
	`

	cRows, err := r.pool.Query(ctx, claimQuery, chainID, address.Hex())
	if err != nil {
		return nil, fmt.Errorf("failed to query claims for employee %s: %w", address.Hex(), err)
	}
	defer cRows.Close()

	for cRows.Next() {
		var (
			id             int64
			cEmployee      string
			amountStr      string
			txHashStr      string
			blockNumber    int64
			blockTimestamp time.Time
			logIndex       int
			cid            int64
		)

		if err := cRows.Scan(&id, &cEmployee, &amountStr, &txHashStr, &blockNumber, &blockTimestamp, &logIndex, &cid); err != nil {
			return nil, fmt.Errorf("failed to scan claim row: %w", err)
		}

		amount, _ := new(big.Int).SetString(amountStr, 10)

		resp.ClaimHistory = append(resp.ClaimHistory, &models.SalaryClaim{
			ID:             id,
			Employee:       common.HexToAddress(cEmployee),
			Amount:         amount,
			TxHash:         common.HexToHash(txHashStr),
			BlockNumber:    uint64(blockNumber),
			BlockTimestamp: blockTimestamp.UTC(),
			LogIndex:       uint(logIndex),
			ChainID:        cid,
		})
	}

	if err := cRows.Err(); err != nil {
		return nil, fmt.Errorf("error reading claim rows: %w", err)
	}

	return resp, nil
}
