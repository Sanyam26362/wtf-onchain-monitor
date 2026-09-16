package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

type EmployersRepository struct {
	pool *pgxpool.Pool
}

func NewEmployersRepository(pool *pgxpool.Pool) *EmployersRepository {
	return &EmployersRepository{pool: pool}
}

// GetByAddress retrieves an employer profile and their associated employees.
func (r *EmployersRepository) GetByAddress(ctx context.Context, chainID int64, address common.Address) (*models.EmployerResponse, error) {
	const empQuery = `
		SELECT
			wallet,
			funds,
			total_salary_per_second,
			active,
			deactivation_time,
			added_at,
			removed_at,
			latest_tx_hash
		FROM employers
		WHERE chain_id = $1 AND LOWER(wallet) = LOWER($2)
	`

	var (
		walletStr            string
		fundsStr             string
		totalSalaryPerSecStr string
		active               bool
		deactTime            sql.NullInt64
		addedAt              sql.NullTime
		removedAt            sql.NullTime
		latestTxHashStr      sql.NullString
	)

	err := r.pool.QueryRow(ctx, empQuery, chainID, address.Hex()).Scan(
		&walletStr,
		&fundsStr,
		&totalSalaryPerSecStr,
		&active,
		&deactTime,
		&addedAt,
		&removedAt,
		&latestTxHashStr,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query employer: %w", err)
	}

	resp := &models.EmployerResponse{
		Address:                  common.HexToAddress(walletStr).Hex(),
		Active:                   active,
		Funds:                    fundsStr,
		TotalSalaryPerSecond:     totalSalaryPerSecStr,
		Employees:                make([]*models.EmployeeSummary, 0),
		PayrollBalanceProjection: nil, // Intentionally null: cannot project off-chain balances without token reserves
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

	// Fetch active/associated employees for this employer
	const employeesQuery = `
		SELECT
			wallet,
			salary_per_second,
			active
		FROM employees
		WHERE chain_id = $1 AND LOWER(employer) = LOWER($2)
		ORDER BY added_at DESC NULLS LAST, wallet ASC
	`

	rows, err := r.pool.Query(ctx, employeesQuery, chainID, address.Hex())
	if err != nil {
		return nil, fmt.Errorf("failed to query employees for employer %s: %w", address.Hex(), err)
	}
	defer rows.Close()

	activeCount := 0
	for rows.Next() {
		var (
			empWalletStr string
			salaryStr    string
			empActive    bool
		)
		if err := rows.Scan(&empWalletStr, &salaryStr, &empActive); err != nil {
			return nil, fmt.Errorf("failed to scan employee row: %w", err)
		}

		if empActive {
			activeCount++
		}

		resp.Employees = append(resp.Employees, &models.EmployeeSummary{
			Address:         common.HexToAddress(empWalletStr).Hex(),
			SalaryPerSecond: salaryStr,
			Active:          empActive,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating employees: %w", err)
	}

	resp.RunwayInputs = map[string]any{
		"funds":                   fundsStr,
		"total_salary_per_second": totalSalaryPerSecStr,
		"active_employees_count":  activeCount,
	}

	return resp, nil
}
