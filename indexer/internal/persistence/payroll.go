package persistence

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/decoder"
)

// ProjectPayrollEvent updates domain projection tables (employers, employees,
// payroll_fundings, salary_claims) based on decoded MonthlyPayroll events.
// All writes are idempotent to support safe replays.
func (p *Postgres) ProjectPayrollEvent(
	ctx context.Context,
	chainID int64,
	blockTimestamp uint64,
	event *decoder.DecodedEvent,
) error {
	if event == nil {
		return fmt.Errorf("decoded event is nil")
	}

	blockTime := time.Unix(int64(blockTimestamp), 0).UTC()
	txHash := event.Log.TxHash.Hex()
	blockNum := event.Log.BlockNumber
	logIdx := event.Log.Index

	switch event.Type {
	case decoder.EventEmployerAdded:
		empAdded, ok := event.Data.(*abi.ABIEmployerAddedEvent)
		if !ok {
			return fmt.Errorf("invalid event data type for EmployerAdded")
		}
		return p.saveEmployerAdded(ctx, chainID, empAdded.Employer, blockTime, txHash)

	case decoder.EventEmployerRemoved:
		empRemoved, ok := event.Data.(*abi.ABIEmployerRemovedEvent)
		if !ok {
			return fmt.Errorf("invalid event data type for EmployerRemoved")
		}
		return p.saveEmployerRemoved(ctx, chainID, empRemoved.Employer, blockTime, int64(blockTimestamp), txHash)

	case decoder.EventEmployeeAdded:
		eeAdded, ok := event.Data.(*abi.ABIEmployeeAddedEvent)
		if !ok {
			return fmt.Errorf("invalid event data type for EmployeeAdded")
		}
		return p.saveEmployeeAdded(ctx, chainID, eeAdded, blockTime, txHash)

	case decoder.EventEmployeeRemoved:
		eeRemoved, ok := event.Data.(*abi.ABIEmployeeRemovedEvent)
		if !ok {
			return fmt.Errorf("invalid event data type for EmployeeRemoved")
		}
		return p.saveEmployeeRemoved(ctx, chainID, eeRemoved, blockTime, int64(blockTimestamp), txHash)

	case decoder.EventPayrollFunded:
		funded, ok := event.Data.(*abi.ABIPayrollFundedEvent)
		if !ok {
			return fmt.Errorf("invalid event data type for PayrollFunded")
		}
		return p.savePayrollFunded(ctx, chainID, funded, txHash, blockNum, blockTime, logIdx)

	case decoder.EventSalaryClaimed:
		claimed, ok := event.Data.(*abi.ABISalaryClaimedEvent)
		if !ok {
			return fmt.Errorf("invalid event data type for SalaryClaimed")
		}
		return p.saveSalaryClaimed(ctx, chainID, claimed, txHash, blockNum, blockTime, logIdx, int64(blockTimestamp))

	case decoder.EventOwnershipTransferred:
		// Ownership changes are tracked in chain_events table only
		return nil

	default:
		return nil
	}
}

func (p *Postgres) saveEmployerAdded(
	ctx context.Context,
	chainID int64,
	employer common.Address,
	addedAt time.Time,
	txHash string,
) error {
	const query = `
		INSERT INTO employers (
			chain_id,
			wallet,
			active,
			added_at,
			latest_tx_hash
		)
		VALUES ($1, $2, true, $3, $4)
		ON CONFLICT (chain_id, wallet)
		DO UPDATE SET
			active = true,
			added_at = COALESCE(employers.added_at, EXCLUDED.added_at),
			latest_tx_hash = EXCLUDED.latest_tx_hash
	`

	_, err := p.pool.Exec(ctx, query, chainID, employer.Hex(), addedAt, txHash)
	if err != nil {
		return fmt.Errorf("failed to save employer %s: %w", employer.Hex(), err)
	}
	return nil
}

func (p *Postgres) saveEmployerRemoved(
	ctx context.Context,
	chainID int64,
	employer common.Address,
	removedAt time.Time,
	deactivationTime int64,
	txHash string,
) error {
	const query = `
		INSERT INTO employers (
			chain_id,
			wallet,
			active,
			removed_at,
			deactivation_time,
			latest_tx_hash
		)
		VALUES ($1, $2, false, $3, $4, $5)
		ON CONFLICT (chain_id, wallet)
		DO UPDATE SET
			active = false,
			removed_at = EXCLUDED.removed_at,
			deactivation_time = EXCLUDED.deactivation_time,
			latest_tx_hash = EXCLUDED.latest_tx_hash
	`

	_, err := p.pool.Exec(ctx, query, chainID, employer.Hex(), removedAt, deactivationTime, txHash)
	if err != nil {
		return fmt.Errorf("failed to deactivate employer %s: %w", employer.Hex(), err)
	}
	return nil
}

func (p *Postgres) saveEmployeeAdded(
	ctx context.Context,
	chainID int64,
	event *abi.ABIEmployeeAddedEvent,
	addedAt time.Time,
	txHash string,
) error {
	// 1. Ensure employer exists to satisfy fk_employee_employer foreign key constraint
	const ensureEmployer = `
		INSERT INTO employers (
			chain_id,
			wallet,
			active,
			added_at,
			latest_tx_hash
		)
		VALUES ($1, $2, true, $3, $4)
		ON CONFLICT (chain_id, wallet) DO NOTHING
	`
	if _, err := p.pool.Exec(ctx, ensureEmployer, chainID, event.Employer.Hex(), addedAt, txHash); err != nil {
		return fmt.Errorf("failed to ensure employer %s for employee %s: %w", event.Employer.Hex(), event.Employee.Hex(), err)
	}

	// 2. Upsert employee
	const query = `
		INSERT INTO employees (
			chain_id,
			wallet,
			employer,
			salary_per_second,
			allocation,
			active,
			added_at,
			latest_tx_hash
		)
		VALUES ($1, $2, $3, $4, $5, true, $6, $7)
		ON CONFLICT (chain_id, wallet)
		DO UPDATE SET
			employer = EXCLUDED.employer,
			salary_per_second = EXCLUDED.salary_per_second,
			allocation = EXCLUDED.allocation,
			active = true,
			added_at = COALESCE(employees.added_at, EXCLUDED.added_at),
			latest_tx_hash = EXCLUDED.latest_tx_hash
	`

	salaryStr := "0"
	if event.Salary != nil {
		salaryStr = event.Salary.String()
	}

	allocationStr := "0"
	if event.Allocation != nil {
		allocationStr = event.Allocation.String()
	}

	_, err := p.pool.Exec(
		ctx,
		query,
		chainID,
		event.Employee.Hex(),
		event.Employer.Hex(),
		salaryStr,
		allocationStr,
		addedAt,
		txHash,
	)
	if err != nil {
		return fmt.Errorf("failed to save employee %s: %w", event.Employee.Hex(), err)
	}

	return nil
}

func (p *Postgres) saveEmployeeRemoved(
	ctx context.Context,
	chainID int64,
	event *abi.ABIEmployeeRemovedEvent,
	removedAt time.Time,
	deactivationTime int64,
	txHash string,
) error {
	// 1. Ensure employer exists to satisfy fk_employee_employer
	const ensureEmployer = `
		INSERT INTO employers (
			chain_id,
			wallet,
			active,
			added_at,
			latest_tx_hash
		)
		VALUES ($1, $2, true, $3, $4)
		ON CONFLICT (chain_id, wallet) DO NOTHING
	`
	if _, err := p.pool.Exec(ctx, ensureEmployer, chainID, event.Employer.Hex(), removedAt, txHash); err != nil {
		return fmt.Errorf("failed to ensure employer %s: %w", event.Employer.Hex(), err)
	}

	// 2. Upsert employee with active = false
	const query = `
		INSERT INTO employees (
			chain_id,
			wallet,
			employer,
			active,
			removed_at,
			deactivation_time,
			latest_tx_hash
		)
		VALUES ($1, $2, $3, false, $4, $5, $6)
		ON CONFLICT (chain_id, wallet)
		DO UPDATE SET
			active = false,
			removed_at = EXCLUDED.removed_at,
			deactivation_time = EXCLUDED.deactivation_time,
			latest_tx_hash = EXCLUDED.latest_tx_hash
	`

	_, err := p.pool.Exec(
		ctx,
		query,
		chainID,
		event.Employee.Hex(),
		event.Employer.Hex(),
		removedAt,
		deactivationTime,
		txHash,
	)
	if err != nil {
		return fmt.Errorf("failed to deactivate employee %s: %w", event.Employee.Hex(), err)
	}

	return nil
}

func (p *Postgres) savePayrollFunded(
	ctx context.Context,
	chainID int64,
	event *abi.ABIPayrollFundedEvent,
	txHash string,
	blockNumber uint64,
	blockTimestamp time.Time,
	logIndex uint,
) error {
	// 1. Ensure employer exists
	const ensureEmployer = `
		INSERT INTO employers (chain_id, wallet, active, added_at, latest_tx_hash)
		VALUES ($1, $2, true, $3, $4)
		ON CONFLICT (chain_id, wallet) DO NOTHING
	`
	if _, err := p.pool.Exec(ctx, ensureEmployer, chainID, event.Employer.Hex(), blockTimestamp, txHash); err != nil {
		return fmt.Errorf("failed to ensure employer %s for funding: %w", event.Employer.Hex(), err)
	}

	// 2. Ensure employee exists
	const ensureEmployee = `
		INSERT INTO employees (chain_id, wallet, employer, active, added_at, latest_tx_hash)
		VALUES ($1, $2, $3, true, $4, $5)
		ON CONFLICT (chain_id, wallet) DO NOTHING
	`
	if _, err := p.pool.Exec(ctx, ensureEmployee, chainID, event.Employee.Hex(), event.Employer.Hex(), blockTimestamp, txHash); err != nil {
		return fmt.Errorf("failed to ensure employee %s for funding: %w", event.Employee.Hex(), err)
	}

	// 3. Insert payroll_fundings
	const query = `
		INSERT INTO payroll_fundings (
			chain_id,
			employer,
			employee,
			amount_paid,
			fee,
			amount_credited,
			tx_hash,
			block_number,
			block_timestamp,
			log_index
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (chain_id, tx_hash, log_index)
		DO NOTHING
	`

	amountPaid := "0"
	if event.AmountPaid != nil {
		amountPaid = event.AmountPaid.String()
	}
	fee := "0"
	if event.Fee != nil {
		fee = event.Fee.String()
	}
	amountCredited := "0"
	if event.AmountCredited != nil {
		amountCredited = event.AmountCredited.String()
	}

	_, err := p.pool.Exec(
		ctx,
		query,
		chainID,
		event.Employer.Hex(),
		event.Employee.Hex(),
		amountPaid,
		fee,
		amountCredited,
		txHash,
		int64(blockNumber),
		blockTimestamp,
		int(logIndex),
	)
	if err != nil {
		return fmt.Errorf("failed to save payroll funding tx %s log %d: %w", txHash, logIndex, err)
	}

	return nil
}

func (p *Postgres) saveSalaryClaimed(
	ctx context.Context,
	chainID int64,
	event *abi.ABISalaryClaimedEvent,
	txHash string,
	blockNumber uint64,
	blockTimestamp time.Time,
	logIndex uint,
	withdrawTime int64,
) error {
	// 1. Ensure employee exists to satisfy fk_salary_claim_employee
	// Check if employee already exists
	var exists bool
	checkQuery := `SELECT EXISTS(SELECT 1 FROM employees WHERE chain_id = $1 AND wallet = $2)`
	_ = p.pool.QueryRow(ctx, checkQuery, chainID, event.Employee.Hex()).Scan(&exists)

	if !exists {
		// Insert a placeholder employer if needed
		placeholderEmployer := "0x0000000000000000000000000000000000000000"
		const ensureEmployer = `
			INSERT INTO employers (chain_id, wallet, active, added_at, latest_tx_hash)
			VALUES ($1, $2, true, $3, $4)
			ON CONFLICT (chain_id, wallet) DO NOTHING
		`
		_, _ = p.pool.Exec(ctx, ensureEmployer, chainID, placeholderEmployer, blockTimestamp, txHash)

		const ensureEmployee = `
			INSERT INTO employees (chain_id, wallet, employer, active, added_at, latest_tx_hash)
			VALUES ($1, $2, $3, true, $4, $5)
			ON CONFLICT (chain_id, wallet) DO NOTHING
		`
		_, _ = p.pool.Exec(ctx, ensureEmployee, chainID, event.Employee.Hex(), placeholderEmployer, blockTimestamp, txHash)
	}

	// 2. Insert salary_claims
	const query = `
		INSERT INTO salary_claims (
			chain_id,
			employee,
			amount,
			tx_hash,
			block_number,
			block_timestamp,
			log_index
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (chain_id, tx_hash, log_index)
		DO NOTHING
	`

	amount := "0"
	if event.Amount != nil {
		amount = event.Amount.String()
	}

	_, err := p.pool.Exec(
		ctx,
		query,
		chainID,
		event.Employee.Hex(),
		amount,
		txHash,
		int64(blockNumber),
		blockTimestamp,
		int(logIndex),
	)
	if err != nil {
		return fmt.Errorf("failed to save salary claim tx %s log %d: %w", txHash, logIndex, err)
	}

	// 3. Update last_withdraw on employee
	const updateWithdraw = `
		UPDATE employees
		SET last_withdraw = $3, latest_tx_hash = $4
		WHERE chain_id = $1 AND wallet = $2
	`
	_, _ = p.pool.Exec(ctx, updateWithdraw, chainID, event.Employee.Hex(), withdrawTime, txHash)

	return nil
}

// Ensure interface compatibility
var _ interface {
	ProjectPayrollEvent(ctx context.Context, chainID int64, blockTimestamp uint64, event *decoder.DecodedEvent) error
} = (*Postgres)(nil)
