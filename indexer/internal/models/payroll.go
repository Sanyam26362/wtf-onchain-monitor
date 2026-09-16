package models

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Employer represents an employer profile from the PostgreSQL employers table.
type Employer struct {
	Wallet               common.Address `json:"wallet"`
	Funds                *big.Int       `json:"funds"`
	TotalSalaryPerSecond *big.Int       `json:"total_salary_per_second"`
	Active               bool           `json:"active"`
	DeactivationTime     *int64         `json:"deactivation_time,omitempty"`
	AddedAt              *time.Time     `json:"added_at,omitempty"`
	RemovedAt            *time.Time     `json:"removed_at,omitempty"`
	LatestTxHash         *common.Hash   `json:"latest_tx_hash,omitempty"`
	ChainID              int64          `json:"chain_id"`
}

// Employee represents an employee profile from the PostgreSQL employees table.
type Employee struct {
	Wallet           common.Address `json:"wallet"`
	Employer         common.Address `json:"employer"`
	SalaryPerSecond  *big.Int       `json:"salary_per_second"`
	LastWithdraw     *int64         `json:"last_withdraw,omitempty"`
	Active           bool           `json:"active"`
	TotalLeaves      *big.Int       `json:"total_leaves,omitempty"`
	DeactivationTime *int64         `json:"deactivation_time,omitempty"`
	Allocation       *big.Int       `json:"allocation,omitempty"`
	AddedAt          *time.Time     `json:"added_at,omitempty"`
	RemovedAt        *time.Time     `json:"removed_at,omitempty"`
	LatestTxHash     *common.Hash   `json:"latest_tx_hash,omitempty"`
	ChainID          int64          `json:"chain_id"`
}

// PayrollFunding represents a historical payroll funding event.
type PayrollFunding struct {
	ID             int64          `json:"id"`
	Employer       common.Address `json:"employer"`
	Employee       common.Address `json:"employee"`
	AmountPaid     *big.Int       `json:"amount_paid"`
	Fee            *big.Int       `json:"fee"`
	AmountCredited *big.Int       `json:"amount_credited"`
	TxHash         common.Hash    `json:"tx_hash"`
	BlockNumber    uint64         `json:"block_number"`
	BlockTimestamp time.Time      `json:"block_timestamp"`
	LogIndex       uint           `json:"log_index"`
	ChainID        int64          `json:"chain_id"`
}

// SalaryClaim represents a historical salary claim event.
type SalaryClaim struct {
	ID             int64          `json:"id"`
	Employee       common.Address `json:"employee"`
	Amount         *big.Int       `json:"amount"`
	TxHash         common.Hash    `json:"tx_hash"`
	BlockNumber    uint64         `json:"block_number"`
	BlockTimestamp time.Time      `json:"block_timestamp"`
	LogIndex       uint           `json:"log_index"`
	ChainID        int64          `json:"chain_id"`
}

// EmployerResponse represents the API response DTO for an employer.
type EmployerResponse struct {
	Address                  string             `json:"address"`
	Active                   bool               `json:"active"`
	Funds                    string             `json:"funds"`
	TotalSalaryPerSecond     string             `json:"total_salary_per_second"`
	AddedAt                  *time.Time         `json:"added_at,omitempty"`
	RemovedAt                *time.Time         `json:"removed_at,omitempty"`
	LatestTxHash             *string            `json:"latest_tx_hash,omitempty"`
	Employees                []*EmployeeSummary `json:"employees"`
	PayrollBalanceProjection *string            `json:"payroll_balance_projection"`
	RunwayInputs             map[string]any     `json:"runway_inputs,omitempty"`
}

// EmployeeSummary represents a brief summary of an employee under an employer.
type EmployeeSummary struct {
	Address         string `json:"address"`
	SalaryPerSecond string `json:"salary_per_second"`
	Active          bool   `json:"active"`
}

// EmployeeResponse represents the API response DTO for an employee.
type EmployeeResponse struct {
	Address         string            `json:"address"`
	Employer        string            `json:"employer"`
	SalaryPerSecond string            `json:"salary_per_second"`
	LastWithdraw    *int64            `json:"last_withdraw,omitempty"`
	Active          bool              `json:"active"`
	TotalLeaves     string            `json:"total_leaves"`
	Allocation      *string           `json:"allocation,omitempty"`
	AddedAt         *time.Time        `json:"added_at,omitempty"`
	RemovedAt       *time.Time        `json:"removed_at,omitempty"`
	LatestTxHash    *string           `json:"latest_tx_hash,omitempty"`
	FundingHistory  []*PayrollFunding `json:"funding_history"`
	ClaimHistory    []*SalaryClaim    `json:"claim_history"`
}
