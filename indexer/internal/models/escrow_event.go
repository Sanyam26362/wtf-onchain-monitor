package models

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// EscrowEvent represents a single decoded event emitted by the WTFEscrow contract.
// Keyed by the blockchain log identity: (chain_id, contract_address, tx_hash, log_index).
// This guarantees exactly-once persistence and correct idempotency on re-runs or reorgs.
type EscrowEvent struct {
	ID              int64          `json:"id" db:"id"`
	ChainID         int64          `json:"chain_id" db:"chain_id"`
	ContractAddress common.Address `json:"contract_address" db:"contract_address"`
	EventType       string         `json:"event_type" db:"event_type"`
	TxHash          common.Hash    `json:"tx_hash" db:"tx_hash"`
	BlockNumber     uint64         `json:"block_number" db:"block_number"`
	BlockTimestamp  time.Time      `json:"block_timestamp" db:"block_timestamp"`
	LogIndex        uint           `json:"log_index" db:"log_index"`
	Removed         bool           `json:"removed" db:"removed"`
	// EscrowID is nullable: events like BudgetDeposited and BudgetWithdrawn do not carry an escrow ID.
	EscrowID  *string        `json:"escrow_id,omitempty" db:"escrow_id"`
	Amount    *string        `json:"amount,omitempty" db:"amount"`
	RawData   map[string]any `json:"raw_data" db:"raw_data"`
	CreatedAt time.Time      `json:"created_at" db:"created_at"`
}
