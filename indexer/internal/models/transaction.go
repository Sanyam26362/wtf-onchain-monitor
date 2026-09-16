package models

import (
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Transaction represents a stored blockchain transaction record.
type Transaction struct {
	TxHash        common.Hash     `json:"tx_hash"`
	ChainID       int64           `json:"chain_id"`
	BlockNumber   *uint64         `json:"block_number,omitempty"`
	Sender        common.Address  `json:"sender"`
	Recipient     *common.Address `json:"recipient,omitempty"`
	GasUsed       *uint64         `json:"gas_used,omitempty"`
	Status        string          `json:"status"`
	Confirmations uint64          `json:"confirmations"`
	ErrorData     json.RawMessage `json:"error_data,omitempty"`
	FirstSeenAt   time.Time       `json:"first_seen_at"`
	FinalizedAt   *time.Time      `json:"finalized_at,omitempty"`
}

// TransactionEvent represents a decoded event associated with a transaction.
type TransactionEvent struct {
	EventName       string          `json:"event_name"`
	ContractAddress common.Address  `json:"contract_address"`
	LogIndex        uint            `json:"log_index"`
	Data            json.RawMessage `json:"data,omitempty"`
}

// TransactionResponse represents the API response DTO for a transaction.
type TransactionResponse struct {
	Hash          string              `json:"hash"`
	ChainID       int64               `json:"chain_id"`
	Sender        string              `json:"sender"`
	Recipient     *string             `json:"recipient,omitempty"`
	Status        string              `json:"status"`
	BlockNumber   *uint64             `json:"block_number,omitempty"`
	Confirmations uint64              `json:"confirmations"`
	GasUsed       *string             `json:"gas_used,omitempty"`
	Events        []*TransactionEvent `json:"events"`
	ExplorerURL   string              `json:"explorer_url"`
	ErrorData     json.RawMessage     `json:"error_data,omitempty"`
	FirstSeenAt   time.Time           `json:"first_seen_at"`
}
