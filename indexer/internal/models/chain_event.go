package models

import "time"

// ChainEvent represents an on-chain event record stored in the PostgreSQL chain_events table.
type ChainEvent struct {
	EventID          int64     `json:"event_id"`
	ChainID          int64     `json:"chain_id"`
	ContractAddress  string    `json:"contract_address"`
	EventName        string    `json:"event_name"`
	TxHash           string    `json:"tx_hash"`
	BlockNumber      int64     `json:"block_number"`
	BlockHash        string    `json:"block_hash"`
	LogIndex         int       `json:"log_index"`
	TxIndex          int       `json:"tx_index"`
	BlockTimestamp   int64     `json:"block_timestamp"`
	EscrowID         string    `json:"escrow_id,omitempty"`
	Buyer            string    `json:"buyer,omitempty"`
	Seller           string    `json:"seller,omitempty"`
	Amount           string    `json:"amount,omitempty"`
	AlchemyWebhookID string    `json:"alchemy_webhook_id,omitempty"`
	RawPayload       string    `json:"raw_payload,omitempty"`
	IndexedAt        time.Time `json:"indexed_at"`
}
