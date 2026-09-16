package models

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// TokenTransfer represents a normalized ERC-20 token transfer record.
type TokenTransfer struct {
	ID             int64          `json:"id"`
	ChainID        int64          `json:"chain_id"`
	Token          common.Address `json:"token"`
	FromAddress    common.Address `json:"from_address"`
	ToAddress      common.Address `json:"to_address"`
	Amount         *big.Int       `json:"amount"`
	TxHash         common.Hash    `json:"tx_hash"`
	BlockNumber    uint64         `json:"block_number"`
	BlockTimestamp time.Time      `json:"block_timestamp"`
	LogIndex       uint           `json:"log_index"`
	Removed        bool           `json:"removed"`
	CreatedAt      time.Time      `json:"created_at"`
}

// SyncCheckpoint represents the latest indexed block for a stream on a given chain.
type SyncCheckpoint struct {
	ChainID          int64     `json:"chain_id"`
	StreamID         string    `json:"stream_id"`
	LastIndexedBlock uint64    `json:"last_indexed_block"`
	LastBlockHash    string    `json:"last_block_hash,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}
