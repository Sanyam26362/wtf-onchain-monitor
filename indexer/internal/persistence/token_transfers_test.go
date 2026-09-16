package persistence

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/models"
)

func getTestPostgres(t *testing.T) *Postgres {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:root@localhost:5432/wtf_onchain"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pg, err := NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping database test: failed to connect to postgres: %v", err)
	}

	return pg
}

func TestPersistence_TokenTransfer_PersistenceAndIdempotency(t *testing.T) {
	pg := getTestPostgres(t)
	defer pg.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tokenAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	fromAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	toAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	txHash := common.HexToHash("0xaaaabbbbccccddddeeeeffff0000111122223333444455556666777788889999")
	blockNumber := uint64(5000000)
	logIndex := uint(1)
	amount, _ := new(big.Int).SetString("999999999999999999999999999", 10)
	now := time.Now().UTC().Truncate(time.Second)

	// Ensure transaction exists first to satisfy foreign key constraint
	txMeta := &blockchain.TransactionMetadata{
		Hash:        txHash,
		BlockNumber: blockNumber,
		Sender:      fromAddr,
		Recipient:   &toAddr,
		GasUsed:     21000,
		Status:      1,
	}
	if err := pg.SaveTransaction(ctx, chainID, txMeta); err != nil {
		t.Fatalf("failed to save transaction: %v", err)
	}

	transfer := &models.TokenTransfer{
		ChainID:        chainID,
		Token:          tokenAddr,
		FromAddress:    fromAddr,
		ToAddress:      toAddr,
		Amount:         amount,
		TxHash:         txHash,
		BlockNumber:    blockNumber,
		BlockTimestamp: now,
		LogIndex:       logIndex,
		Removed:        false,
	}

	// 1. First insert
	if err := pg.SaveTokenTransfer(ctx, transfer); err != nil {
		t.Fatalf("failed to save token transfer: %v", err)
	}

	// 2. Second insert (Idempotency test - must not fail or duplicate)
	if err := pg.SaveTokenTransfer(ctx, transfer); err != nil {
		t.Fatalf("failed to re-save same token transfer (idempotency): %v", err)
	}

	// Query count of records with this unique event key
	var count int
	countQuery := `
		SELECT count(*) FROM token_transfers
		WHERE chain_id = $1 AND token = $2 AND tx_hash = $3 AND log_index = $4
	`
	err := pg.pool.QueryRow(ctx, countQuery, chainID, tokenAddr.Hex(), txHash.Hex(), int(logIndex)).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count records: %v", err)
	}
	if count != 1 {
		t.Fatalf("idempotency violated: expected 1 record, got %d", count)
	}

	// Verify fields
	transfers, err := pg.GetTokenTransfers(ctx, chainID, tokenAddr, 10)
	if err != nil {
		t.Fatalf("failed to get token transfers: %v", err)
	}

	var found *models.TokenTransfer
	for _, tr := range transfers {
		if tr.TxHash == txHash && tr.LogIndex == logIndex {
			found = tr
			break
		}
	}
	if found == nil {
		t.Fatalf("saved token transfer not found in query results")
	}

	if found.ChainID != chainID {
		t.Errorf("expected chain ID %d, got %d", chainID, found.ChainID)
	}
	if found.BlockNumber != blockNumber {
		t.Errorf("expected block number %d, got %d", blockNumber, found.BlockNumber)
	}
	if found.LogIndex != logIndex {
		t.Errorf("expected log index %d, got %d", logIndex, found.LogIndex)
	}
	if found.Amount.Cmp(amount) != 0 {
		t.Errorf("expected amount %s, got %s", amount.String(), found.Amount.String())
	}
}

func TestPersistence_TokenTransfer_MultipleEventsPerTransaction(t *testing.T) {
	pg := getTestPostgres(t)
	defer pg.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	tokenAddr := common.HexToAddress("0x7777777777777777777777777777777777777777")
	txHash := common.HexToHash("0x1234123412341234123412341234123412341234123412341234123412341234")
	blockNumber := uint64(6000000)
	sender := common.HexToAddress("0x8888888888888888888888888888888888888888")

	// Save transaction
	txMeta := &blockchain.TransactionMetadata{
		Hash:        txHash,
		BlockNumber: blockNumber,
		Sender:      sender,
		GasUsed:     50000,
		Status:      1,
	}
	if err := pg.SaveTransaction(ctx, chainID, txMeta); err != nil {
		t.Fatalf("failed to save transaction: %v", err)
	}

	// 2 distinct Transfer events within the same transaction (log_index 0 and 1)
	transfer1 := &models.TokenTransfer{
		ChainID:        chainID,
		Token:          tokenAddr,
		FromAddress:    sender,
		ToAddress:      common.HexToAddress("0x9999999999999999999999999999999999999991"),
		Amount:         big.NewInt(100),
		TxHash:         txHash,
		BlockNumber:    blockNumber,
		BlockTimestamp: time.Now().UTC().Truncate(time.Second),
		LogIndex:       0,
	}

	transfer2 := &models.TokenTransfer{
		ChainID:        chainID,
		Token:          tokenAddr,
		FromAddress:    sender,
		ToAddress:      common.HexToAddress("0x9999999999999999999999999999999999999992"),
		Amount:         big.NewInt(200),
		TxHash:         txHash,
		BlockNumber:    blockNumber,
		BlockTimestamp: time.Now().UTC().Truncate(time.Second),
		LogIndex:       1,
	}

	if err := pg.SaveTokenTransfer(ctx, transfer1); err != nil {
		t.Fatalf("failed to save transfer 1: %v", err)
	}
	if err := pg.SaveTokenTransfer(ctx, transfer2); err != nil {
		t.Fatalf("failed to save transfer 2: %v", err)
	}

	var count int
	err := pg.pool.QueryRow(ctx, "SELECT count(*) FROM token_transfers WHERE chain_id = $1 AND tx_hash = $2", chainID, txHash.Hex()).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 distinct transfers for the same tx_hash distinguished by log_index, got %d", count)
	}
}

func TestPersistence_SyncCheckpoint(t *testing.T) {
	pg := getTestPostgres(t)
	defer pg.Close()

	ctx := context.Background()
	chainID := int64(11155111)
	streamID := "test_stream_erc20_checkpoint"

	// 1. Initial checkpoint: should be not found
	_, found, err := pg.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to query initial checkpoint: %v", err)
	}

	// 2. Save checkpoint at block 100
	if err := pg.SaveCheckpoint(ctx, chainID, streamID, 100, "0xhash100"); err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	// 3. Verify retrieved block is 100
	block, found, err := pg.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to get checkpoint: %v", err)
	}
	if !found || block != 100 {
		t.Fatalf("expected checkpoint block 100, got %d (found=%t)", block, found)
	}

	// 4. Advance checkpoint to block 150
	if err := pg.SaveCheckpoint(ctx, chainID, streamID, 150, "0xhash150"); err != nil {
		t.Fatalf("failed to advance checkpoint: %v", err)
	}

	block, found, err = pg.GetCheckpoint(ctx, chainID, streamID)
	if err != nil {
		t.Fatalf("failed to get advanced checkpoint: %v", err)
	}
	if !found || block != 150 {
		t.Fatalf("expected advanced checkpoint block 150, got %d", block)
	}
}
