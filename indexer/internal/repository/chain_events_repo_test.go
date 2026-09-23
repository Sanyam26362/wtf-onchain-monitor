package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
)

func getTestPool(t *testing.T) (*persistence.Postgres, *ChainEventsRepository) {
	_ = godotenv.Load("../../.env")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://wtf_user:wtf_password@localhost:5433/wtf_indexer?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping database test: failed to connect to postgres: %v", err)
	}

	// Apply migrations to ensure schema is up to date
	migrationsDir := "../../migrations"
	if _, err := os.Stat(migrationsDir); err == nil {
		_ = pg.RunMigrations(ctx, migrationsDir)
	}

	repo := NewChainEventsRepository(pg.Pool())
	return pg, repo
}

func TestChainEventsRepository_EscrowSettled_StandaloneInsertAndIdempotency(t *testing.T) {
	pg, repo := getTestPool(t)
	defer pg.Close()

	ctx := context.Background()
	testNonce := time.Now().UnixNano()
	txHash := fmt.Sprintf("0x%064x", testNonce)
	escrowID := fmt.Sprintf("0x%064x", testNonce+1)
	contractAddr := "0x1111111111111111111111111111111111111111"
	buyer := "0x2222222222222222222222222222222222222222"
	seller := "0x3333333333333333333333333333333333333333"

	event := &models.ChainEvent{
		ChainID:          11155111,
		ContractAddress:  contractAddr,
		EventName:        "EscrowSettled",
		TxHash:           txHash,
		BlockNumber:      12000000,
		BlockHash:        fmt.Sprintf("0x%064x", testNonce+2),
		LogIndex:         0,
		TxIndex:          1,
		BlockTimestamp:   time.Now().Unix(),
		EscrowID:         escrowID,
		Buyer:            buyer,
		Seller:           seller,
		Amount:           "500.000000000000000000",
		AlchemyWebhookID: "wh_test_123456",
		RawPayload:       `{"event":"EscrowSettled","amount":500}`,
		IndexedAt:        time.Now().UTC(),
	}

	// 1. Test standalone insert without preceding transaction in indexer.transactions
	err := repo.InsertChainEvent(ctx, event)
	if err != nil {
		t.Fatalf("failed to insert standalone EscrowSettled event: %v", err)
	}
	if event.EventID == 0 {
		t.Fatalf("expected non-zero EventID after insert, got 0")
	}

	// 2. Test duplicate insert with same (tx_hash, log_index) for idempotency
	dupEvent := &models.ChainEvent{
		ChainID:          11155111,
		ContractAddress:  contractAddr,
		EventName:        "EscrowSettled",
		TxHash:           txHash,
		BlockNumber:      12000000,
		LogIndex:         0,
		EscrowID:         escrowID,
		Buyer:            buyer,
		Seller:           seller,
		Amount:           "500.000000000000000000",
		AlchemyWebhookID: "wh_test_123456_duplicate",
	}
	err = repo.InsertChainEvent(ctx, dupEvent)
	if err != nil {
		t.Fatalf("expected duplicate insert to be ignored without error, got: %v", err)
	}

	// 3. Test GetEventsByEscrowID
	events, err := repo.GetEventsByEscrowID(ctx, escrowID)
	if err != nil {
		t.Fatalf("failed to get events by escrow_id: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 event for escrow_id %s, got %d", escrowID, len(events))
	}
	retrieved := events[0]
	if retrieved.TxHash != txHash {
		t.Errorf("expected TxHash %s, got %s", txHash, retrieved.TxHash)
	}
	if retrieved.Buyer != buyer {
		t.Errorf("expected Buyer %s, got %s", buyer, retrieved.Buyer)
	}
	if retrieved.Seller != seller {
		t.Errorf("expected Seller %s, got %s", seller, retrieved.Seller)
	}
	if retrieved.EventName != "EscrowSettled" {
		t.Errorf("expected EventName EscrowSettled, got %s", retrieved.EventName)
	}
	if retrieved.AlchemyWebhookID != "wh_test_123456" {
		t.Errorf("expected AlchemyWebhookID wh_test_123456, got %s", retrieved.AlchemyWebhookID)
	}

	// 4. Test GetEventByTxAndLogIndex
	byTuple, err := repo.GetEventByTxAndLogIndex(ctx, txHash, 0)
	if err != nil {
		t.Fatalf("failed to get event by tx and log index: %v", err)
	}
	if byTuple == nil {
		t.Fatalf("expected event by tuple to be found, got nil")
	}
	if byTuple.EventID != event.EventID {
		t.Errorf("expected EventID %d, got %d", event.EventID, byTuple.EventID)
	}

	// Verify non-existent tuple returns nil, nil
	notFound, err := repo.GetEventByTxAndLogIndex(ctx, "0x0000000000000000000000000000000000000000000000000000000000000000", 999)
	if err != nil {
		t.Fatalf("unexpected error for non-existent event: %v", err)
	}
	if notFound != nil {
		t.Errorf("expected nil for non-existent event, got %+v", notFound)
	}
}
