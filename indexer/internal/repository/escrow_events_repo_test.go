package repository_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/repository"
)

func getTestEscrowPool(t *testing.T) (*persistence.Postgres, *repository.EscrowEventsRepository) {
	_ = godotenv.Load("../../.env")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://wtf_user:wtf_password@localhost:5433/wtf_indexer?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping live database test: failed to connect to postgres: %v", err)
	}

	migrationsDir := "../../migrations"
	if _, err := os.Stat(migrationsDir); err == nil {
		_ = pg.RunMigrations(ctx, migrationsDir)
	}

	repo := repository.NewEscrowEventsRepository(pg.Pool())
	return pg, repo
}

func TestEscrowEventsRepository_CRUD(t *testing.T) {
	pg, repo := getTestEscrowPool(t)
	defer pg.Close()

	ctx := context.Background()
	nonce := time.Now().UnixNano()
	txHash := common.HexToHash(fmt.Sprintf("0x%016x%016x%016x%016x", nonce, nonce, nonce, nonce))
	contractAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	escrowID := fmt.Sprintf("%d", nonce)
	amount := "1000000000"

	event := &models.EscrowEvent{
		ChainID:         11155111,
		ContractAddress: contractAddr,
		EventType:       "EscrowCreated",
		TxHash:          txHash,
		BlockNumber:     12000000,
		BlockTimestamp:  time.Now().UTC().Truncate(time.Second),
		LogIndex:        uint(nonce % 1000),
		Removed:         false,
		EscrowID:        &escrowID,
		Amount:          &amount,
		RawData: map[string]any{
			"amount": "1000000000",
			"buyer":  "0x2222222222222222222222222222222222222222",
			"seller": "0x3333333333333333333333333333333333333333",
		},
	}

	// 1. Insert event
	if err := repo.SaveEscrowEvent(ctx, event); err != nil {
		t.Fatalf("failed to save escrow event: %v", err)
	}
	if event.ID == 0 {
		t.Fatalf("expected non-zero ID after insert")
	}

	// 2. Query by escrow ID
	events, err := repo.GetEscrowEventsByEscrowID(ctx, escrowID)
	if err != nil {
		t.Fatalf("failed to get escrow events by escrow ID: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected at least 1 event for escrow ID %s", escrowID)
	}
	found := false
	for _, e := range events {
		if e.TxHash == txHash && e.LogIndex == event.LogIndex {
			found = true
			if e.EventType != "EscrowCreated" {
				t.Errorf("expected EventType 'EscrowCreated', got %s", e.EventType)
			}
			if e.RawData["amount"] != "1000000000" {
				t.Errorf("expected amount '1000000000', got %v", e.RawData["amount"])
			}
		}
	}
	if !found {
		t.Fatalf("saved event not found in query results")
	}

	// 3. Query by txHash and logIndex
	single, err := repo.GetEscrowEventByTxAndLogIndex(ctx, txHash.Hex(), int(event.LogIndex))
	if err != nil {
		t.Fatalf("failed to get escrow event by tx and log index: %v", err)
	}
	if single == nil {
		t.Fatalf("expected single event, got nil")
	}
	if single.ID != event.ID {
		t.Errorf("expected ID %d, got %d", event.ID, single.ID)
	}

	// 4. Test idempotency (ON CONFLICT DO UPDATE)
	event.RawData["amount"] = "2000000000"
	if err := repo.SaveEscrowEvent(ctx, event); err != nil {
		t.Fatalf("failed to re-save escrow event on conflict: %v", err)
	}
}
