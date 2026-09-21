package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/persistence"
)

func main() {
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env not loaded: %v", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL not set")
	}

	ctx := context.Background()

	// Share the indexer's connection setup so search_path points at the
	// indexer-owned schema rather than public.
	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pg.Close()

	pool := pg.Pool()
	fmt.Printf("✓ Connected to Neon PostgreSQL (schema: %s)\n", pg.Schema())

	// Delete old payroll checkpoint
	result, err := pool.Exec(ctx, "DELETE FROM sync_checkpoints WHERE stream_id = 'monthly_payroll'")
	if err != nil {
		log.Fatalf("failed to delete payroll checkpoint: %v", err)
	}
	fmt.Printf("✓ Deleted %d payroll checkpoint(s)\n", result.RowsAffected())

	// Delete old token checkpoint
	result, err = pool.Exec(ctx, "DELETE FROM sync_checkpoints WHERE stream_id LIKE 'erc20_transfers_%'")
	if err != nil {
		log.Fatalf("failed to delete token checkpoint: %v", err)
	}
	fmt.Printf("✓ Deleted %d token checkpoint(s)\n", result.RowsAffected())

	// Show remaining checkpoints
	rows, err := pool.Query(ctx, "SELECT stream_id, last_indexed_block, last_block_hash FROM sync_checkpoints")
	if err != nil {
		log.Fatalf("failed to query checkpoints: %v", err)
	}
	defer rows.Close()

	fmt.Println("\n📋 Remaining checkpoints:")
	count := 0
	for rows.Next() {
		var streamID string
		var lastBlock int64
		var lastHash *string
		if err := rows.Scan(&streamID, &lastBlock, &lastHash); err != nil {
			log.Printf("error scanning row: %v", err)
			continue
		}
		hashStr := "NULL"
		if lastHash != nil {
			hashStr = *lastHash
		}
		fmt.Printf("  - %s: block %d (hash: %s)\n", streamID, lastBlock, hashStr)
		count++
	}
	if count == 0 {
		fmt.Println("  (none)")
	}

	fmt.Println("\n✅ Checkpoints cleared! Restart the indexer to begin fresh from START_BLOCK (payroll) and TOKEN_START_BLOCK (token)")
}
