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
	_ = godotenv.Load()
	ctx := context.Background()

	pg, err := persistence.NewPostgres(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	migrationsDir := "./migrations"
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		migrationsDir = "../../migrations"
	}
	if _, err := os.Stat(migrationsDir); err == nil {
		if err := pg.RunMigrations(ctx, migrationsDir); err != nil {
			log.Fatalf("failed to run migrations: %v", err)
		}
	}

	pool := pg.Pool()
	schema := pg.Schema()

	fmt.Printf("=== tables in schema %q ===\n", schema)
	rows, err := pool.Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = $1 ORDER BY table_name`, schema)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var t string
		_ = rows.Scan(&t)
		fmt.Println("  ", t)
	}
	rows.Close()

	fmt.Printf("\n=== foreign keys in schema %q ===\n", schema)
	fkRows, err := pool.Query(ctx, `
		SELECT con.conname, rel.relname, confrel.relname
		FROM pg_constraint con
		JOIN pg_class rel ON rel.oid = con.conrelid
		JOIN pg_class confrel ON confrel.oid = con.confrelid
		JOIN pg_namespace ns ON ns.oid = con.connamespace
		WHERE con.contype = 'f' AND ns.nspname = $1
		ORDER BY con.conname`, schema)
	if err != nil {
		log.Fatal(err)
	}
	defer fkRows.Close()
	n := 0
	for fkRows.Next() {
		var name, child, parent string
		_ = fkRows.Scan(&name, &child, &parent)
		fmt.Printf("   %-35s %s -> %s\n", name, child, parent)
		n++
	}
	fmt.Printf("   (%d foreign keys)\n", n)
}
