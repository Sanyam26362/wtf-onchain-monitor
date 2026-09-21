package persistence

import (
	"context"
	"fmt"
	"os"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultSchema is the dedicated PostgreSQL schema the indexer owns.
// It is kept separate from "public" so indexer-owned tables (employers,
// employees, ...) never collide with the application tables of the same
// name that other services write to in the shared Neon database.
const DefaultSchema = "indexer"

var schemaNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// SchemaName returns the schema the indexer should use, overridable via DB_SCHEMA.
func SchemaName() (string, error) {
	schema := os.Getenv("DB_SCHEMA")
	if schema == "" {
		schema = DefaultSchema
	}
	if !schemaNamePattern.MatchString(schema) {
		return "", fmt.Errorf("invalid DB_SCHEMA %q: must match [a-zA-Z_][a-zA-Z0-9_]*", schema)
	}
	return schema, nil
}

type Postgres struct {
	pool   *pgxpool.Pool
	schema string
}

func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is empty")
	}

	schema, err := SchemaName()
	if err != nil {
		return nil, err
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	searchPath := schema + ", public"

	// Ask for the search_path in the startup packet so it is part of the
	// connection identity, and re-assert it per connection because the Neon
	// pooler does not always forward startup parameters.
	cfg.ConnConfig.RuntimeParams["search_path"] = searchPath
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET search_path TO %s, public", pgx.Identifier{schema}.Sanitize()))
		return err
	}

	// The schema has to exist before any pooled connection sets search_path to
	// it, so create it over a plain single connection first.
	if err := ensureSchema(ctx, databaseURL, schema); err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return &Postgres{
		pool:   pool,
		schema: schema,
	}, nil
}

func ensureSchema(ctx context.Context, databaseURL, schema string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pgx.Identifier{schema}.Sanitize())
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("failed to create schema %q: %w", schema, err)
	}
	return nil
}

// Schema returns the schema this pool is bound to.
func (p *Postgres) Schema() string {
	if p == nil {
		return ""
	}
	return p.schema
}

func (p *Postgres) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}

func (p *Postgres) Pool() *pgxpool.Pool {
	if p == nil {
		return nil
	}
	return p.pool
}

func (p *Postgres) RunMigrations(ctx context.Context, migrationsDir string) error {
	return RunMigrations(ctx, p.pool, migrationsDir)
}
