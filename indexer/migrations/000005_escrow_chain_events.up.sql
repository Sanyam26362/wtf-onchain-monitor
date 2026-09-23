-- 000005_escrow_chain_events.up.sql

-- 1. Drop foreign key constraint to allow standalone event ingestion from webhooks
ALTER TABLE indexer.chain_events DROP CONSTRAINT IF EXISTS fk_chain_event_transaction;

-- 2. Add EscrowSettled fields & metadata
ALTER TABLE indexer.chain_events 
    ADD COLUMN IF NOT EXISTS escrow_id VARCHAR(66),
    ADD COLUMN IF NOT EXISTS buyer VARCHAR(42),
    ADD COLUMN IF NOT EXISTS seller VARCHAR(42),
    ADD COLUMN IF NOT EXISTS amount NUMERIC(36, 18),
    ADD COLUMN IF NOT EXISTS alchemy_webhook_id TEXT,
    ADD COLUMN IF NOT EXISTS raw_payload TEXT,
    ADD COLUMN IF NOT EXISTS block_hash VARCHAR(66),
    ADD COLUMN IF NOT EXISTS tx_index INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS indexed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();

ALTER TABLE indexer.chain_events ALTER COLUMN raw_data DROP NOT NULL;

-- 3. Ensure index on escrow_id for rapid UI/worker lookups
CREATE INDEX IF NOT EXISTS idx_chain_events_escrow_id ON indexer.chain_events (escrow_id);

-- 4. Deduplicate any existing records before creating unique index
DELETE FROM indexer.chain_events a USING indexer.chain_events b
WHERE a.event_id < b.event_id
  AND a.tx_hash = b.tx_hash
  AND a.log_index = b.log_index;

-- 5. Ensure unique constraint on tx_hash + log_index for deduplication idempotency
CREATE UNIQUE INDEX IF NOT EXISTS uq_chain_events_tx_log ON indexer.chain_events (tx_hash, log_index);

