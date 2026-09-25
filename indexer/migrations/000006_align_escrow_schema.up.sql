-- 000006_align_escrow_schema.up.sql
-- Aligns escrow_events schema for 256-bit EVM values and idempotency constraint.

ALTER TABLE indexer.escrow_events 
    ALTER COLUMN escrow_id TYPE NUMERIC(78, 0);

ALTER TABLE indexer.escrow_events 
    ADD COLUMN IF NOT EXISTS amount NUMERIC(78, 0);

CREATE UNIQUE INDEX IF NOT EXISTS uq_escrow_events_tx_log 
    ON indexer.escrow_events (tx_hash, log_index);
