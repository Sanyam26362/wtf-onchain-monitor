-- 000006_align_escrow_schema.down.sql
-- Reverses changes introduced in 000006_align_escrow_schema.up.sql.

DROP INDEX IF EXISTS indexer.uq_escrow_events_tx_log;

ALTER TABLE indexer.escrow_events 
    DROP COLUMN IF EXISTS amount;

ALTER TABLE indexer.escrow_events 
    ALTER COLUMN escrow_id TYPE BIGINT;
