-- 000005_escrow_chain_events.down.sql

DROP INDEX IF EXISTS indexer.idx_chain_events_escrow_id;
DROP INDEX IF EXISTS indexer.uq_chain_events_tx_log;

ALTER TABLE indexer.chain_events 
    DROP COLUMN IF EXISTS escrow_id,
    DROP COLUMN IF EXISTS buyer,
    DROP COLUMN IF EXISTS seller,
    DROP COLUMN IF EXISTS amount,
    DROP COLUMN IF EXISTS alchemy_webhook_id,
    DROP COLUMN IF EXISTS raw_payload,
    DROP COLUMN IF EXISTS block_hash,
    DROP COLUMN IF EXISTS tx_index,
    DROP COLUMN IF EXISTS indexed_at;

ALTER TABLE indexer.chain_events 
    ADD CONSTRAINT fk_chain_event_transaction 
    FOREIGN KEY (chain_id, tx_hash) REFERENCES indexer.transactions(chain_id, tx_hash) ON DELETE CASCADE;
