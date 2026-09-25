-- 000005_escrow_events.up.sql
-- Creates the escrow_events table for WTFEscrow contract events.
-- Keyed by blockchain log identity: (chain_id, contract_address, tx_hash, log_index).
-- escrow_id is nullable because events like BudgetDeposited and BudgetWithdrawn do not carry an escrow ID.

CREATE TABLE IF NOT EXISTS escrow_events (
    id               BIGSERIAL PRIMARY KEY,
    chain_id         BIGINT        NOT NULL,
    contract_address VARCHAR(42)   NOT NULL,
    event_type       VARCHAR(64)   NOT NULL,
    tx_hash          VARCHAR(66)   NOT NULL,
    block_number     BIGINT        NOT NULL,
    block_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    log_index        INTEGER       NOT NULL,
    removed          BOOLEAN       NOT NULL DEFAULT FALSE,
    escrow_id        BIGINT,
    raw_data         JSONB         NOT NULL DEFAULT '{}',
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_escrow_event UNIQUE (chain_id, contract_address, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_escrow_events_chain_block
    ON escrow_events (chain_id, block_number);

CREATE INDEX IF NOT EXISTS idx_escrow_events_escrow_id
    ON escrow_events (escrow_id)
    WHERE escrow_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_escrow_events_event_type
    ON escrow_events (event_type);

CREATE INDEX IF NOT EXISTS idx_escrow_events_contract
    ON escrow_events (contract_address);
