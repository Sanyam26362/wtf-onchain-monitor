-- 000002_token_transfers_and_checkpoints.up.sql

-- 1. sync_checkpoints table for tracking indexer progress across streams
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    chain_id BIGINT NOT NULL,
    stream_id VARCHAR(100) NOT NULL,
    last_indexed_block BIGINT NOT NULL,
    last_block_hash VARCHAR(66),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT sync_checkpoints_pkey PRIMARY KEY (chain_id, stream_id)
);

-- 2. token_transfers table for normalized ERC-20 movement history
CREATE TABLE IF NOT EXISTS token_transfers (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    token VARCHAR(42) NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    log_index INTEGER NOT NULL,
    removed BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT uq_token_transfer UNIQUE (chain_id, token, tx_hash, log_index)
);

-- 3. Indexes for efficient queries by token, address, and block
CREATE INDEX IF NOT EXISTS idx_token_transfers_token ON token_transfers(token);
CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON token_transfers(from_address);
CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers(to_address);
CREATE INDEX IF NOT EXISTS idx_token_transfers_block ON token_transfers(block_number);
CREATE INDEX IF NOT EXISTS idx_token_transfers_chain_token ON token_transfers(chain_id, token);
