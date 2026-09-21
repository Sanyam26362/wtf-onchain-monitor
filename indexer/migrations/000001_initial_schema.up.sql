-- 000001_initial_schema.up.sql

CREATE TABLE IF NOT EXISTS transactions (
    tx_hash VARCHAR(66) NOT NULL,
    chain_id BIGINT NOT NULL,
    block_number BIGINT,
    sender VARCHAR(42) NOT NULL,
    recipient VARCHAR(42),
    gas_used BIGINT,
    status VARCHAR(20) DEFAULT 'pending' NOT NULL,
    confirmations BIGINT DEFAULT 0 NOT NULL,
    error_data JSONB,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    finalized_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT transactions_pkey PRIMARY KEY (chain_id, tx_hash),
    CONSTRAINT chk_transaction_status CHECK (status IN ('pending', 'confirmed', 'finalized', 'failed', 'reorged'))
);

CREATE TABLE IF NOT EXISTS employers (
    wallet VARCHAR(42) NOT NULL,
    funds NUMERIC(78, 0) DEFAULT 0 NOT NULL,
    total_salary_per_second NUMERIC(78, 0) DEFAULT 0 NOT NULL,
    active BOOLEAN DEFAULT TRUE NOT NULL,
    deactivation_time BIGINT,
    added_at TIMESTAMP WITH TIME ZONE,
    removed_at TIMESTAMP WITH TIME ZONE,
    latest_tx_hash VARCHAR(66),
    chain_id BIGINT NOT NULL,
    CONSTRAINT employers_pkey PRIMARY KEY (chain_id, wallet)
);

CREATE TABLE IF NOT EXISTS employees (
    wallet VARCHAR(42) NOT NULL,
    employer VARCHAR(42) NOT NULL,
    salary_per_second NUMERIC(78, 0) DEFAULT 0 NOT NULL,
    last_withdraw BIGINT,
    active BOOLEAN DEFAULT TRUE NOT NULL,
    total_leaves NUMERIC(78, 0) DEFAULT 0 NOT NULL,
    deactivation_time BIGINT,
    allocation NUMERIC(78, 0),
    added_at TIMESTAMP WITH TIME ZONE,
    removed_at TIMESTAMP WITH TIME ZONE,
    latest_tx_hash VARCHAR(66),
    chain_id BIGINT NOT NULL,
    CONSTRAINT employees_pkey PRIMARY KEY (chain_id, wallet)
);

CREATE TABLE IF NOT EXISTS chain_events (
    event_id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    contract_address VARCHAR(42) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    log_index INTEGER NOT NULL,
    removed BOOLEAN DEFAULT FALSE NOT NULL,
    raw_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT uq_chain_event UNIQUE (chain_id, contract_address, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS payroll_fundings (
    id BIGSERIAL PRIMARY KEY,
    employer VARCHAR(42) NOT NULL,
    employee VARCHAR(42) NOT NULL,
    amount_paid NUMERIC(78, 0) NOT NULL,
    fee NUMERIC(78, 0) NOT NULL,
    amount_credited NUMERIC(78, 0) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    log_index INTEGER NOT NULL,
    chain_id BIGINT NOT NULL,
    CONSTRAINT uq_payroll_funding_event UNIQUE (chain_id, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS salary_claims (
    id BIGSERIAL PRIMARY KEY,
    employee VARCHAR(42) NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    log_index INTEGER NOT NULL,
    chain_id BIGINT NOT NULL,
    CONSTRAINT uq_salary_claim_event UNIQUE (chain_id, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS reconciliation_exceptions (
    id BIGSERIAL PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    entity_ref VARCHAR(255) NOT NULL,
    expected JSONB NOT NULL,
    observed JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'open' NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    resolved_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT chk_reconciliation_severity CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    CONSTRAINT chk_reconciliation_status CHECK (status IN ('open', 'resolved'))
);
