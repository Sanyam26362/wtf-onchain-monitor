-- 000003_api_indexes.up.sql
-- Optimized composite indexes for API queries

CREATE INDEX IF NOT EXISTS idx_token_transfers_chain_token_from ON token_transfers(chain_id, token, from_address);
CREATE INDEX IF NOT EXISTS idx_token_transfers_chain_token_to ON token_transfers(chain_id, token, to_address);
CREATE INDEX IF NOT EXISTS idx_payroll_fundings_chain_employer ON payroll_fundings(chain_id, employer);
CREATE INDEX IF NOT EXISTS idx_payroll_fundings_chain_employee ON payroll_fundings(chain_id, employee);
CREATE INDEX IF NOT EXISTS idx_salary_claims_chain_employee ON salary_claims(chain_id, employee);
CREATE INDEX IF NOT EXISTS idx_chain_events_chain_tx ON chain_events(chain_id, tx_hash);
CREATE INDEX IF NOT EXISTS idx_reconciliation_exceptions_status ON reconciliation_exceptions(status);
