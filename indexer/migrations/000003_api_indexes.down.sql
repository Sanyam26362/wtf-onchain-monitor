-- 000003_api_indexes.down.sql
DROP INDEX IF EXISTS idx_token_transfers_chain_token_from;
DROP INDEX IF EXISTS idx_token_transfers_chain_token_to;
DROP INDEX IF EXISTS idx_payroll_fundings_chain_employer;
DROP INDEX IF EXISTS idx_payroll_fundings_chain_employee;
DROP INDEX IF EXISTS idx_salary_claims_chain_employee;
DROP INDEX IF EXISTS idx_chain_events_chain_tx;
DROP INDEX IF EXISTS idx_reconciliation_exceptions_status;
