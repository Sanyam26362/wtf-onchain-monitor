-- 000004_add_foreign_keys.up.sql
--
-- Composite foreign keys for the indexer-owned tables.
--
-- These previously failed with SQLSTATE 42703 ("column chain_id does not
-- exist") because the indexer shared the "public" schema with the
-- application, whose own employers/employees tables have a different shape.
-- CREATE TABLE IF NOT EXISTS silently skipped ours, so the FKs pointed at
-- the application's tables. The indexer now owns a dedicated schema, so the
-- constraints resolve against the correct tables.

ALTER TABLE employees
    ADD CONSTRAINT fk_employee_employer
    FOREIGN KEY (chain_id, employer) REFERENCES employers(chain_id, wallet);

ALTER TABLE chain_events
    ADD CONSTRAINT fk_chain_event_transaction
    FOREIGN KEY (chain_id, tx_hash) REFERENCES transactions(chain_id, tx_hash);

ALTER TABLE payroll_fundings
    ADD CONSTRAINT fk_payroll_funding_employer
    FOREIGN KEY (chain_id, employer) REFERENCES employers(chain_id, wallet);

ALTER TABLE payroll_fundings
    ADD CONSTRAINT fk_payroll_funding_employee
    FOREIGN KEY (chain_id, employee) REFERENCES employees(chain_id, wallet);

ALTER TABLE payroll_fundings
    ADD CONSTRAINT fk_payroll_funding_transaction
    FOREIGN KEY (chain_id, tx_hash) REFERENCES transactions(chain_id, tx_hash);

ALTER TABLE salary_claims
    ADD CONSTRAINT fk_salary_claim_employee
    FOREIGN KEY (chain_id, employee) REFERENCES employees(chain_id, wallet);
