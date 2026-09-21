-- 000004_add_foreign_keys.down.sql

ALTER TABLE salary_claims DROP CONSTRAINT IF EXISTS fk_salary_claim_employee;
ALTER TABLE payroll_fundings DROP CONSTRAINT IF EXISTS fk_payroll_funding_transaction;
ALTER TABLE payroll_fundings DROP CONSTRAINT IF EXISTS fk_payroll_funding_employee;
ALTER TABLE payroll_fundings DROP CONSTRAINT IF EXISTS fk_payroll_funding_employer;
ALTER TABLE chain_events DROP CONSTRAINT IF EXISTS fk_chain_event_transaction;
ALTER TABLE employees DROP CONSTRAINT IF EXISTS fk_employee_employer;
