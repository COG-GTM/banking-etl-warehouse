-- Unity Catalog informational constraints for the gold star schema.
-- Databricks never enforces these; they exist for lineage, BI tools and the
-- optimizer. They live in their own file because they are Unity Catalog only:
-- open-source Delta rejects PRIMARY KEY / FOREIGN KEY clauses, so the runner
-- skips this file with --local.
--
-- DROP ... IF EXISTS before ADD keeps the script idempotent.

ALTER TABLE banking.gold.dim_account DROP CONSTRAINT IF EXISTS pk_dim_account;
ALTER TABLE banking.gold.dim_account ADD CONSTRAINT pk_dim_account PRIMARY KEY (account_id);

ALTER TABLE banking.gold.dim_branch DROP CONSTRAINT IF EXISTS pk_dim_branch;
ALTER TABLE banking.gold.dim_branch ADD CONSTRAINT pk_dim_branch PRIMARY KEY (branch_id);

ALTER TABLE banking.gold.dim_customer DROP CONSTRAINT IF EXISTS pk_dim_customer;
ALTER TABLE banking.gold.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_id);

ALTER TABLE banking.gold.fact_transaction DROP CONSTRAINT IF EXISTS pk_fact_transaction;
ALTER TABLE banking.gold.fact_transaction ADD CONSTRAINT pk_fact_transaction PRIMARY KEY (transaction_id);

-- Replaces FK_FactTransaction_DimAccount (enforced in SQL Server, informational here).
ALTER TABLE banking.gold.fact_transaction DROP CONSTRAINT IF EXISTS fk_fact_transaction_dim_account;
ALTER TABLE banking.gold.fact_transaction
  ADD CONSTRAINT fk_fact_transaction_dim_account
  FOREIGN KEY (account_id) REFERENCES banking.gold.dim_account (account_id) NOT ENFORCED;

-- Replaces FK_FactTransaction_DimBranch.
ALTER TABLE banking.gold.fact_transaction DROP CONSTRAINT IF EXISTS fk_fact_transaction_dim_branch;
ALTER TABLE banking.gold.fact_transaction
  ADD CONSTRAINT fk_fact_transaction_dim_branch
  FOREIGN KEY (branch_id) REFERENCES banking.gold.dim_branch (branch_id) NOT ENFORCED;

-- Not present in the legacy schema: DimCustomer is reachable only through
-- DimAccount.CustomerID, so the relationship is declared here for BI lineage.
ALTER TABLE banking.gold.dim_account DROP CONSTRAINT IF EXISTS fk_dim_account_dim_customer;
ALTER TABLE banking.gold.dim_account
  ADD CONSTRAINT fk_dim_account_dim_customer
  FOREIGN KEY (customer_id) REFERENCES banking.gold.dim_customer (customer_id) NOT ENFORCED;
