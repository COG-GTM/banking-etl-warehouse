-- Informational PRIMARY KEY / FOREIGN KEY constraints (Unity Catalog only; not enforced).
-- Mirrors the PRIMARY KEY columns and FK_FactTransaction_DimAccount / FK_FactTransaction_DimBranch
-- from sql_scripts/01_create_tables.sql. Dropped and re-added so the script can be re-run.
ALTER TABLE ${catalog}.${schema}.fact_transaction DROP CONSTRAINT IF EXISTS fk_fact_transaction_dim_account;
ALTER TABLE ${catalog}.${schema}.fact_transaction DROP CONSTRAINT IF EXISTS fk_fact_transaction_dim_branch;

ALTER TABLE ${catalog}.${schema}.dim_account DROP PRIMARY KEY IF EXISTS CASCADE;
ALTER TABLE ${catalog}.${schema}.dim_branch DROP PRIMARY KEY IF EXISTS CASCADE;
ALTER TABLE ${catalog}.${schema}.dim_customer DROP PRIMARY KEY IF EXISTS CASCADE;
ALTER TABLE ${catalog}.${schema}.fact_transaction DROP PRIMARY KEY IF EXISTS CASCADE;

ALTER TABLE ${catalog}.${schema}.dim_account ADD CONSTRAINT pk_dim_account PRIMARY KEY (account_id);
ALTER TABLE ${catalog}.${schema}.dim_branch ADD CONSTRAINT pk_dim_branch PRIMARY KEY (branch_id);
ALTER TABLE ${catalog}.${schema}.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_id);
ALTER TABLE ${catalog}.${schema}.fact_transaction ADD CONSTRAINT pk_fact_transaction PRIMARY KEY (transaction_id);

ALTER TABLE ${catalog}.${schema}.fact_transaction ADD CONSTRAINT fk_fact_transaction_dim_account
  FOREIGN KEY (account_id) REFERENCES ${catalog}.${schema}.dim_account (account_id);
ALTER TABLE ${catalog}.${schema}.fact_transaction ADD CONSTRAINT fk_fact_transaction_dim_branch
  FOREIGN KEY (branch_id) REFERENCES ${catalog}.${schema}.dim_branch (branch_id);
