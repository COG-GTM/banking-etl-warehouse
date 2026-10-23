-- Ticket 1: Unity Catalog namespace for the banking_dwh Lakehouse (prod target).
-- Equivalent to notebooks/00_setup/01_setup_unity_catalog.py with target=prod; the
-- notebook is the supported path because it resolves names from config/environments.yml
-- (dev maps the same layout onto migration_demo.bdwh_<layer>).

CREATE CATALOG IF NOT EXISTS banking_dwh
  COMMENT 'Banking DWH Lakehouse migrated from SQL Server DWH + Talend (COG-GTM/banking-etl-warehouse).';

CREATE SCHEMA IF NOT EXISTS banking_dwh.bronze COMMENT 'Raw, source-shaped copies of the SQL Server sample DB and the transaction CSV/Excel files.';
CREATE SCHEMA IF NOT EXISTS banking_dwh.silver COMMENT 'Conformed star schema: dim_branch, dim_account, dim_customer, fact_transaction.';
CREATE SCHEMA IF NOT EXISTS banking_dwh.gold   COMMENT 'Analytics layer replacing sp_DailyTransaction and sp_BalancePerCustomer.';
CREATE SCHEMA IF NOT EXISTS banking_dwh.ops    COMMENT 'Data-quality results, reconciliation, run audit, streaming checkpoints.';

-- Raw landing: csv/transaction/, excel/transaction/, sqlserver/<table>/
CREATE VOLUME IF NOT EXISTS banking_dwh.bronze.landing COMMENT 'Raw landing zone for CSV, Excel and SQL Server extracts.';
CREATE VOLUME IF NOT EXISTS banking_dwh.ops.checkpoints COMMENT 'Auto Loader / Structured Streaming checkpoints and schemas.';

-- Read access for analysts (account-level principals only).
GRANT USE CATALOG ON CATALOG banking_dwh TO `account users`;
GRANT USE SCHEMA, SELECT ON SCHEMA banking_dwh.silver TO `account users`;
GRANT USE SCHEMA, SELECT ON SCHEMA banking_dwh.gold TO `account users`;
