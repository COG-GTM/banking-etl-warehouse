-- Source: sql_scripts/01_create_tables.sql, table FactTransaction.
-- Foreign keys to dim_account and dim_branch are added in 90_constraints.sql.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.fact_transaction (
  transaction_id   INT           NOT NULL COMMENT 'Transaction identifier (primary key). Source: FactTransaction.TransactionID INT PRIMARY KEY',
  account_id       INT                    COMMENT 'Account the transaction belongs to (FK to dim_account.account_id). Source: FactTransaction.AccountID INT',
  transaction_date TIMESTAMP              COMMENT 'Date and time of the transaction. Source: FactTransaction.TransactionDate DATETIME',
  amount           DECIMAL(19,4)          COMMENT 'Transaction amount. Source: FactTransaction.Amount MONEY',
  transaction_type VARCHAR(50)            COMMENT 'Transaction type (e.g. deposit, withdrawal). Source: FactTransaction.TransactionType VARCHAR(50)',
  branch_id        INT                    COMMENT 'Branch where the transaction happened (FK to dim_branch.branch_id). Source: FactTransaction.BranchID INT'
)
USING DELTA
COMMENT 'Transaction fact: one row per banking transaction, keyed to dim_account and dim_branch. Migrated from SQL Server DWH.dbo.FactTransaction (loaded by Talend job Load_FactTransaction).'
TBLPROPERTIES (
  'banking_etl.layer' = 'gold',
  'banking_etl.source_table' = 'DWH.dbo.FactTransaction'
);
