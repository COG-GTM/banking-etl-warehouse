-- gold.fact_transaction: port of DWH.dbo.FactTransaction (sql_scripts/01_create_tables.sql).
-- Full refresh by ticket 8 (Load_FactTransaction). Rows whose AccountID/BranchID have no dimension
-- match are written to ops.fact_transaction_rejects instead (the legacy FKs would have rejected them).
-- Placeholders are substituted by banking_etl.gold.ddl from banking_etl.config.Settings.
-- Blocks between /*uc:begin*/ and /*uc:end*/ are Unity Catalog-only and are stripped for OSS Delta.
CREATE TABLE IF NOT EXISTS ${fact_transaction} (
  TransactionID INT NOT NULL COMMENT 'Natural key; transaction_id deduplicated across the SQL Server, CSV and Excel sources (legacy PRIMARY KEY).',
  AccountID INT COMMENT 'Account natural key (legacy FK_FactTransaction_DimAccount -> DimAccount.AccountID).',
  TransactionDate TIMESTAMP COMMENT 'Transaction timestamp (legacy DATETIME -> TIMESTAMP).',
  Amount DECIMAL(19,4) COMMENT 'Transaction amount (legacy MONEY -> DECIMAL(19,4)).',
  TransactionType STRING COMMENT 'Deposit / Withdrawal / Transfer / Payment (legacy VARCHAR(50)).',
  BranchID INT COMMENT 'Branch natural key (legacy FK_FactTransaction_DimBranch -> DimBranch.BranchID).',
  AccountKey BIGINT COMMENT 'Surrogate key looked up from dim_account on AccountID.',
  BranchKey BIGINT COMMENT 'Surrogate key looked up from dim_branch on BranchID.'
  /*uc:begin*/,
  CONSTRAINT pk_fact_transaction PRIMARY KEY (TransactionID),
  CONSTRAINT fk_fact_transaction_dim_account FOREIGN KEY (AccountID) REFERENCES ${dim_account} (AccountID),
  CONSTRAINT fk_fact_transaction_dim_branch FOREIGN KEY (BranchID) REFERENCES ${dim_branch} (BranchID)/*uc:end*/
)
USING DELTA
COMMENT 'Transaction fact (full refresh). Port of DWH.dbo.FactTransaction; legacy PascalCase columns kept for reconciliation parity.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
