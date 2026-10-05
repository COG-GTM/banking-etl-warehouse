-- ops.fact_transaction_rejects: fact rows that failed referential integrity (or other fact checks).
-- Populated by ticket 8. Columns mirror gold.fact_transaction (all nullable) plus reject metadata.
-- Placeholders are substituted by banking_etl.gold.ddl from banking_etl.config.Settings.
CREATE TABLE IF NOT EXISTS ${fact_transaction_rejects} (
  TransactionID INT COMMENT 'Natural key of the rejected transaction.',
  AccountID INT COMMENT 'Account natural key as received.',
  TransactionDate TIMESTAMP COMMENT 'Transaction timestamp as received.',
  Amount DECIMAL(19,4) COMMENT 'Transaction amount as received.',
  TransactionType STRING COMMENT 'Transaction type as received.',
  BranchID INT COMMENT 'Branch natural key as received.',
  AccountKey BIGINT COMMENT 'dim_account surrogate key if the lookup matched, else NULL.',
  BranchKey BIGINT COMMENT 'dim_branch surrogate key if the lookup matched, else NULL.',
  reject_reason STRING NOT NULL COMMENT 'Why the row was rejected, e.g. MISSING_DIM_ACCOUNT, MISSING_DIM_BRANCH.',
  rejected_at TIMESTAMP NOT NULL COMMENT 'When the load rejected the row.'
)
USING DELTA
COMMENT 'Rejected gold.fact_transaction rows (replaces the SQL Server FK violations / Talend tMSSqlOutput reject flow).'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
