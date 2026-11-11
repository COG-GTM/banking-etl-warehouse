-- Source: sql_scripts/01_create_tables.sql, table DimAccount.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.dim_account (
  account_id   INT           NOT NULL COMMENT 'Account identifier (primary key). Source: DimAccount.AccountID INT PRIMARY KEY',
  customer_id  INT                    COMMENT 'Owning customer, matches dim_customer.customer_id (no FK declared in source). Source: DimAccount.CustomerID INT',
  account_type VARCHAR(50)            COMMENT 'Account type. Source: DimAccount.AccountType VARCHAR(50)',
  balance      DECIMAL(19,4)          COMMENT 'Current account balance. Source: DimAccount.Balance MONEY',
  date_opened  DATE                   COMMENT 'Date the account was opened. Source: DimAccount.DateOpened DATE',
  status       VARCHAR(50)            COMMENT 'Account status (e.g. active). Source: DimAccount.Status VARCHAR(50)'
)
USING DELTA
COMMENT 'Account dimension: one row per bank account. Migrated from SQL Server DWH.dbo.DimAccount (loaded by Talend job Load_DimAccount).'
TBLPROPERTIES (
  'banking_etl.layer' = 'gold',
  'banking_etl.source_table' = 'DWH.dbo.DimAccount'
);
