-- Gold: the star schema, a one-for-one replacement of the DWH database created
-- by sql_scripts/01_create_tables.sql. Column order matches that T-SQL DDL.
-- Primary keys and foreign keys are declared in 04_constraints.sql.

CREATE TABLE IF NOT EXISTS banking.gold.dim_account (
  account_id   INT           NOT NULL COMMENT 'T-SQL: INT PRIMARY KEY (DWH.DimAccount.AccountID)',
  customer_id  INT           COMMENT 'T-SQL: INT (DimAccount.CustomerID)',
  account_type STRING        COMMENT 'T-SQL: VARCHAR(50) (DimAccount.AccountType)',
  balance      DECIMAL(19,4) COMMENT 'T-SQL: MONEY (DimAccount.Balance)',
  date_opened  DATE          COMMENT 'T-SQL: DATE (DimAccount.DateOpened)',
  status       STRING        COMMENT 'T-SQL: VARCHAR(50) (DimAccount.Status)'
)
USING DELTA
COMMENT 'Account dimension; replaces DWH.dbo.DimAccount'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.gold.dim_branch (
  branch_id       INT    NOT NULL COMMENT 'T-SQL: INT PRIMARY KEY (DWH.DimBranch.BranchID)',
  branch_name     STRING COMMENT 'T-SQL: VARCHAR(100) (DimBranch.BranchName)',
  branch_location STRING COMMENT 'T-SQL: VARCHAR(255) (DimBranch.BranchLocation)'
)
USING DELTA
COMMENT 'Branch dimension; replaces DWH.dbo.DimBranch'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.gold.dim_customer (
  customer_id   INT    NOT NULL COMMENT 'T-SQL: INT PRIMARY KEY (DWH.DimCustomer.CustomerID)',
  customer_name STRING COMMENT 'T-SQL: VARCHAR(100) (DimCustomer.CustomerName); uppercased',
  address       STRING COMMENT 'T-SQL: VARCHAR(255) (DimCustomer.Address); uppercased',
  city_name     STRING COMMENT 'T-SQL: VARCHAR(100) (DimCustomer.CityName)',
  state_name    STRING COMMENT 'T-SQL: VARCHAR(100) (DimCustomer.StateName)',
  age           INT    COMMENT 'T-SQL: INT (DimCustomer.Age); source column is VARCHAR(3)',
  gender        STRING COMMENT 'T-SQL: VARCHAR(10) (DimCustomer.Gender); uppercased',
  email         STRING COMMENT 'T-SQL: VARCHAR(100) (DimCustomer.Email)'
)
USING DELTA
COMMENT 'Customer dimension denormalised with city and state; replaces DWH.dbo.DimCustomer'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.gold.fact_transaction (
  transaction_id   INT           NOT NULL COMMENT 'T-SQL: INT PRIMARY KEY (DWH.FactTransaction.TransactionID)',
  account_id       INT           COMMENT 'T-SQL: INT FK -> DimAccount.AccountID',
  transaction_date TIMESTAMP     COMMENT 'T-SQL: DATETIME (FactTransaction.TransactionDate)',
  amount           DECIMAL(19,4) COMMENT 'T-SQL: MONEY (FactTransaction.Amount)',
  transaction_type STRING        COMMENT 'T-SQL: VARCHAR(50) (FactTransaction.TransactionType)',
  branch_id        INT           COMMENT 'T-SQL: INT FK -> DimBranch.BranchID'
)
USING DELTA
COMMENT 'Transaction fact, deduplicated on transaction_id; replaces DWH.dbo.FactTransaction'
TBLPROPERTIES (delta.enableChangeDataFeed = true);
