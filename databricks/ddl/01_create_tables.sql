-- Delta Lake replacement for sql_scripts/01_create_tables.sql.
--
-- Type mapping: MONEY -> DECIMAL(19,4), DATETIME -> TIMESTAMP, VARCHAR(n) -> STRING.
-- Delta does not enforce PRIMARY KEY / FOREIGN KEY constraints. Keys are declared NOT NULL
-- here; informational PK/FK constraints for Unity Catalog live in
-- 02_informational_constraints_uc.sql. Referential integrity is enforced by
-- jobs/load_fact_transaction.py (orphan rows are rejected, as the SQL Server FKs did).
--
-- `dwh` is the default schema name. jobs/create_tables.py runs this file and rewrites
-- `dwh.` to the configured catalog/schema; to run it by hand, `USE CATALOG <catalog>` first.

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_account (
    AccountID   INT NOT NULL,
    CustomerID  INT,
    AccountType STRING,
    Balance     DECIMAL(19,4),
    DateOpened  DATE,
    Status      STRING
) USING DELTA
COMMENT 'Account dimension (was DWH.dbo.DimAccount)';

CREATE TABLE IF NOT EXISTS dwh.dim_branch (
    BranchID       INT NOT NULL,
    BranchName     STRING,
    BranchLocation STRING
) USING DELTA
COMMENT 'Branch dimension (was DWH.dbo.DimBranch)';

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    CustomerID   INT NOT NULL,
    CustomerName STRING,
    Address      STRING,
    CityName     STRING,
    StateName    STRING,
    Age          INT,
    Gender       STRING,
    Email        STRING
) USING DELTA
COMMENT 'Customer dimension denormalised from customer/city/state (was DWH.dbo.DimCustomer)';

CREATE TABLE IF NOT EXISTS dwh.fact_transaction (
    TransactionID   INT NOT NULL,
    AccountID       INT,
    TransactionDate TIMESTAMP,
    Amount          DECIMAL(19,4),
    TransactionType STRING,
    BranchID        INT
) USING DELTA
COMMENT 'Transaction fact. AccountID -> dim_account, BranchID -> dim_branch (not enforced by Delta)';
