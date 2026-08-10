-- ============================================================================
-- Delta Lake DDL for the banking data warehouse (TICKET-3)
--
-- Databricks SQL translation of sql_scripts/01_create_tables.sql (SQL Server).
-- The SQL Server database `DWH` becomes the Unity Catalog catalog `dwh`; the
-- star schema lives in the `gold` schema of that catalog.
--
-- Type mappings applied:
--   SQL Server MONEY       -> DECIMAL(19,4)   (MONEY is a 4-decimal fixed type)
--   SQL Server DATETIME    -> TIMESTAMP
--   SQL Server VARCHAR(n)  -> STRING          (Delta has no length-bound string)
--   SQL Server INT / DATE  -> INT / DATE      (unchanged)
--
-- Constraints:
--   Delta Lake does not enforce PRIMARY KEY or FOREIGN KEY constraints, so the
--   PK/FK definitions of the original DDL are dropped here. Their guarantees
--   are re-implemented in the pipelines instead:
--     * Uniqueness of the business key is enforced by deduplication and a
--       MERGE on that key during the load (the fact-load ticket dedups
--       TransactionID, replacing the Talend tUniqRow component).
--     * Referential integrity is enforced by data-quality checks (orphan
--       AccountID / BranchID counts) run after each gold load.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS dwh;

CREATE SCHEMA IF NOT EXISTS dwh.bronze
  COMMENT 'Raw, as-is landing zone for source systems (SQL Server sample DB, CSV, Excel)';

CREATE SCHEMA IF NOT EXISTS dwh.silver
  COMMENT 'Cleansed, conformed and deduplicated entities';

CREATE SCHEMA IF NOT EXISTS dwh.gold
  COMMENT 'Star schema serving analytics: DimAccount, DimBranch, DimCustomer, FactTransaction';

-- ----------------------------------------------------------------------------
-- Dimension: Account
-- Source DDL: AccountID INT PRIMARY KEY (not enforced in Delta; AccountID is
-- the business key deduplicated/merged on by the DimAccount load).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.gold.DimAccount (
    AccountID    INT            COMMENT 'Business key; uniqueness enforced by MERGE, not by a Delta constraint',
    CustomerID   INT            COMMENT 'References dwh.gold.DimCustomer.CustomerID (not enforced by Delta)',
    AccountType  STRING         COMMENT 'Source VARCHAR(50)',
    Balance      DECIMAL(19,4)  COMMENT 'Source MONEY',
    DateOpened   DATE,
    Status       STRING         COMMENT 'Source VARCHAR(50)'
)
USING DELTA
COMMENT 'Account dimension. Migrated from SQL Server DWH.DimAccount; PK on AccountID replaced by dedup + MERGE in the load.';

-- ----------------------------------------------------------------------------
-- Dimension: Branch
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.gold.DimBranch (
    BranchID       INT     COMMENT 'Business key; uniqueness enforced by MERGE, not by a Delta constraint',
    BranchName     STRING  COMMENT 'Source VARCHAR(100)',
    BranchLocation STRING  COMMENT 'Source VARCHAR(255)'
)
USING DELTA
COMMENT 'Branch dimension. Migrated from SQL Server DWH.DimBranch; PK on BranchID replaced by dedup + MERGE in the load.';

-- ----------------------------------------------------------------------------
-- Dimension: Customer (customer joined with city and state in the source ETL)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.gold.DimCustomer (
    CustomerID   INT     COMMENT 'Business key; uniqueness enforced by MERGE, not by a Delta constraint',
    CustomerName STRING  COMMENT 'Source VARCHAR(100)',
    Address      STRING  COMMENT 'Source VARCHAR(255)',
    CityName     STRING  COMMENT 'Source VARCHAR(100), denormalized from the city table',
    StateName    STRING  COMMENT 'Source VARCHAR(100), denormalized from the state table',
    Age          INT,
    Gender       STRING  COMMENT 'Source VARCHAR(10)',
    Email        STRING  COMMENT 'Source VARCHAR(100)'
)
USING DELTA
COMMENT 'Customer dimension. Migrated from SQL Server DWH.DimCustomer; PK on CustomerID replaced by dedup + MERGE in the load.';

-- ----------------------------------------------------------------------------
-- Fact: Transaction
-- Source DDL declared FK_FactTransaction_DimAccount and
-- FK_FactTransaction_DimBranch. Delta does not enforce foreign keys, so those
-- relationships are validated by data-quality checks (counting rows whose
-- AccountID / BranchID has no match in the corresponding dimension) after the
-- fact load. Duplicate TransactionID values coming from the three transaction
-- sources are removed by the dedup step of the fact-load ticket.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.gold.FactTransaction (
    TransactionID   INT            COMMENT 'Business key; dedup + MERGE replaces the source PRIMARY KEY',
    AccountID       INT            COMMENT 'References dwh.gold.DimAccount.AccountID (not enforced by Delta)',
    TransactionDate TIMESTAMP      COMMENT 'Source DATETIME',
    Amount          DECIMAL(19,4)  COMMENT 'Source MONEY',
    TransactionType STRING         COMMENT 'Source VARCHAR(50)',
    BranchID        INT            COMMENT 'References dwh.gold.DimBranch.BranchID (not enforced by Delta)'
)
USING DELTA
COMMENT 'Transaction fact. Migrated from SQL Server DWH.FactTransaction; PK/FK constraints replaced by dedup and data-quality checks.';
