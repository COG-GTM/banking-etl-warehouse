-- =====================================================================================
-- Delta Lake DDL for the banking DWH star schema (Unity Catalog)
-- Port of sql_scripts/01_create_tables.sql (SQL Server T-SQL) to Databricks SQL.
--
-- Catalog/schema creation is NOT done here -- see databricks/ddl/00_catalog_schemas.sql.
-- Tables are created in dependency order: silver dimensions first, then the gold fact,
-- because an informational FOREIGN KEY may only reference an existing table that already
-- declares the matching PRIMARY KEY.
--
-- -- Notes: semantic differences vs. the SQL Server DDL
-- 1. Constraint enforcement. SQL Server enforces PRIMARY KEY / FOREIGN KEY at write time
--    (and a PK also creates a clustered index). On Unity Catalog these constraints are
--    *informational metadata only*: they are declared NOT ENFORCED, no index is created,
--    and Databricks will happily write duplicate keys or orphan facts. Uniqueness,
--    de-duplication and referential integrity therefore have to be enforced by the ETL
--    (MERGE on the key + a reject/quarantine path for unmatched dimension keys) -- see
--    ticket 8. RELY is deliberately NOT specified, so the optimizer will not assume the
--    constraints hold. NOT NULL, by contrast, *is* enforced by Delta at write time, which
--    makes the key columns stricter here than a plain nullable T-SQL column.
-- 2. MONEY -> DECIMAL(19,4). T-SQL MONEY is a fixed 8-byte type with 4 decimal digits and
--    a range of +/-922,337,203,685,477.5807; DECIMAL(19,4) has the same scale and a very
--    slightly larger range, so all source values round-trip exactly. The behavioural
--    difference is in arithmetic: SQL Server truncates intermediate MONEY results to 4
--    decimals and MONEY/MONEY division has known precision quirks, whereas Spark widens
--    DECIMAL precision/scale for +,-,* and rounds HALF_UP on division, and returns NULL
--    (or errors under ANSI mode) on overflow instead of raising an arithmetic error.
--    Aggregations (SUM in sp_DailyTransaction / sp_BalancePerCustomer) can therefore differ
--    in the last digit from the SQL Server results; downstream analytics should CAST/ROUND
--    explicitly to 4 decimals.
-- 3. DATETIME -> TIMESTAMP. T-SQL DATETIME is timezone-naive with ~3.33 ms rounding
--    granularity. Spark TIMESTAMP is microsecond-precision and *instant* semantics: values
--    are stored as UTC and rendered in the session time zone (spark.sql.session.timeZone).
--    Ingestion must set the session time zone to the time zone the source data was recorded
--    in (UTC is the convention for this project) so that day-bucketing in the daily report
--    matches the legacy output. Use TIMESTAMP_NTZ instead only if a strictly naive value is
--    required; TIMESTAMP was chosen so that Databricks' date functions behave predictably.
-- 4. DATE maps 1:1. VARCHAR(n) -> STRING: Delta has no length limit, so the source-side
--    truncation/`String or binary data would be truncated` error disappears; the original
--    lengths are recorded in the column comments and length checks belong in the ETL.
-- 5. IF NOT EXISTS makes this script idempotent, but it also means column/comment/property
--    changes to an existing table are NOT applied by re-running it -- evolve those with
--    ALTER TABLE (or CREATE OR REPLACE for a full rebuild).
--
-- -- Column name mapping (T-SQL PascalCase -> Delta snake_case)
--   DimAccount            -> dwh.silver.dim_account
--     AccountID           -> account_id
--     CustomerID          -> customer_id
--     AccountType         -> account_type
--     Balance             -> balance
--     DateOpened          -> date_opened
--     Status              -> status
--   DimBranch             -> dwh.silver.dim_branch
--     BranchID            -> branch_id
--     BranchName          -> branch_name
--     BranchLocation      -> branch_location
--   DimCustomer           -> dwh.silver.dim_customer
--     CustomerID          -> customer_id
--     CustomerName        -> customer_name
--     Address             -> address
--     CityName            -> city_name
--     StateName           -> state_name
--     Age                 -> age
--     Gender              -> gender
--     Email               -> email
--   FactTransaction       -> dwh.gold.fact_transaction
--     TransactionID       -> transaction_id
--     AccountID           -> account_id
--     TransactionDate     -> transaction_date
--     Amount              -> amount
--     TransactionType     -> transaction_type
--     BranchID            -> branch_id
--
-- Table properties used everywhere, and why:
--   delta.enableChangeDataFeed = true       -- lets downstream/incremental jobs read row-level
--                                              changes (CDF) instead of re-scanning the table.
--   delta.autoOptimize.optimizeWrite = true -- compacts the many small files produced by the
--                                              batch MERGE loads into right-sized files.
--   delta.autoOptimize.autoCompact = true   -- background compaction after writes, keeping
--                                              file counts low without a manual OPTIMIZE job.
--   delta.columnMapping.mode = 'name'       -- allows renaming/dropping columns later without
--                                              rewriting the data (needed for schema evolution).
--   delta.minReaderVersion/minWriterVersion -- required protocol versions for column mapping.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- dwh.silver.dim_account  (was DWH.dbo.DimAccount)
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.silver.dim_account (
  account_id    INT            NOT NULL COMMENT 'Natural key of the account, from Sample_DB.account.account_id (T-SQL AccountID INT PRIMARY KEY). NOT NULL because it backs the informational PK.',
  customer_id   INT                     COMMENT 'Owning customer; joins to dwh.silver.dim_customer.customer_id (T-SQL CustomerID INT).',
  account_type  STRING                  COMMENT 'Account product type, e.g. Savings / Checking (T-SQL VARCHAR(50)).',
  balance       DECIMAL(19,4)           COMMENT 'Current account balance in the source currency (T-SQL MONEY, same 4-digit scale).',
  date_opened   DATE                    COMMENT 'Date the account was opened (T-SQL DATE).',
  status        STRING                  COMMENT 'Account status, e.g. Active / Inactive / Closed; sp_BalancePerCustomer filters on Active (T-SQL VARCHAR(50)).',
  CONSTRAINT dim_account_pk PRIMARY KEY (account_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Silver conformed account dimension. Informational PK only: uniqueness of account_id must be guaranteed by the ETL MERGE (ticket 8).'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.columnMapping.mode'         = 'name',
  'delta.minReaderVersion'           = '2',
  'delta.minWriterVersion'           = '5'
);

-- -------------------------------------------------------------------------------------
-- dwh.silver.dim_branch  (was DWH.dbo.DimBranch)
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.silver.dim_branch (
  branch_id       INT    NOT NULL COMMENT 'Natural key of the branch, from Sample_DB.branch.branch_id (T-SQL BranchID INT PRIMARY KEY). NOT NULL because it backs the informational PK.',
  branch_name     STRING          COMMENT 'Branch display name (T-SQL VARCHAR(100)).',
  branch_location STRING          COMMENT 'Branch address / location description (T-SQL VARCHAR(255)).',
  CONSTRAINT dim_branch_pk PRIMARY KEY (branch_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Silver conformed branch dimension. Informational PK only: uniqueness of branch_id must be guaranteed by the ETL MERGE (ticket 8).'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.columnMapping.mode'         = 'name',
  'delta.minReaderVersion'           = '2',
  'delta.minWriterVersion'           = '5'
);

-- -------------------------------------------------------------------------------------
-- dwh.silver.dim_customer  (was DWH.dbo.DimCustomer)
-- Denormalised: the Talend Load_DimCustomer job joins customer + city + state.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.silver.dim_customer (
  customer_id   INT    NOT NULL COMMENT 'Natural key of the customer, from Sample_DB.customer.customer_id (T-SQL CustomerID INT PRIMARY KEY). NOT NULL because it backs the informational PK.',
  customer_name STRING          COMMENT 'Customer full name; upper-cased by the legacy tMap cleansing step (T-SQL VARCHAR(100)).',
  address       STRING          COMMENT 'Street address of the customer (T-SQL VARCHAR(255)).',
  city_name     STRING          COMMENT 'City name, denormalised from the source city table (T-SQL VARCHAR(100)).',
  state_name    STRING          COMMENT 'State name, denormalised from the source state table (T-SQL VARCHAR(100)).',
  age           INT             COMMENT 'Customer age in years as recorded in the source (T-SQL INT).',
  gender        STRING          COMMENT 'Customer gender code (T-SQL VARCHAR(10)).',
  email         STRING          COMMENT 'Customer e-mail address (T-SQL VARCHAR(100)).',
  CONSTRAINT dim_customer_pk PRIMARY KEY (customer_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Silver conformed customer dimension, denormalised with city and state. Informational PK only: uniqueness of customer_id must be guaranteed by the ETL MERGE (ticket 8).'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.columnMapping.mode'         = 'name',
  'delta.minReaderVersion'           = '2',
  'delta.minWriterVersion'           = '5'
);

-- -------------------------------------------------------------------------------------
-- dwh.gold.fact_transaction  (was DWH.dbo.FactTransaction)
-- Union of the SQL Server, CSV and Excel transaction sources, de-duplicated on
-- transaction_id by the legacy tUniqRow step.
-- The two FOREIGN KEYs below are the ports of FK_FactTransaction_DimAccount and
-- FK_FactTransaction_DimBranch. They are metadata only (NOT ENFORCED): Databricks will
-- not reject a fact row whose account_id/branch_id is missing from the dimension, so the
-- ETL must perform the lookup and quarantine unmatched rows (ticket 8).
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.gold.fact_transaction (
  transaction_id   INT            NOT NULL COMMENT 'Natural key of the transaction (T-SQL TransactionID INT PRIMARY KEY); unique across the SQL/CSV/Excel sources after de-duplication. NOT NULL because it backs the informational PK.',
  account_id       INT                     COMMENT 'Account the transaction belongs to; informational FK to dwh.silver.dim_account.account_id (T-SQL AccountID INT).',
  transaction_date TIMESTAMP               COMMENT 'Instant the transaction occurred (T-SQL DATETIME); stored as UTC, rendered in spark.sql.session.timeZone.',
  amount           DECIMAL(19,4)           COMMENT 'Transaction amount in the source currency (T-SQL MONEY, same 4-digit scale); sign convention is carried by transaction_type, not by the value.',
  transaction_type STRING                  COMMENT 'Transaction category, e.g. Deposit / Withdrawal; drives the CASE WHEN balance logic of sp_BalancePerCustomer (T-SQL VARCHAR(50)).',
  branch_id        INT                     COMMENT 'Branch where the transaction was made; informational FK to dwh.silver.dim_branch.branch_id (T-SQL BranchID INT).',
  CONSTRAINT fact_transaction_pk PRIMARY KEY (transaction_id) NOT ENFORCED,
  CONSTRAINT fk_fact_transaction_dim_account FOREIGN KEY (account_id) REFERENCES dwh.silver.dim_account (account_id) NOT ENFORCED,
  CONSTRAINT fk_fact_transaction_dim_branch  FOREIGN KEY (branch_id)  REFERENCES dwh.silver.dim_branch  (branch_id)  NOT ENFORCED
)
USING DELTA
COMMENT 'Gold transaction fact table (star-schema centre). PK/FKs are informational and NOT ENFORCED; de-duplication and referential integrity are the ETL''s responsibility (ticket 8).'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.columnMapping.mode'         = 'name',
  'delta.minReaderVersion'           = '2',
  'delta.minWriterVersion'           = '5'
);
