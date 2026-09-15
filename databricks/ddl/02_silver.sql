-- Silver: cleansed and conformed entities, one row per business key.
-- Types are already the target Delta types (the Talend tMap output types, with
-- MONEY/amount widened to DECIMAL(19,4) per the migration contract).

CREATE TABLE IF NOT EXISTS banking.silver.account (
  account_id   INT           NOT NULL COMMENT 'T-SQL: INT (sample.dbo.account.account_id)',
  customer_id  INT           COMMENT 'T-SQL: INT',
  account_type STRING        COMMENT 'T-SQL: VARCHAR(10) source / VARCHAR(50) in DWH.DimAccount',
  balance      DECIMAL(19,4) COMMENT 'T-SQL: INT source / MONEY in DWH.DimAccount',
  date_opened  DATE          COMMENT 'T-SQL: DATETIME2 source / DATE in DWH.DimAccount',
  status       STRING        COMMENT 'T-SQL: VARCHAR(10) source / VARCHAR(50) in DWH.DimAccount',
  _ingest_ts   TIMESTAMP     COMMENT 'Lineage: bronze ingest timestamp carried forward'
)
USING DELTA
COMMENT 'Cleansed accounts from bronze.mssql_account (Load_DimAccount passthrough tMap)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.silver.branch (
  branch_id       INT       NOT NULL COMMENT 'T-SQL: INT (sample.dbo.branch.branch_id)',
  branch_name     STRING    COMMENT 'T-SQL: VARCHAR(50) source / VARCHAR(100) in DWH.DimBranch',
  branch_location STRING    COMMENT 'T-SQL: VARCHAR(50) source / VARCHAR(255) in DWH.DimBranch',
  _ingest_ts      TIMESTAMP COMMENT 'Lineage: bronze ingest timestamp carried forward'
)
USING DELTA
COMMENT 'Cleansed branches from bronze.mssql_branch (Load_DimBranch passthrough tMap)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.silver.city (
  city_id      INT       NOT NULL COMMENT 'T-SQL: INT (sample.dbo.city.city_id)',
  city_name    STRING    COMMENT 'T-SQL: VARCHAR(50)',
  state_id     INT       COMMENT 'T-SQL: INT NOT NULL',
  _ingest_ts   TIMESTAMP COMMENT 'Lineage: bronze ingest timestamp carried forward'
)
USING DELTA
COMMENT 'Cleansed cities from bronze.mssql_city (tMap lookup row2 in Load_DimCustomer)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.silver.state (
  state_id     INT       NOT NULL COMMENT 'T-SQL: INT (sample.dbo.state.state_id)',
  state_name   STRING    COMMENT 'T-SQL: VARCHAR(50)',
  _ingest_ts   TIMESTAMP COMMENT 'Lineage: bronze ingest timestamp carried forward'
)
USING DELTA
COMMENT 'Cleansed states from bronze.mssql_state (tMap lookup row3 in Load_DimCustomer)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.silver.customer (
  customer_id   INT       NOT NULL COMMENT 'T-SQL: INT (sample.dbo.customer.customer_id)',
  customer_name STRING    COMMENT 'T-SQL: VARCHAR(50) source / VARCHAR(100) in DWH.DimCustomer; StringHandling.UPCASE applied',
  address       STRING    COMMENT 'T-SQL: VARCHAR(MAX) source / VARCHAR(255) in DWH.DimCustomer; StringHandling.UPCASE applied',
  city_id       INT       COMMENT 'T-SQL: INT; join key to silver.city, not carried into gold',
  city_name     STRING    COMMENT 'T-SQL: VARCHAR(50) from sample.dbo.city via tMap lookup',
  state_name    STRING    COMMENT 'T-SQL: VARCHAR(50) from sample.dbo.state via tMap lookup',
  age           INT       COMMENT 'T-SQL: VARCHAR(3) source / INT in DWH.DimCustomer; cast at this layer',
  gender        STRING    COMMENT 'T-SQL: VARCHAR(10); StringHandling.UPCASE applied',
  email         STRING    COMMENT 'T-SQL: VARCHAR(50) source / VARCHAR(100) in DWH.DimCustomer',
  _ingest_ts    TIMESTAMP COMMENT 'Lineage: bronze ingest timestamp carried forward'
)
USING DELTA
COMMENT 'Customers conformed with their city and state (Load_DimCustomer tMap: row1 left-outer row2 left-outer row3)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.silver.transaction (
  transaction_id   INT           NOT NULL COMMENT 'T-SQL: INT; dedup key (tUniqRow_1 key column)',
  account_id       INT           COMMENT 'T-SQL: INT',
  transaction_date TIMESTAMP     COMMENT 'T-SQL: DATETIME2 source / DATETIME in DWH.FactTransaction; files parsed with dd-MM-yyyy HH:mm:ss',
  amount           DECIMAL(19,4) COMMENT 'T-SQL: INT source / MONEY in DWH.FactTransaction',
  transaction_type STRING        COMMENT 'T-SQL: VARCHAR(50)',
  branch_id        INT           COMMENT 'T-SQL: INT',
  _source_system   STRING        COMMENT 'Lineage: which tUnite input the surviving row came from (mssql | csv | excel)',
  _ingest_ts       TIMESTAMP     COMMENT 'Lineage: bronze ingest timestamp carried forward'
)
USING DELTA
COMMENT 'Union of the three transaction sources, deduplicated on transaction_id (tUnite_1 -> tUniqRow_1)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);
