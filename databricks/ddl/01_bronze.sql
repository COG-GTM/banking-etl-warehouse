-- Bronze: raw landing zone, one table per (source system, entity).
-- Naming: bronze.<source>_<entity>. Every table carries the ingest metadata
-- columns `_ingest_ts` and `_source_file`.
--
-- File-sourced tables (csv_*, excel_*) are STRING-typed: the Talend readers
-- parsed text at read time (`dd-MM-yyyy HH:mm:ss` dates), so the raw layer keeps
-- the unparsed value and parsing happens in silver.
-- SQL Server-sourced tables keep the source column types from
-- Sample_DB_Connection (metadata/connections/Sample_DB_Connection_0.1.item).

CREATE TABLE IF NOT EXISTS banking.bronze.mssql_transaction_db (
  transaction_id   INT       COMMENT 'T-SQL: INT NOT NULL (PK of dbo.transaction_db)',
  account_id       INT       COMMENT 'T-SQL: INT',
  transaction_date TIMESTAMP COMMENT 'T-SQL: DATETIME2(19)',
  amount           INT       COMMENT 'T-SQL: INT (source stores whole rupiah)',
  transaction_type STRING    COMMENT 'T-SQL: VARCHAR(50)',
  branch_id        INT       COMMENT 'T-SQL: INT',
  _ingest_ts       TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file     STRING    COMMENT 'Ingest metadata: source identifier (sample.dbo.transaction_db)'
)
USING DELTA
COMMENT 'Raw dbo.transaction_db from the SQL Server `sample` database (Load_FactTransaction / tMSSqlInput tDBInput_1)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.csv_transaction (
  transaction_id   STRING    COMMENT 'Raw text from transaction_csv.csv',
  account_id       STRING    COMMENT 'Raw text from transaction_csv.csv',
  transaction_date STRING    COMMENT 'Raw text, Talend pattern "dd-MM-yyyy HH:mm:ss"',
  amount           STRING    COMMENT 'Raw text from transaction_csv.csv',
  transaction_type STRING    COMMENT 'Raw text from transaction_csv.csv',
  branch_id        STRING    COMMENT 'Raw text from transaction_csv.csv',
  _ingest_ts       TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file     STRING    COMMENT 'Ingest metadata: source file path'
)
USING DELTA
COMMENT 'Raw data_sources/transaction_csv.csv (Load_FactTransaction / tFileInputDelimited_1, header=1, sep=",")'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.excel_transaction (
  transaction_id   STRING    COMMENT 'Raw cell value from transaction_excel.xlsx',
  account_id       STRING    COMMENT 'Raw cell value from transaction_excel.xlsx',
  transaction_date STRING    COMMENT 'Raw cell value, Talend pattern "dd-MM-yyyy HH:mm:ss"',
  amount           STRING    COMMENT 'Raw cell value from transaction_excel.xlsx',
  transaction_type STRING    COMMENT 'Raw cell value from transaction_excel.xlsx',
  branch_id        STRING    COMMENT 'Raw cell value from transaction_excel.xlsx',
  _ingest_ts       TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file     STRING    COMMENT 'Ingest metadata: source file path'
)
USING DELTA
COMMENT 'Raw data_sources/transaction_excel.xlsx, Sheet1 (Load_FactTransaction / tFileInputExcel_1, header=1)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.mssql_account (
  account_id   INT       COMMENT 'T-SQL: INT NOT NULL (PK of dbo.account)',
  customer_id  INT       COMMENT 'T-SQL: INT',
  account_type STRING    COMMENT 'T-SQL: VARCHAR(10)',
  balance      INT       COMMENT 'T-SQL: INT',
  date_opened  TIMESTAMP COMMENT 'T-SQL: DATETIME2(19)',
  status       STRING    COMMENT 'T-SQL: VARCHAR(10)',
  _ingest_ts   TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file STRING    COMMENT 'Ingest metadata: source identifier (sample.dbo.account)'
)
USING DELTA
COMMENT 'Raw dbo.account from the SQL Server `sample` database (Load_DimAccount / tDBInput_1)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.mssql_branch (
  branch_id       INT       COMMENT 'T-SQL: INT NOT NULL (PK of dbo.branch)',
  branch_name     STRING    COMMENT 'T-SQL: VARCHAR(50)',
  branch_location STRING    COMMENT 'T-SQL: VARCHAR(50)',
  _ingest_ts      TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file    STRING    COMMENT 'Ingest metadata: source identifier (sample.dbo.branch)'
)
USING DELTA
COMMENT 'Raw dbo.branch from the SQL Server `sample` database (Load_DimBranch / tDBInput_1)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.mssql_customer (
  customer_id   INT    COMMENT 'T-SQL: INT NOT NULL (PK of dbo.customer)',
  customer_name STRING COMMENT 'T-SQL: VARCHAR(50)',
  address       STRING COMMENT 'T-SQL: VARCHAR(MAX)',
  city_id       INT    COMMENT 'T-SQL: INT',
  age           STRING COMMENT 'T-SQL: VARCHAR(3) - numeric-looking but typed as text in the source',
  gender        STRING COMMENT 'T-SQL: VARCHAR(10)',
  email         STRING COMMENT 'T-SQL: VARCHAR(50)',
  _ingest_ts    TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file  STRING    COMMENT 'Ingest metadata: source identifier (sample.dbo.customer)'
)
USING DELTA
COMMENT 'Raw dbo.customer from the SQL Server `sample` database (Load_DimCustomer / tDBInput_1)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.mssql_city (
  city_id      INT       COMMENT 'T-SQL: INT NOT NULL (PK of dbo.city)',
  city_name    STRING    COMMENT 'T-SQL: VARCHAR(50)',
  state_id     INT       COMMENT 'T-SQL: INT NOT NULL',
  _ingest_ts   TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file STRING    COMMENT 'Ingest metadata: source identifier (sample.dbo.city)'
)
USING DELTA
COMMENT 'Raw dbo.city from the SQL Server `sample` database (Load_DimCustomer lookup / tDBInput_2)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS banking.bronze.mssql_state (
  state_id     INT       COMMENT 'T-SQL: INT NOT NULL (PK of dbo.state)',
  state_name   STRING    COMMENT 'T-SQL: VARCHAR(50)',
  _ingest_ts   TIMESTAMP COMMENT 'Ingest metadata: load timestamp',
  _source_file STRING    COMMENT 'Ingest metadata: source identifier (sample.dbo.state)'
)
USING DELTA
COMMENT 'Raw dbo.state from the SQL Server `sample` database (Load_DimCustomer lookup / tDBInput_3)'
TBLPROPERTIES (delta.enableChangeDataFeed = true);
