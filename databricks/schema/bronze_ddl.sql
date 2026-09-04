-- Bronze layer: raw landing copies of the source systems used by the Talend jobs.
--   * SQL Server source database `sample` (restored from data_sources/sample.bak):
--       customer, city, state, account, branch, transaction (landed as transaction_db)
--   * data_sources/transaction_csv.csv   -> transaction_csv   (all columns kept as STRING, raw file values)
--   * data_sources/transaction_excel.xlsx -> transaction_excel (typed as read by the spreadsheet reader)
-- Every bronze table carries `_ingest_ts` (load timestamp) and `_source_file` (origin file / table) metadata.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.customer (
    customer_id INT,
    customer_name STRING,
    address STRING,
    city_id INT,
    age INT,
    gender STRING,
    email STRING,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw copy of sample.dbo.customer';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.city (
    city_id INT,
    city_name STRING,
    state_id INT,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw copy of sample.dbo.city';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.state (
    state_id INT,
    state_name STRING,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw copy of sample.dbo.state';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.account (
    account_id INT,
    customer_id INT,
    account_type STRING,
    balance DECIMAL(19,4),
    date_opened DATE,
    status STRING,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw copy of sample.dbo.account';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.branch (
    branch_id INT,
    branch_name STRING,
    branch_location STRING,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw copy of sample.dbo.branch';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.transaction_db (
    transaction_id INT,
    account_id INT,
    transaction_date TIMESTAMP,
    amount DECIMAL(19,4),
    transaction_type STRING,
    branch_id INT,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw copy of sample.dbo.transaction (SQL Server transaction source)';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.transaction_csv (
    transaction_id STRING,
    account_id STRING,
    transaction_date STRING COMMENT 'Raw text in dd-MM-yyyy HH:mm:ss format',
    amount STRING,
    transaction_type STRING,
    branch_id STRING,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw landing of data_sources/transaction_csv.csv (headers: transaction_id,account_id,transaction_date,amount,transaction_type,branch_id)';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.transaction_excel (
    transaction_id INT,
    account_id INT,
    transaction_date TIMESTAMP,
    amount DECIMAL(19,4),
    transaction_type STRING,
    branch_id INT,
    _ingest_ts TIMESTAMP,
    _source_file STRING
)
USING DELTA
COMMENT 'Raw landing of data_sources/transaction_excel.xlsx (headers: transaction_id,account_id,transaction_date,amount,transaction_type,branch_id)';
