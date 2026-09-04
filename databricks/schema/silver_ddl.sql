-- Silver layer: cleansed, typed and conformed equivalents of the bronze tables.
--   * text columns trimmed / upper-cased (mirrors the Talend tMap string normalisation)
--   * the three transaction sources are unioned (tUnite) and de-duplicated on transaction_id (tUniqRow)
--     into a single `transaction` table; `_source_system` records which source won.
-- Keys are declared NOT NULL with informational primary keys.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.customer (
    customer_id INT NOT NULL,
    customer_name STRING,
    address STRING,
    city_id INT,
    age INT,
    gender STRING,
    email STRING,
    _ingest_ts TIMESTAMP,
    CONSTRAINT pk_silver_customer PRIMARY KEY (customer_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Cleansed customer records (trimmed, upper-cased text)';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.city (
    city_id INT NOT NULL,
    city_name STRING,
    state_id INT,
    _ingest_ts TIMESTAMP,
    CONSTRAINT pk_silver_city PRIMARY KEY (city_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Cleansed city records';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.state (
    state_id INT NOT NULL,
    state_name STRING,
    _ingest_ts TIMESTAMP,
    CONSTRAINT pk_silver_state PRIMARY KEY (state_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Cleansed state records';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.account (
    account_id INT NOT NULL,
    customer_id INT,
    account_type STRING,
    balance DECIMAL(19,4),
    date_opened DATE,
    status STRING,
    _ingest_ts TIMESTAMP,
    CONSTRAINT pk_silver_account PRIMARY KEY (account_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Cleansed account records';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.branch (
    branch_id INT NOT NULL,
    branch_name STRING,
    branch_location STRING,
    _ingest_ts TIMESTAMP,
    CONSTRAINT pk_silver_branch PRIMARY KEY (branch_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Cleansed branch records';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.transaction (
    transaction_id INT NOT NULL,
    account_id INT,
    transaction_date TIMESTAMP,
    amount DECIMAL(19,4),
    transaction_type STRING,
    branch_id INT,
    _source_system STRING COMMENT 'db | csv | excel',
    _ingest_ts TIMESTAMP,
    CONSTRAINT pk_silver_transaction PRIMARY KEY (transaction_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Unified, typed and de-duplicated transactions from SQL Server, CSV and Excel sources';
