-- DuckDB rendering of the target dbt DAG (staging -> intermediate -> marts).
-- One view per dbt model, named exactly like the model file it mirrors, with
-- the Jinja resolved: `{{ source('sample', 'x') }}` -> src_x,
-- `{{ ref('y') }}` -> y, `{{ var('z') }}` -> the $z bind parameter.
--
-- This is the parity harness' executable copy of the model logic. It exists so
-- the logic can be exercised without a Databricks workspace; dbt/models/ is
-- owned by the sibling tickets and is the source of truth for the deployed SQL.

---------------------------------------------------------------------------
-- staging: 1:1 with the sources, renamed to snake_case, typed, cleansed
---------------------------------------------------------------------------
CREATE OR REPLACE VIEW stg_sample__branch AS
SELECT
    CAST(branch_id AS INT)      AS branch_id,
    CAST(branch_name AS VARCHAR)     AS branch_name,
    CAST(branch_location AS VARCHAR) AS branch_location
FROM src_branch;

CREATE OR REPLACE VIEW stg_sample__account AS
SELECT
    CAST(account_id AS INT)          AS account_id,
    CAST(customer_id AS INT)         AS customer_id,
    CAST(account_type AS VARCHAR)    AS account_type,
    CAST(balance AS DECIMAL(18,2))   AS balance,
    CAST(date_opened AS DATE)        AS date_opened,
    CAST(status AS VARCHAR)          AS status
FROM src_account;

CREATE OR REPLACE VIEW stg_sample__customer AS
SELECT
    CAST(customer_id AS INT)      AS customer_id,
    CAST(customer_name AS VARCHAR) AS customer_name,
    CAST(address AS VARCHAR)      AS address,
    CAST(city_id AS INT)          AS city_id,
    CAST(age AS INT)              AS age,
    CAST(gender AS VARCHAR)       AS gender,
    CAST(email AS VARCHAR)        AS email
FROM src_customer;

CREATE OR REPLACE VIEW stg_sample__city AS
SELECT
    CAST(city_id AS INT)      AS city_id,
    CAST(city_name AS VARCHAR) AS city_name,
    CAST(state_id AS INT)     AS state_id
FROM src_city;

CREATE OR REPLACE VIEW stg_sample__state AS
SELECT
    CAST(state_id AS INT)      AS state_id,
    CAST(state_name AS VARCHAR) AS state_name
FROM src_state;

CREATE OR REPLACE VIEW stg_sample__transaction AS
SELECT
    CAST(transaction_id AS INT)            AS transaction_id,
    CAST(account_id AS INT)                AS account_id,
    CAST(transaction_date AS TIMESTAMP)    AS transaction_date,
    CAST(amount AS DECIMAL(18,2))          AS amount,
    CAST(transaction_type AS VARCHAR)      AS transaction_type,
    CAST(branch_id AS INT)                 AS branch_id
FROM src_transaction_db;

CREATE OR REPLACE VIEW stg_seed__transaction_excel AS
SELECT
    CAST(transaction_id AS INT)         AS transaction_id,
    CAST(account_id AS INT)             AS account_id,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date,
    CAST(amount AS DECIMAL(18,2))       AS amount,
    CAST(transaction_type AS VARCHAR)   AS transaction_type,
    CAST(branch_id AS INT)              AS branch_id
FROM src_transaction_excel;

CREATE OR REPLACE VIEW stg_seed__transaction_csv AS
SELECT
    CAST(transaction_id AS INT)         AS transaction_id,
    CAST(account_id AS INT)             AS account_id,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date,
    CAST(amount AS DECIMAL(18,2))       AS amount,
    CAST(transaction_type AS VARCHAR)   AS transaction_type,
    CAST(branch_id AS INT)              AS branch_id
FROM src_transaction_csv;

---------------------------------------------------------------------------
-- intermediate: tUnite + tUniqRow
---------------------------------------------------------------------------
CREATE OR REPLACE VIEW int_transactions_unioned AS
WITH unioned AS (
    SELECT *, 'sql_server' AS record_source, 1 AS source_priority FROM stg_sample__transaction
    UNION ALL
    SELECT *, 'excel'      AS record_source, 2 AS source_priority FROM stg_seed__transaction_excel
    UNION ALL
    SELECT *, 'csv'        AS record_source, 3 AS source_priority FROM stg_seed__transaction_csv
),
deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY transaction_id ORDER BY source_priority
    ) AS row_num
    FROM unioned
)
SELECT
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_type,
    branch_id,
    record_source
FROM deduplicated
WHERE row_num = 1;

CREATE OR REPLACE VIEW int_customer_enriched AS
SELECT
    c.customer_id,
    UPPER(c.customer_name) AS customer_name,
    UPPER(c.address)       AS address,
    UPPER(ci.city_name)    AS city_name,
    UPPER(s.state_name)    AS state_name,
    c.age,
    c.gender,
    c.email
FROM stg_sample__customer c
LEFT JOIN stg_sample__city  ci ON c.city_id  = ci.city_id
LEFT JOIN stg_sample__state s  ON ci.state_id = s.state_id;

---------------------------------------------------------------------------
-- marts
---------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_branch AS
SELECT branch_id, branch_name, branch_location FROM stg_sample__branch;

CREATE OR REPLACE VIEW dim_account AS
SELECT account_id, customer_id, account_type, balance, date_opened, status
FROM stg_sample__account;

CREATE OR REPLACE VIEW dim_customer AS
SELECT customer_id, customer_name, address, city_name, state_name, age, gender, email
FROM int_customer_enriched;

CREATE OR REPLACE VIEW fct_transaction AS
SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id
FROM int_transactions_unioned;

-- The two marts/reporting models take dbt vars, so they live in their own
-- files (05_*, 06_*): DuckDB views cannot carry bind parameters.
