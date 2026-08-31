-- Simulation of the four Talend jobs, i.e. the legacy SQL Server DWH as it is
-- populated today. Table and column names deliberately keep the legacy
-- PascalCase spelling of sql_scripts/01_create_tables.sql.
--
-- Input relations created by run_parity.py:
--   src_state, src_city, src_customer, src_account, src_branch,
--   src_transaction_db  (from the `sample` OLTP database)
--   src_transaction_csv, src_transaction_excel (from data_sources/)

-- Load_DimBranch: straight copy of branch.
CREATE OR REPLACE TABLE "DimBranch" AS
SELECT
    branch_id        AS "BranchID",
    branch_name      AS "BranchName",
    branch_location  AS "BranchLocation"
FROM src_branch;

-- Load_DimAccount: straight copy of account (balance widened to MONEY).
CREATE OR REPLACE TABLE "DimAccount" AS
SELECT
    account_id                  AS "AccountID",
    customer_id                 AS "CustomerID",
    account_type                AS "AccountType",
    CAST(balance AS DECIMAL(18,2)) AS "Balance",
    CAST(date_opened AS DATE)   AS "DateOpened",
    status                      AS "Status"
FROM src_account;

-- Load_DimCustomer: tMap join customer -> city -> state with uppercase cleansing.
CREATE OR REPLACE TABLE "DimCustomer" AS
SELECT
    c.customer_id             AS "CustomerID",
    UPPER(c.customer_name)    AS "CustomerName",
    UPPER(c.address)          AS "Address",
    UPPER(ci.city_name)       AS "CityName",
    UPPER(s.state_name)       AS "StateName",
    CAST(c.age AS INT)        AS "Age",
    c.gender                  AS "Gender",
    c.email                   AS "Email"
FROM src_customer c
LEFT JOIN src_city  ci ON c.city_id  = ci.city_id
LEFT JOIN src_state s  ON ci.state_id = s.state_id;

-- Load_FactTransaction: tUnite of the three transaction streams, then tUniqRow
-- on transaction_id (first row of the stream wins; stream order is the tUnite
-- input order: SQL Server, Excel, CSV).
CREATE OR REPLACE TABLE "FactTransaction" AS
WITH unioned AS (
    SELECT 1 AS _stream_order, * FROM src_transaction_db
    UNION ALL
    SELECT 2 AS _stream_order, * FROM src_transaction_excel
    UNION ALL
    SELECT 3 AS _stream_order, * FROM src_transaction_csv
),
deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY transaction_id ORDER BY _stream_order
    ) AS _rn
    FROM unioned
)
SELECT
    transaction_id                AS "TransactionID",
    account_id                    AS "AccountID",
    CAST(transaction_date AS TIMESTAMP) AS "TransactionDate",
    CAST(amount AS DECIMAL(18,2)) AS "Amount",
    transaction_type              AS "TransactionType",
    branch_id                     AS "BranchID"
FROM deduplicated
WHERE _rn = 1;
