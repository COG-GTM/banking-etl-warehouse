-- Databricks Unity Catalog SQL functions replacing the two T-SQL stored procedures
-- in sql_scripts/02_create_procedures.sql.
--
-- These table-valued functions are the natural SQL-native equivalent of the original
-- procedures: both procedures are single parameterized SELECTs with no side effects,
-- which is exactly what a UC SQL TVF models. They let BI tools call
--   SELECT * FROM dwh.analytics.daily_transaction(DATE'2024-01-01', DATE'2024-01-31')
-- without going through a notebook. The PySpark notebooks in this directory expose the
-- same logic for job/orchestration use and for unit testing.
--
-- A plain view is NOT a good fit here: both procedures are parameterized, and a view
-- would push the date range / name filter onto every caller, losing the encapsulation.
--
-- Semantic notes (see the notebooks for the full discussion):
--   * MONEY -> DECIMAL(19,4); aggregates are cast back to DECIMAL(19,4).
--   * ISNULL -> coalesce.
--   * BETWEEN bounds are inclusive, applied to the date-truncated timestamp.
--   * SQL Server's default collation is case-insensitive; Spark is case-sensitive, so the
--     name match and the 'Deposit'/'active' literals are compared with lower().
--   * The LIKE wildcards are added around the parameter value; callers that need literal
--     '%' or '_' should escape them (the PySpark helper does this automatically).

CREATE SCHEMA IF NOT EXISTS dwh.analytics;

-- sp_DailyTransaction
CREATE OR REPLACE FUNCTION dwh.analytics.daily_transaction(
    start_date DATE COMMENT 'Inclusive lower bound',
    end_date   DATE COMMENT 'Inclusive upper bound'
)
RETURNS TABLE (
    date               DATE,
    total_transactions BIGINT,
    total_amount       DECIMAL(19, 4)
)
COMMENT 'Daily transaction count and total amount over an inclusive date range (replaces sp_DailyTransaction).'
RETURN
    SELECT
        CAST(t.transaction_date AS DATE)      AS date,
        COUNT(t.transaction_id)               AS total_transactions,
        CAST(SUM(t.amount) AS DECIMAL(19, 4)) AS total_amount
    FROM dwh.gold.fact_transaction t
    WHERE CAST(t.transaction_date AS DATE)
          BETWEEN daily_transaction.start_date AND daily_transaction.end_date
    GROUP BY CAST(t.transaction_date AS DATE)
    ORDER BY date;

-- sp_BalancePerCustomer
CREATE OR REPLACE FUNCTION dwh.analytics.balance_per_customer(
    customer_name STRING COMMENT 'Case-insensitive substring of the customer name'
)
RETURNS TABLE (
    customer_name   STRING,
    account_type    STRING,
    initial_balance DECIMAL(19, 4),
    current_balance DECIMAL(19, 4)
)
COMMENT 'Initial and current balance of every active account of the matching customers (replaces sp_BalancePerCustomer).'
RETURN
    WITH transaction_summary AS (
        SELECT
            t.account_id,
            CAST(
                SUM(
                    CASE
                        WHEN lower(t.transaction_type) = 'deposit' THEN t.amount
                        ELSE -t.amount
                    END
                ) AS DECIMAL(19, 4)
            ) AS total_transaction_amount
        FROM dwh.gold.fact_transaction t
        GROUP BY t.account_id
    )
    SELECT
        c.customer_name                   AS customer_name,
        a.account_type                    AS account_type,
        CAST(a.balance AS DECIMAL(19, 4)) AS initial_balance,
        CAST(a.balance + coalesce(ts.total_transaction_amount, 0) AS DECIMAL(19, 4))
                                          AS current_balance
    FROM dwh.silver.dim_customer c
    JOIN dwh.silver.dim_account a
        ON c.customer_id = a.customer_id
    LEFT JOIN transaction_summary ts
        ON a.account_id = ts.account_id
    WHERE lower(c.customer_name) LIKE '%' || lower(balance_per_customer.customer_name) || '%'
      AND lower(a.status) = 'active';
