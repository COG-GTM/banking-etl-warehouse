-- Unity Catalog SQL port of sql_scripts/02_create_procedures.sql.
-- Databricks has no stored procedures, so each T-SQL procedure becomes a SQL
-- table function (parameterized, callable from SQL warehouses and notebooks).
--
-- Run with: DATABRICKS_CATALOG defaulting to `banking`; substitute the catalog
-- name if the deployment uses a different one.

USE CATALOG banking;
USE SCHEMA gold;

-- sp_DailyTransaction @start_date DATE, @end_date DATE
CREATE OR REPLACE FUNCTION gold.daily_transaction(start_date DATE, end_date DATE)
RETURNS TABLE (
  date DATE,
  total_transactions BIGINT,
  total_amount DECIMAL(19, 4)
)
COMMENT 'Daily transaction count and total amount between start_date and end_date (inclusive).'
RETURN
  SELECT
    CAST(f.transaction_date AS DATE) AS date,
    COUNT(f.transaction_id)          AS total_transactions,
    CAST(SUM(f.amount) AS DECIMAL(19, 4)) AS total_amount
  FROM gold.fact_transaction f
  WHERE CAST(f.transaction_date AS DATE)
        BETWEEN daily_transaction.start_date AND daily_transaction.end_date
  GROUP BY CAST(f.transaction_date AS DATE)
  ORDER BY date;

-- sp_BalancePerCustomer @customer_name VARCHAR(100)
CREATE OR REPLACE FUNCTION gold.balance_per_customer(customer_name STRING)
RETURNS TABLE (
  customer_name STRING,
  account_type STRING,
  initial_balance DECIMAL(19, 4),
  current_balance DECIMAL(19, 4)
)
COMMENT 'Current balance of every active account of customers matching %customer_name%.'
RETURN
  WITH transaction_summary AS (
    SELECT
      account_id,
      CAST(SUM(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE -amount END)
           AS DECIMAL(19, 4)) AS total_transaction_amount
    FROM gold.fact_transaction
    GROUP BY account_id
  )
  SELECT
    c.customer_name,
    a.account_type,
    CAST(a.balance AS DECIMAL(19, 4)) AS initial_balance,
    CAST(a.balance + coalesce(ts.total_transaction_amount, 0) AS DECIMAL(19, 4)) AS current_balance
  FROM gold.dim_customer c
  JOIN gold.dim_account a
    ON c.customer_id = a.customer_id
  LEFT JOIN transaction_summary ts
    ON a.account_id = ts.account_id
  WHERE c.customer_name LIKE '%' || balance_per_customer.customer_name || '%'
    AND a.status = 'active';

-- Convenience views used by the orchestration validation task.
CREATE OR REPLACE VIEW gold.v_daily_transaction
COMMENT 'Unparameterized daily aggregation over the whole fact table.'
AS
SELECT
  CAST(transaction_date AS DATE) AS date,
  COUNT(transaction_id)          AS total_transactions,
  CAST(SUM(amount) AS DECIMAL(19, 4)) AS total_amount
FROM gold.fact_transaction
GROUP BY CAST(transaction_date AS DATE);

CREATE OR REPLACE VIEW gold.v_fact_transaction_orphan_summary
COMMENT 'Referential-integrity quarantine summary (Delta FKs are NOT ENFORCED).'
AS
SELECT
  _orphan_reason,
  COUNT(*) AS orphan_rows
FROM gold.fact_transaction_orphan
GROUP BY _orphan_reason;
