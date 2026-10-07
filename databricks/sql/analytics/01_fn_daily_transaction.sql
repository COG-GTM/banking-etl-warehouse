-- gold.fn_daily_transaction: port of DWH.dbo.sp_DailyTransaction (sql_scripts/02_create_procedures.sql).
-- Usage: SELECT * FROM migration_demo.banking_mig_gold.fn_daily_transaction(DATE'2024-01-18', DATE'2024-01-20');
-- ${...} placeholders are substituted by banking_etl.analytics.functions (${create} is
-- "CREATE OR REPLACE FUNCTION" on Databricks, "CREATE OR REPLACE TEMPORARY FUNCTION" in local tests).
-- Parity: inclusive BETWEEN on the calendar date; a NULL bound or start > end returns no rows (as in T-SQL).
-- COUNT is INT and SUM(amount) DECIMAL(19,4), the T-SQL COUNT / SUM(MONEY) result types.
${create} ${fn_daily_transaction}(
  start_date DATE COMMENT 'First calendar day, inclusive (T-SQL @start_date DATE).',
  end_date DATE COMMENT 'Last calendar day, inclusive (T-SQL @end_date DATE).'
)
RETURNS TABLE (
  `date` DATE COMMENT 'CAST(transaction_date AS DATE) (T-SQL [Date]).',
  total_transactions INT COMMENT 'COUNT(transaction_id) for the day (T-SQL TotalTransactions).',
  total_amount DECIMAL(19,4) COMMENT 'SUM(amount) for the day, MONEY precision (T-SQL TotalAmount).'
)
COMMENT 'Daily transaction count and total amount between two dates (inclusive). Port of sp_DailyTransaction.'
RETURN
  SELECT
    CAST(transaction_date AS DATE) AS `date`,
    CAST(COUNT(transaction_id) AS INT) AS total_transactions,
    CAST(SUM(amount) AS DECIMAL(19,4)) AS total_amount
  FROM ${fact_transaction}
  WHERE CAST(transaction_date AS DATE) BETWEEN ${fn_daily_transaction_short}.start_date AND ${fn_daily_transaction_short}.end_date
  GROUP BY CAST(transaction_date AS DATE)
  ORDER BY `date`;
