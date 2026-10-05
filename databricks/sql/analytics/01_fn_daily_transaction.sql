-- gold.fn_daily_transaction: port of DWH.dbo.sp_DailyTransaction (sql_scripts/02_create_procedures.sql).
-- Unity Catalog SQL table-valued function: SELECT * FROM <catalog>.<gold>.fn_daily_transaction(DATE'2024-01-18', DATE'2024-01-20').
-- Placeholders are substituted by banking_etl.analytics.functions from banking_etl.config.Settings
-- (${create} is "CREATE OR REPLACE FUNCTION" on Databricks, "CREATE OR REPLACE TEMPORARY FUNCTION" locally).
-- Parity: inclusive BETWEEN on the calendar date; a NULL bound or start > end returns no rows (as in T-SQL).
-- COUNT is returned as INT and SUM(Amount) as DECIMAL(19,4), the T-SQL COUNT / SUM(MONEY) result types
-- (ANSI mode raises on overflow, like SQL Server's arithmetic overflow error).
${create} ${fn_daily_transaction}(
  start_date DATE COMMENT 'First calendar day, inclusive (T-SQL @start_date DATE).',
  end_date DATE COMMENT 'Last calendar day, inclusive (T-SQL @end_date DATE).'
)
RETURNS TABLE (
  `Date` DATE COMMENT 'CAST(TransactionDate AS DATE).',
  TotalTransactions INT COMMENT 'COUNT(TransactionID) for the day.',
  TotalAmount DECIMAL(19,4) COMMENT 'SUM(Amount) for the day (MONEY precision).'
)
COMMENT 'Daily transaction count and total amount between two dates (inclusive). Port of sp_DailyTransaction.'
RETURN
  SELECT
    CAST(TransactionDate AS DATE) AS `Date`,
    CAST(COUNT(TransactionID) AS INT) AS TotalTransactions,
    CAST(SUM(Amount) AS DECIMAL(19,4)) AS TotalAmount
  FROM ${fact_transaction}
  WHERE CAST(TransactionDate AS DATE) BETWEEN fn_daily_transaction.start_date AND fn_daily_transaction.end_date
  GROUP BY CAST(TransactionDate AS DATE)
  ORDER BY `Date`;
