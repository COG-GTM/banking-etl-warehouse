-- dbt model marts/reporting/rpt_daily_transaction.sql, rendered for DuckDB.
-- `{{ var('start_date') }}` / `{{ var('end_date') }}` become bind parameters.
SELECT
    CAST(transaction_date AS DATE) AS transaction_day,
    COUNT(transaction_id)          AS total_transactions,
    SUM(amount)                    AS total_amount
FROM fct_transaction
WHERE CAST(transaction_date AS DATE)
      BETWEEN CAST($start_date AS DATE) AND CAST($end_date AS DATE)
GROUP BY CAST(transaction_date AS DATE)
ORDER BY transaction_day;
