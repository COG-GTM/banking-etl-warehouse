-- DuckDB translation of sp_DailyTransaction (sql_scripts/02_create_procedures.sql).
-- The only change is that @start_date / @end_date become bind parameters.
SELECT
    CAST("TransactionDate" AS DATE) AS "Date",
    COUNT("TransactionID")          AS "TotalTransactions",
    SUM("Amount")                   AS "TotalAmount"
FROM "FactTransaction"
WHERE CAST("TransactionDate" AS DATE) BETWEEN CAST($start_date AS DATE) AND CAST($end_date AS DATE)
GROUP BY CAST("TransactionDate" AS DATE)
ORDER BY "Date";
