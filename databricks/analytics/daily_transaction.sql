-- Spark SQL equivalent of sp_DailyTransaction(@start_date, @end_date).
-- Uses Databricks SQL named parameter markers (:start_date, :end_date).
-- Example: spark.sql(open("daily_transaction.sql").read(),
--                    args={"start_date": "2024-01-01", "end_date": "2024-01-31"})
SELECT
    CAST(TransactionDate AS DATE)          AS Date,
    COUNT(TransactionID)                   AS TotalTransactions,
    CAST(SUM(CAST(Amount AS DECIMAL(19, 4))) AS DECIMAL(19, 4)) AS TotalAmount
FROM ${catalog}.${schema}.FactTransaction
WHERE CAST(TransactionDate AS DATE) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
GROUP BY CAST(TransactionDate AS DATE)
ORDER BY Date;
