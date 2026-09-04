-- Spark SQL equivalent of sp_BalancePerCustomer(@customer_name).
-- Uses the Databricks SQL named parameter marker :customer_name.
-- LOWER() on both sides reproduces SQL Server's default case-insensitive
-- collation for LIKE and for the Status = 'active' filter.
WITH TransactionSummary AS (
    SELECT
        AccountID,
        CAST(SUM(CASE
                     WHEN LOWER(TransactionType) = 'deposit' THEN CAST(Amount AS DECIMAL(19, 4))
                     ELSE -CAST(Amount AS DECIMAL(19, 4))
                 END) AS DECIMAL(19, 4)) AS TotalTransactionAmount
    FROM ${catalog}.${schema}.FactTransaction
    GROUP BY AccountID
)
SELECT
    c.CustomerName,
    a.AccountType,
    CAST(a.Balance AS DECIMAL(19, 4)) AS InitialBalance,
    CAST(CAST(a.Balance AS DECIMAL(19, 4)) + COALESCE(ts.TotalTransactionAmount, 0) AS DECIMAL(19, 4)) AS CurrentBalance
FROM ${catalog}.${schema}.DimCustomer c
JOIN ${catalog}.${schema}.DimAccount a
    ON c.CustomerID = a.CustomerID
LEFT JOIN TransactionSummary ts
    ON a.AccountID = ts.AccountID
WHERE LOWER(c.CustomerName) LIKE CONCAT('%', LOWER(:customer_name), '%')
  AND LOWER(a.Status) = 'active';
