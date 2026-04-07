-- =============================================================================
-- Databricks Validation Queries
-- Run these against the Databricks Gold layer to extract comparison data.
-- Adjust catalog/schema names to match your Databricks deployment.
-- =============================================================================

-- 1. Row Counts
-- -----------------------------------------------------------------------------
SELECT 'DimCustomer' AS TableName, COUNT(*) AS RowCount FROM gold.DimCustomer
UNION ALL
SELECT 'DimAccount', COUNT(*) FROM gold.DimAccount
UNION ALL
SELECT 'DimBranch', COUNT(*) FROM gold.DimBranch
UNION ALL
SELECT 'FactTransaction', COUNT(*) FROM gold.FactTransaction;

-- 2. Key Aggregates
-- -----------------------------------------------------------------------------

-- Total transaction amount
SELECT SUM(Amount) AS TotalTransactionAmount FROM gold.FactTransaction;

-- Transaction counts by type
SELECT TransactionType, COUNT(*) AS TransactionCount, SUM(Amount) AS TotalAmount
FROM gold.FactTransaction
GROUP BY TransactionType
ORDER BY TransactionType;

-- Distinct customer count
SELECT COUNT(DISTINCT CustomerID) AS DistinctCustomers FROM gold.DimCustomer;

-- Distinct account count
SELECT COUNT(DISTINCT AccountID) AS DistinctAccounts FROM gold.DimAccount;

-- Branch transaction distribution
SELECT BranchID, COUNT(*) AS TransactionCount
FROM gold.FactTransaction
GROUP BY BranchID
ORDER BY BranchID;

-- 3. Stored Procedure Equivalence: sp_DailyTransaction
-- -----------------------------------------------------------------------------
-- Databricks equivalent of sp_DailyTransaction
SELECT
    CAST(TransactionDate AS DATE) AS `Date`,
    COUNT(TransactionID) AS TotalTransactions,
    SUM(Amount) AS TotalAmount
FROM gold.FactTransaction
WHERE CAST(TransactionDate AS DATE) BETWEEN '2024-01-18' AND '2024-01-25'
GROUP BY CAST(TransactionDate AS DATE)
ORDER BY `Date`;

-- 4. Stored Procedure Equivalence: sp_BalancePerCustomer
-- -----------------------------------------------------------------------------
-- Databricks equivalent of sp_BalancePerCustomer
WITH TransactionSummary AS (
    SELECT
        AccountID,
        SUM(
            CASE
                WHEN TransactionType = 'Deposit' THEN Amount
                ELSE -Amount
            END
        ) AS TotalTransactionAmount
    FROM gold.FactTransaction
    GROUP BY AccountID
)
SELECT
    c.CustomerName,
    a.AccountType,
    a.Balance AS InitialBalance,
    a.Balance + COALESCE(ts.TotalTransactionAmount, 0) AS CurrentBalance
FROM gold.DimCustomer c
JOIN gold.DimAccount a ON c.CustomerID = a.CustomerID
LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
WHERE c.CustomerName LIKE '%John%'
    AND a.Status = 'active';

-- 5. Schema Information
-- -----------------------------------------------------------------------------
DESCRIBE TABLE gold.DimCustomer;
DESCRIBE TABLE gold.DimAccount;
DESCRIBE TABLE gold.DimBranch;
DESCRIBE TABLE gold.FactTransaction;
