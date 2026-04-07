-- =============================================================================
-- Legacy SQL Server Validation Queries
-- Run these against the legacy DWH database to extract comparison data.
-- =============================================================================

USE DWH;
GO

-- 1. Row Counts
-- -----------------------------------------------------------------------------
SELECT 'DimCustomer' AS TableName, COUNT(*) AS RowCount FROM DimCustomer
UNION ALL
SELECT 'DimAccount', COUNT(*) FROM DimAccount
UNION ALL
SELECT 'DimBranch', COUNT(*) FROM DimBranch
UNION ALL
SELECT 'FactTransaction', COUNT(*) FROM FactTransaction;
GO

-- 2. Key Aggregates
-- -----------------------------------------------------------------------------

-- Total transaction amount
SELECT SUM(Amount) AS TotalTransactionAmount FROM FactTransaction;
GO

-- Transaction counts by type
SELECT TransactionType, COUNT(*) AS TransactionCount, SUM(Amount) AS TotalAmount
FROM FactTransaction
GROUP BY TransactionType
ORDER BY TransactionType;
GO

-- Distinct customer count
SELECT COUNT(DISTINCT CustomerID) AS DistinctCustomers FROM DimCustomer;
GO

-- Distinct account count
SELECT COUNT(DISTINCT AccountID) AS DistinctAccounts FROM DimAccount;
GO

-- Branch transaction distribution
SELECT BranchID, COUNT(*) AS TransactionCount
FROM FactTransaction
GROUP BY BranchID
ORDER BY BranchID;
GO

-- 3. Stored Procedure Outputs
-- -----------------------------------------------------------------------------

-- sp_DailyTransaction: Sample date range covering CSV data
EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-25';
GO

-- sp_BalancePerCustomer: Sample customers
-- Adjust customer names to match actual data in your DWH
EXEC sp_BalancePerCustomer @customer_name = 'John';
GO

-- 4. Schema Information
-- -----------------------------------------------------------------------------
SELECT
    t.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.IS_NULLABLE
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_NAME = c.TABLE_NAME
WHERE t.TABLE_SCHEMA = 'dbo'
    AND t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION;
GO
