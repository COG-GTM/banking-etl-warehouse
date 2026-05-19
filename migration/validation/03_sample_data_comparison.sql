/************************************************************************************
-- SAMPLE DATA COMPARISON SCRIPT
-- Purpose: Spot-check specific records between source and target to verify
--          individual field-level accuracy after migration.
-- Usage:   Run against the SQL Server instance hosting both Sample_DB and DWH.
-- Expected: All comparisons should show matching values.
************************************************************************************/

USE DWH;
GO

PRINT '=== SAMPLE DATA COMPARISON REPORT ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- Sample 1: DimAccount - Compare first 10 records
PRINT '--- DimAccount Sample (Top 10) ---';
SELECT
    s.AccountID,
    s.CustomerID AS Src_CustomerID,
    t.CustomerID AS Tgt_CustomerID,
    s.AccountType AS Src_AccountType,
    t.AccountType AS Tgt_AccountType,
    s.Balance AS Src_Balance,
    t.Balance AS Tgt_Balance,
    CASE
        WHEN s.CustomerID = t.CustomerID
         AND s.AccountType = t.AccountType
         AND s.Balance = t.Balance
        THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS Status
FROM Sample_DB.dbo.Account s
INNER JOIN DWH.dbo.DimAccount t ON s.AccountID = t.AccountID
ORDER BY s.AccountID
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- Sample 2: DimBranch - Compare all records (typically small table)
PRINT '--- DimBranch Sample (All) ---';
SELECT
    s.BranchID,
    s.BranchName AS Src_BranchName,
    t.BranchName AS Tgt_BranchName,
    s.BranchLocation AS Src_BranchLocation,
    t.BranchLocation AS Tgt_BranchLocation,
    CASE
        WHEN s.BranchName = t.BranchName
         AND s.BranchLocation = t.BranchLocation
        THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS Status
FROM Sample_DB.dbo.Branch s
INNER JOIN DWH.dbo.DimBranch t ON s.BranchID = t.BranchID
ORDER BY s.BranchID;

-- Sample 3: DimCustomer - Compare with UPPER transformation applied
PRINT '--- DimCustomer Sample (Top 10) ---';
SELECT
    c.CustomerID,
    UPPER(c.CustomerName) AS Src_CustomerName,
    dc.CustomerName AS Tgt_CustomerName,
    c.Age AS Src_Age,
    dc.Age AS Tgt_Age,
    c.Gender AS Src_Gender,
    dc.Gender AS Tgt_Gender,
    c.Email AS Src_Email,
    dc.Email AS Tgt_Email,
    CASE
        WHEN UPPER(c.CustomerName) = dc.CustomerName
         AND c.Age = dc.Age
         AND c.Gender = dc.Gender
         AND c.Email = dc.Email
        THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS Status
FROM Sample_DB.dbo.Customer c
INNER JOIN DWH.dbo.DimCustomer dc ON c.CustomerID = dc.CustomerID
ORDER BY c.CustomerID
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- Sample 4: FactTransaction - Compare SQL-sourced transactions
PRINT '--- FactTransaction Sample (Top 10 from SQL source) ---';
SELECT
    s.TransactionID,
    s.AccountID AS Src_AccountID,
    t.AccountID AS Tgt_AccountID,
    s.Amount AS Src_Amount,
    t.Amount AS Tgt_Amount,
    s.TransactionType AS Src_TransactionType,
    t.TransactionType AS Tgt_TransactionType,
    s.BranchID AS Src_BranchID,
    t.BranchID AS Tgt_BranchID,
    CASE
        WHEN s.AccountID = t.AccountID
         AND s.Amount = t.Amount
         AND s.TransactionType = t.TransactionType
         AND s.BranchID = t.BranchID
        THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS Status
FROM Sample_DB.dbo.[Transaction] s
INNER JOIN DWH.dbo.FactTransaction t ON s.TransactionID = t.TransactionID
ORDER BY s.TransactionID
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- Sample 5: Stored Procedure Output Validation
-- Verify sp_DailyTransaction produces expected aggregation
PRINT '--- sp_DailyTransaction Output Verification ---';
SELECT
    CAST(TransactionDate AS DATE) AS [Date],
    COUNT(TransactionID) AS TotalTransactions,
    SUM(Amount) AS TotalAmount
FROM DWH.dbo.FactTransaction
GROUP BY CAST(TransactionDate AS DATE)
ORDER BY [Date];

-- Sample 6: Stored Procedure Output Validation
-- Verify sp_BalancePerCustomer logic matches manual calculation
PRINT '--- sp_BalancePerCustomer Manual Verification ---';
WITH TransactionSummary AS (
    SELECT
        AccountID,
        SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END) AS TotalTransactionAmount
    FROM DWH.dbo.FactTransaction
    GROUP BY AccountID
)
SELECT TOP 10
    c.CustomerName,
    a.AccountType,
    a.Balance AS InitialBalance,
    a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
FROM DWH.dbo.DimCustomer c
JOIN DWH.dbo.DimAccount a ON c.CustomerID = a.CustomerID
LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
WHERE a.Status = 'active'
ORDER BY c.CustomerName;
GO
