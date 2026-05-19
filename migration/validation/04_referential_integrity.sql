/************************************************************************************
-- REFERENTIAL INTEGRITY VALIDATION SCRIPT
-- Purpose: Verify foreign key relationships and data consistency in the DWH
--          star schema after ADF pipeline execution.
-- Usage:   Run against the DWH database.
-- Expected: All orphan counts should be 0.
************************************************************************************/

USE DWH;
GO

PRINT '=== REFERENTIAL INTEGRITY VALIDATION REPORT ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- Check 1: FactTransaction -> DimAccount (FK_FactTransaction_DimAccount)
PRINT '--- FK: FactTransaction.AccountID -> DimAccount.AccountID ---';
SELECT
    'FactTransaction -> DimAccount' AS Relationship,
    COUNT(*) AS OrphanCount,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS Status
FROM DWH.dbo.FactTransaction ft
WHERE NOT EXISTS (
    SELECT 1 FROM DWH.dbo.DimAccount da WHERE da.AccountID = ft.AccountID
);

-- Check 2: FactTransaction -> DimBranch (FK_FactTransaction_DimBranch)
PRINT '--- FK: FactTransaction.BranchID -> DimBranch.BranchID ---';
SELECT
    'FactTransaction -> DimBranch' AS Relationship,
    COUNT(*) AS OrphanCount,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS Status
FROM DWH.dbo.FactTransaction ft
WHERE NOT EXISTS (
    SELECT 1 FROM DWH.dbo.DimBranch db WHERE db.BranchID = ft.BranchID
);

-- Check 3: DimAccount -> DimCustomer (logical relationship)
PRINT '--- Logical FK: DimAccount.CustomerID -> DimCustomer.CustomerID ---';
SELECT
    'DimAccount -> DimCustomer' AS Relationship,
    COUNT(*) AS OrphanCount,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS Status
FROM DWH.dbo.DimAccount da
WHERE NOT EXISTS (
    SELECT 1 FROM DWH.dbo.DimCustomer dc WHERE dc.CustomerID = da.CustomerID
);

-- Check 4: NULL value checks on required columns
PRINT '--- NULL Value Checks ---';
SELECT
    'DimAccount.AccountID' AS ColumnCheck,
    SUM(CASE WHEN AccountID IS NULL THEN 1 ELSE 0 END) AS NullCount,
    CASE WHEN SUM(CASE WHEN AccountID IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END AS Status
FROM DWH.dbo.DimAccount
UNION ALL
SELECT
    'DimBranch.BranchID',
    SUM(CASE WHEN BranchID IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN BranchID IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM DWH.dbo.DimBranch
UNION ALL
SELECT
    'DimCustomer.CustomerID',
    SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM DWH.dbo.DimCustomer
UNION ALL
SELECT
    'FactTransaction.TransactionID',
    SUM(CASE WHEN TransactionID IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN TransactionID IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM DWH.dbo.FactTransaction
UNION ALL
SELECT
    'FactTransaction.AccountID',
    SUM(CASE WHEN AccountID IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN AccountID IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM DWH.dbo.FactTransaction
UNION ALL
SELECT
    'FactTransaction.BranchID',
    SUM(CASE WHEN BranchID IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN BranchID IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM DWH.dbo.FactTransaction;

-- Check 5: Duplicate primary key detection
PRINT '--- Duplicate Primary Key Checks ---';
SELECT
    'FactTransaction.TransactionID' AS PKCheck,
    COUNT(*) AS DuplicateCount,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS Status
FROM (
    SELECT TransactionID, COUNT(*) AS cnt
    FROM DWH.dbo.FactTransaction
    GROUP BY TransactionID
    HAVING COUNT(*) > 1
) dups;
GO
