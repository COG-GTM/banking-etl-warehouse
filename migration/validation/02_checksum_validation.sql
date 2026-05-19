/************************************************************************************
-- CHECKSUM / HASH VALIDATION SCRIPT
-- Purpose: Compare data integrity between source and target using CHECKSUM_AGG
--          and HASHBYTES to detect data corruption or transformation errors.
-- Usage:   Run against the SQL Server instance hosting both Sample_DB and DWH.
-- Expected: All hash comparison columns should show 'MATCH'.
************************************************************************************/

USE DWH;
GO

PRINT '=== CHECKSUM VALIDATION REPORT ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- DimAccount: Checksum comparison
PRINT '--- DimAccount Checksum ---';
SELECT
    'DimAccount' AS TableName,
    src.chk AS SourceChecksum,
    tgt.chk AS TargetChecksum,
    CASE WHEN src.chk = tgt.chk THEN 'MATCH' ELSE 'MISMATCH' END AS Status
FROM
    (SELECT CHECKSUM_AGG(CHECKSUM(AccountID, CustomerID, AccountType, Balance)) AS chk
     FROM Sample_DB.dbo.Account) src,
    (SELECT CHECKSUM_AGG(CHECKSUM(AccountID, CustomerID, AccountType, Balance)) AS chk
     FROM DWH.dbo.DimAccount) tgt;

-- DimBranch: Checksum comparison
PRINT '--- DimBranch Checksum ---';
SELECT
    'DimBranch' AS TableName,
    src.chk AS SourceChecksum,
    tgt.chk AS TargetChecksum,
    CASE WHEN src.chk = tgt.chk THEN 'MATCH' ELSE 'MISMATCH' END AS Status
FROM
    (SELECT CHECKSUM_AGG(CHECKSUM(BranchID, BranchName, BranchLocation)) AS chk
     FROM Sample_DB.dbo.Branch) src,
    (SELECT CHECKSUM_AGG(CHECKSUM(BranchID, BranchName, BranchLocation)) AS chk
     FROM DWH.dbo.DimBranch) tgt;

-- DimCustomer: Hash comparison on key columns
-- Note: CustomerName is UPPER() transformed in ADF, so we apply UPPER() to source
-- for a fair comparison.
PRINT '--- DimCustomer Hash ---';
WITH SourceHash AS (
    SELECT
        c.CustomerID,
        HASHBYTES('SHA2_256',
            CONCAT(
                CAST(c.CustomerID AS VARCHAR),
                '|', UPPER(c.CustomerName),
                '|', UPPER(c.Address),
                '|', CAST(c.Age AS VARCHAR),
                '|', c.Gender,
                '|', c.Email
            )
        ) AS row_hash
    FROM Sample_DB.dbo.Customer c
),
TargetHash AS (
    SELECT
        CustomerID,
        HASHBYTES('SHA2_256',
            CONCAT(
                CAST(CustomerID AS VARCHAR),
                '|', CustomerName,
                '|', Address,
                '|', CAST(Age AS VARCHAR),
                '|', Gender,
                '|', Email
            )
        ) AS row_hash
    FROM DWH.dbo.DimCustomer
)
SELECT
    'DimCustomer' AS TableName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN s.row_hash = t.row_hash THEN 1 ELSE 0 END) AS MatchingRows,
    SUM(CASE WHEN s.row_hash != t.row_hash THEN 1 ELSE 0 END) AS MismatchedRows,
    CASE
        WHEN SUM(CASE WHEN s.row_hash != t.row_hash THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS Status
FROM SourceHash s
INNER JOIN TargetHash t ON s.CustomerID = t.CustomerID;

-- FactTransaction: Hash comparison on key columns
PRINT '--- FactTransaction Hash ---';
WITH SourceHash AS (
    SELECT
        TransactionID,
        HASHBYTES('SHA2_256',
            CONCAT(
                CAST(TransactionID AS VARCHAR),
                '|', CAST(AccountID AS VARCHAR),
                '|', CAST(Amount AS VARCHAR),
                '|', TransactionType,
                '|', CAST(BranchID AS VARCHAR)
            )
        ) AS row_hash
    FROM Sample_DB.dbo.[Transaction]
),
TargetHash AS (
    SELECT
        TransactionID,
        HASHBYTES('SHA2_256',
            CONCAT(
                CAST(TransactionID AS VARCHAR),
                '|', CAST(AccountID AS VARCHAR),
                '|', CAST(Amount AS VARCHAR),
                '|', TransactionType,
                '|', CAST(BranchID AS VARCHAR)
            )
        ) AS row_hash
    FROM DWH.dbo.FactTransaction
)
SELECT
    'FactTransaction (SQL source)' AS TableName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN s.row_hash = t.row_hash THEN 1 ELSE 0 END) AS MatchingRows,
    SUM(CASE WHEN s.row_hash != t.row_hash THEN 1 ELSE 0 END) AS MismatchedRows,
    CASE
        WHEN SUM(CASE WHEN s.row_hash != t.row_hash THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS Status
FROM SourceHash s
INNER JOIN TargetHash t ON s.TransactionID = t.TransactionID;
GO
