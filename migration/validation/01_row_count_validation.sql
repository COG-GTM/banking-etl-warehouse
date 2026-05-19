/************************************************************************************
-- ROW COUNT VALIDATION SCRIPT
-- Purpose: Compare row counts between source (Sample_DB) and target (DWH) tables
--          to verify data completeness after ADF pipeline execution.
-- Usage:   Run against the SQL Server instance hosting both Sample_DB and DWH.
-- Expected: All delta columns should show 0 (source count = target count).
************************************************************************************/

USE DWH;
GO

PRINT '=== ROW COUNT VALIDATION REPORT ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- DimAccount: Source vs Target
PRINT '--- DimAccount ---';
SELECT
    'DimAccount' AS TableName,
    src.cnt AS SourceCount,
    tgt.cnt AS TargetCount,
    src.cnt - tgt.cnt AS Delta,
    CASE WHEN src.cnt = tgt.cnt THEN 'PASS' ELSE 'FAIL' END AS Status
FROM
    (SELECT COUNT(*) AS cnt FROM Sample_DB.dbo.Account) src,
    (SELECT COUNT(*) AS cnt FROM DWH.dbo.DimAccount) tgt;

-- DimBranch: Source vs Target
PRINT '--- DimBranch ---';
SELECT
    'DimBranch' AS TableName,
    src.cnt AS SourceCount,
    tgt.cnt AS TargetCount,
    src.cnt - tgt.cnt AS Delta,
    CASE WHEN src.cnt = tgt.cnt THEN 'PASS' ELSE 'FAIL' END AS Status
FROM
    (SELECT COUNT(*) AS cnt FROM Sample_DB.dbo.Branch) src,
    (SELECT COUNT(*) AS cnt FROM DWH.dbo.DimBranch) tgt;

-- DimCustomer: Source vs Target
-- Note: DimCustomer is a denormalized JOIN of customer + city + state,
-- so row count should match the customer table (1 row per customer).
PRINT '--- DimCustomer ---';
SELECT
    'DimCustomer' AS TableName,
    src.cnt AS SourceCount,
    tgt.cnt AS TargetCount,
    src.cnt - tgt.cnt AS Delta,
    CASE WHEN src.cnt = tgt.cnt THEN 'PASS' ELSE 'FAIL' END AS Status
FROM
    (SELECT COUNT(*) AS cnt FROM Sample_DB.dbo.Customer) src,
    (SELECT COUNT(*) AS cnt FROM DWH.dbo.DimCustomer) tgt;

-- FactTransaction: Deduplicated count across all three sources
-- The ADF data flow deduplicates on TransactionID (replaces tUniqRow),
-- so the target count should equal the distinct transaction_id count
-- across SQL, CSV, and Excel sources.
PRINT '--- FactTransaction ---';
DECLARE @sql_count INT, @csv_count INT, @excel_count INT;
DECLARE @total_raw INT, @distinct_count INT, @target_count INT;

SELECT @sql_count = COUNT(*) FROM Sample_DB.dbo.[Transaction];
-- Note: CSV and Excel counts need to be obtained from staging or
-- pre-computed during the ETL run. The following uses the DWH target
-- as the deduplicated baseline.
SELECT @target_count = COUNT(*) FROM DWH.dbo.FactTransaction;
SELECT @distinct_count = COUNT(DISTINCT TransactionID) FROM DWH.dbo.FactTransaction;

SELECT
    'FactTransaction' AS TableName,
    @target_count AS TargetCount,
    @distinct_count AS DistinctTransactionIDs,
    @target_count - @distinct_count AS DuplicatesDelta,
    CASE WHEN @target_count = @distinct_count THEN 'PASS' ELSE 'FAIL - DUPLICATES FOUND' END AS DeduplicationStatus;

-- Summary
PRINT '';
PRINT '=== SUMMARY ===';
SELECT
    (SELECT COUNT(*) FROM DWH.dbo.DimAccount) AS DimAccountRows,
    (SELECT COUNT(*) FROM DWH.dbo.DimBranch) AS DimBranchRows,
    (SELECT COUNT(*) FROM DWH.dbo.DimCustomer) AS DimCustomerRows,
    (SELECT COUNT(*) FROM DWH.dbo.FactTransaction) AS FactTransactionRows;
GO
