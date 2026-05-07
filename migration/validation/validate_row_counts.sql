/************************************************************************************
-- VALIDATION SCRIPT: Row Count Comparison
-- Purpose: Compare row counts between source tables and DWH target tables
--          to verify data completeness after ADF pipeline execution.
-- Usage:   Execute against the DWH database after each ETL run.
************************************************************************************/

USE DWH;
GO

PRINT '=== Row Count Validation Report ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-------------------------------------------------------------------------------------
-- 1. DimBranch Validation
-------------------------------------------------------------------------------------
PRINT '--- DimBranch ---';

DECLARE @src_branch INT, @tgt_branch INT;

SELECT @src_branch = COUNT(*) FROM [sample].dbo.branch;
SELECT @tgt_branch = COUNT(*) FROM [DWH].dbo.DimBranch;

PRINT 'Source (branch):     ' + CAST(@src_branch AS VARCHAR);
PRINT 'Target (DimBranch):  ' + CAST(@tgt_branch AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @src_branch = @tgt_branch THEN 'PASS' ELSE 'FAIL - Row count mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 2. DimAccount Validation
-------------------------------------------------------------------------------------
PRINT '--- DimAccount ---';

DECLARE @src_account INT, @tgt_account INT;

SELECT @src_account = COUNT(*) FROM [sample].dbo.account;
SELECT @tgt_account = COUNT(*) FROM [DWH].dbo.DimAccount;

PRINT 'Source (account):     ' + CAST(@src_account AS VARCHAR);
PRINT 'Target (DimAccount):  ' + CAST(@tgt_account AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @src_account = @tgt_account THEN 'PASS' ELSE 'FAIL - Row count mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 3. DimCustomer Validation
-------------------------------------------------------------------------------------
PRINT '--- DimCustomer ---';

DECLARE @src_customer INT, @tgt_customer INT;

SELECT @src_customer = COUNT(*) FROM [sample].dbo.customer;
SELECT @tgt_customer = COUNT(*) FROM [DWH].dbo.DimCustomer;

PRINT 'Source (customer):     ' + CAST(@src_customer AS VARCHAR);
PRINT 'Target (DimCustomer):  ' + CAST(@tgt_customer AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @src_customer = @tgt_customer THEN 'PASS' ELSE 'FAIL - Row count mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 4. FactTransaction Validation
-- Note: Source count is the DEDUPLICATED total across all 3 sources (SQL, Excel, CSV)
-------------------------------------------------------------------------------------
PRINT '--- FactTransaction ---';

DECLARE @src_sql INT, @src_excel INT, @src_csv INT, @src_total_dedup INT, @tgt_fact INT;

SELECT @src_sql = COUNT(*) FROM [sample].dbo.transaction_sql;
SELECT @src_excel = COUNT(*) FROM [sample].dbo.transaction_excel;
SELECT @src_csv = COUNT(*) FROM [sample].dbo.transaction_csv;

-- Deduplicated count across all sources (mirrors tUniqRow behavior)
SELECT @src_total_dedup = COUNT(DISTINCT transaction_id) FROM (
    SELECT transaction_id FROM [sample].dbo.transaction_sql
    UNION ALL
    SELECT transaction_id FROM [sample].dbo.transaction_excel
    UNION ALL
    SELECT transaction_id FROM [sample].dbo.transaction_csv
) AS combined;

SELECT @tgt_fact = COUNT(*) FROM [DWH].dbo.FactTransaction;

PRINT 'Source (SQL):                ' + CAST(@src_sql AS VARCHAR);
PRINT 'Source (Excel):              ' + CAST(@src_excel AS VARCHAR);
PRINT 'Source (CSV):                ' + CAST(@src_csv AS VARCHAR);
PRINT 'Source (Deduplicated Total): ' + CAST(@src_total_dedup AS VARCHAR);
PRINT 'Target (FactTransaction):    ' + CAST(@tgt_fact AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @src_total_dedup = @tgt_fact THEN 'PASS' ELSE 'FAIL - Row count mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 5. Summary Table
-------------------------------------------------------------------------------------
PRINT '=== Summary ===';

SELECT
    'DimBranch' AS [Table],
    @src_branch AS [Source_Count],
    @tgt_branch AS [Target_Count],
    CASE WHEN @src_branch = @tgt_branch THEN 'PASS' ELSE 'FAIL' END AS [Status]
UNION ALL
SELECT
    'DimAccount',
    @src_account,
    @tgt_account,
    CASE WHEN @src_account = @tgt_account THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT
    'DimCustomer',
    @src_customer,
    @tgt_customer,
    CASE WHEN @src_customer = @tgt_customer THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT
    'FactTransaction',
    @src_total_dedup,
    @tgt_fact,
    CASE WHEN @src_total_dedup = @tgt_fact THEN 'PASS' ELSE 'FAIL' END;
GO
