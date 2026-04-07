/************************************************************************************
-- VERIFICATION SCRIPT FOR THE BANKING DATA WAREHOUSE
-- Project: Banking ETL Data Warehouse
-- Description: Run this script after completing the full pipeline (either via
--              Talend ETL or direct seed scripts) to confirm all tables are
--              populated correctly and stored procedures return expected results.
--
-- Each check prints PASS or FAIL so you can quickly scan the output.
************************************************************************************/

USE DWH;
GO

PRINT '====================================================================';
PRINT '  BANKING DATA WAREHOUSE - VERIFICATION REPORT';
PRINT '====================================================================';
PRINT '';

-- ============================================================================
-- CHECK 1: DimBranch row count
-- ============================================================================
DECLARE @dimBranchCount INT;
SELECT @dimBranchCount = COUNT(*) FROM DimBranch;
PRINT 'CHECK 1: DimBranch row count';
PRINT '  Expected: >= 5';
PRINT '  Actual:   ' + CAST(@dimBranchCount AS VARCHAR(10));
IF @dimBranchCount >= 5
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL';
PRINT '';
GO

-- ============================================================================
-- CHECK 2: DimAccount row count
-- ============================================================================
DECLARE @dimAccountCount INT;
SELECT @dimAccountCount = COUNT(*) FROM DimAccount;
PRINT 'CHECK 2: DimAccount row count';
PRINT '  Expected: >= 25';
PRINT '  Actual:   ' + CAST(@dimAccountCount AS VARCHAR(10));
IF @dimAccountCount >= 25
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL';
PRINT '';
GO

-- ============================================================================
-- CHECK 3: DimCustomer row count
-- ============================================================================
DECLARE @dimCustomerCount INT;
SELECT @dimCustomerCount = COUNT(*) FROM DimCustomer;
PRINT 'CHECK 3: DimCustomer row count';
PRINT '  Expected: >= 25';
PRINT '  Actual:   ' + CAST(@dimCustomerCount AS VARCHAR(10));
IF @dimCustomerCount >= 25
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL';
PRINT '';
GO

-- ============================================================================
-- CHECK 4: DimCustomer names are UPPERCASE (Talend transformation)
-- ============================================================================
DECLARE @lowerCaseNames INT;
SELECT @lowerCaseNames = COUNT(*)
FROM DimCustomer
WHERE CustomerName COLLATE Latin1_General_BIN != UPPER(CustomerName);
PRINT 'CHECK 4: DimCustomer names are UPPERCASE';
PRINT '  Expected lowercase names: 0';
PRINT '  Actual:   ' + CAST(@lowerCaseNames AS VARCHAR(10));
IF @lowerCaseNames = 0
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL - Some CustomerName values are not uppercase';
PRINT '';
GO

-- ============================================================================
-- CHECK 5: FactTransaction row count
-- ============================================================================
DECLARE @factTxnCount INT;
SELECT @factTxnCount = COUNT(*) FROM FactTransaction;
PRINT 'CHECK 5: FactTransaction row count';
PRINT '  Expected: >= 25 (after deduplication across all 3 sources)';
PRINT '  Actual:   ' + CAST(@factTxnCount AS VARCHAR(10));
IF @factTxnCount >= 25
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL';
PRINT '';
GO

-- ============================================================================
-- CHECK 6: FactTransaction has no duplicate TransactionIDs
-- ============================================================================
DECLARE @dupTxnIds INT;
SELECT @dupTxnIds = COUNT(*)
FROM (
    SELECT TransactionID, COUNT(*) AS cnt
    FROM FactTransaction
    GROUP BY TransactionID
    HAVING COUNT(*) > 1
) AS dups;
PRINT 'CHECK 6: FactTransaction has no duplicate TransactionIDs';
PRINT '  Expected duplicates: 0';
PRINT '  Actual:   ' + CAST(@dupTxnIds AS VARCHAR(10));
IF @dupTxnIds = 0
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL - Duplicate TransactionIDs found';
PRINT '';
GO

-- ============================================================================
-- CHECK 7: Foreign key integrity - FactTransaction.AccountID -> DimAccount
-- ============================================================================
DECLARE @orphanAccounts INT;
SELECT @orphanAccounts = COUNT(*)
FROM FactTransaction ft
WHERE NOT EXISTS (SELECT 1 FROM DimAccount da WHERE da.AccountID = ft.AccountID);
PRINT 'CHECK 7: FK integrity - FactTransaction.AccountID -> DimAccount';
PRINT '  Expected orphans: 0';
PRINT '  Actual:   ' + CAST(@orphanAccounts AS VARCHAR(10));
IF @orphanAccounts = 0
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL - Orphan AccountIDs in FactTransaction';
PRINT '';
GO

-- ============================================================================
-- CHECK 8: Foreign key integrity - FactTransaction.BranchID -> DimBranch
-- ============================================================================
DECLARE @orphanBranches INT;
SELECT @orphanBranches = COUNT(*)
FROM FactTransaction ft
WHERE NOT EXISTS (SELECT 1 FROM DimBranch db WHERE db.BranchID = ft.BranchID);
PRINT 'CHECK 8: FK integrity - FactTransaction.BranchID -> DimBranch';
PRINT '  Expected orphans: 0';
PRINT '  Actual:   ' + CAST(@orphanBranches AS VARCHAR(10));
IF @orphanBranches = 0
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL - Orphan BranchIDs in FactTransaction';
PRINT '';
GO

-- ============================================================================
-- CHECK 9: sp_DailyTransaction returns results
-- ============================================================================
PRINT 'CHECK 9: sp_DailyTransaction returns results';
PRINT '  Executing: EXEC sp_DailyTransaction @start_date=''2024-01-18'', @end_date=''2024-01-22''';
PRINT '';

DECLARE @dailyTxnRows INT;
CREATE TABLE #DailyTxnResult (
    [Date] DATE,
    TotalTransactions INT,
    TotalAmount MONEY
);
INSERT INTO #DailyTxnResult
EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-22';

SELECT @dailyTxnRows = COUNT(*) FROM #DailyTxnResult;

PRINT '  Rows returned: ' + CAST(@dailyTxnRows AS VARCHAR(10));
IF @dailyTxnRows > 0
BEGIN
    PRINT '  Result:   PASS';
    PRINT '';
    PRINT '  --- Daily Transaction Summary ---';
    -- Display the results
    SELECT * FROM #DailyTxnResult ORDER BY [Date];
END
ELSE
    PRINT '  Result:   FAIL - No rows returned';

DROP TABLE #DailyTxnResult;
PRINT '';
GO

-- ============================================================================
-- CHECK 10: sp_BalancePerCustomer returns results
-- ============================================================================
PRINT 'CHECK 10: sp_BalancePerCustomer returns results';
PRINT '  Executing: EXEC sp_BalancePerCustomer @customer_name=''ANDI''';
PRINT '';

DECLARE @balanceRows INT;
CREATE TABLE #BalanceResult (
    CustomerName VARCHAR(100),
    AccountType VARCHAR(50),
    InitialBalance MONEY,
    CurrentBalance MONEY
);
INSERT INTO #BalanceResult
EXEC sp_BalancePerCustomer @customer_name = 'ANDI';

SELECT @balanceRows = COUNT(*) FROM #BalanceResult;

PRINT '  Rows returned: ' + CAST(@balanceRows AS VARCHAR(10));
IF @balanceRows > 0
BEGIN
    PRINT '  Result:   PASS';
    PRINT '';
    PRINT '  --- Balance Per Customer (ANDI) ---';
    SELECT * FROM #BalanceResult;
END
ELSE
    PRINT '  Result:   FAIL - No rows returned';

DROP TABLE #BalanceResult;
PRINT '';
GO

-- ============================================================================
-- CHECK 11: Transaction types coverage
-- ============================================================================
PRINT 'CHECK 11: Transaction type coverage';
DECLARE @txnTypes INT;
SELECT @txnTypes = COUNT(DISTINCT TransactionType) FROM FactTransaction;
PRINT '  Expected distinct types: >= 4 (Deposit, Withdrawal, Transfer, Payment)';
PRINT '  Actual:   ' + CAST(@txnTypes AS VARCHAR(10));
IF @txnTypes >= 4
    PRINT '  Result:   PASS';
ELSE
    PRINT '  Result:   FAIL';
PRINT '';

SELECT TransactionType, COUNT(*) AS [Count], SUM(Amount) AS TotalAmount
FROM FactTransaction
GROUP BY TransactionType
ORDER BY TransactionType;
GO

-- ============================================================================
-- SUMMARY
-- ============================================================================
PRINT '';
PRINT '====================================================================';
PRINT '  VERIFICATION COMPLETE';
PRINT '  Review the output above for any FAIL results.';
PRINT '  All 11 checks should show PASS for a healthy warehouse.';
PRINT '====================================================================';
