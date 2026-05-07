/************************************************************************************
-- VALIDATION SCRIPT: Business Rules Verification
-- Purpose: Verify that stored procedure outputs produce expected results using
--          the migrated ADF data, ensuring business logic integrity is preserved.
-- Usage:   Execute against the DWH database after ETL run and SP deployment.
************************************************************************************/

USE DWH;
GO

PRINT '=== Business Rules Validation Report ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-------------------------------------------------------------------------------------
-- 1. Referential Integrity: FactTransaction -> DimAccount
-------------------------------------------------------------------------------------
PRINT '--- FK Integrity: FactTransaction -> DimAccount ---';

DECLARE @orphan_accounts INT;

SELECT @orphan_accounts = COUNT(*)
FROM [DWH].dbo.FactTransaction ft
LEFT JOIN [DWH].dbo.DimAccount da ON ft.AccountID = da.AccountID
WHERE da.AccountID IS NULL;

PRINT 'Orphan Records (no matching DimAccount): ' + CAST(@orphan_accounts AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @orphan_accounts = 0 THEN 'PASS' ELSE 'FAIL - Orphan records found!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 2. Referential Integrity: FactTransaction -> DimBranch
-------------------------------------------------------------------------------------
PRINT '--- FK Integrity: FactTransaction -> DimBranch ---';

DECLARE @orphan_branches INT;

SELECT @orphan_branches = COUNT(*)
FROM [DWH].dbo.FactTransaction ft
LEFT JOIN [DWH].dbo.DimBranch db ON ft.BranchID = db.BranchID
WHERE db.BranchID IS NULL;

PRINT 'Orphan Records (no matching DimBranch): ' + CAST(@orphan_branches AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @orphan_branches = 0 THEN 'PASS' ELSE 'FAIL - Orphan records found!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 3. Primary Key Uniqueness: DimCustomer
-------------------------------------------------------------------------------------
PRINT '--- PK Uniqueness: DimCustomer ---';

DECLARE @dup_customers INT;

SELECT @dup_customers = COUNT(*) - COUNT(DISTINCT CustomerID)
FROM [DWH].dbo.DimCustomer;

PRINT 'Duplicate CustomerIDs: ' + CAST(@dup_customers AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @dup_customers = 0 THEN 'PASS' ELSE 'FAIL - Duplicate keys found!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 4. Primary Key Uniqueness: FactTransaction
-------------------------------------------------------------------------------------
PRINT '--- PK Uniqueness: FactTransaction ---';

DECLARE @dup_transactions INT;

SELECT @dup_transactions = COUNT(*) - COUNT(DISTINCT TransactionID)
FROM [DWH].dbo.FactTransaction;

PRINT 'Duplicate TransactionIDs: ' + CAST(@dup_transactions AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @dup_transactions = 0 THEN 'PASS' ELSE 'FAIL - Duplicate keys found (tUniqRow migration issue)!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 5. Data Quality: DimCustomer UPPER() Transformation
-------------------------------------------------------------------------------------
PRINT '--- Data Quality: DimCustomer UPPER() Compliance ---';

DECLARE @non_upper INT;

SELECT @non_upper = COUNT(*)
FROM [DWH].dbo.DimCustomer
WHERE CustomerName <> UPPER(CustomerName) COLLATE Latin1_General_BIN
   OR Address <> UPPER(Address) COLLATE Latin1_General_BIN
   OR CityName <> UPPER(CityName) COLLATE Latin1_General_BIN
   OR StateName <> UPPER(StateName) COLLATE Latin1_General_BIN;

PRINT 'Records with non-uppercase text fields: ' + CAST(@non_upper AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @non_upper = 0 THEN 'PASS' ELSE 'FAIL - UPPER() transformation not applied correctly!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 6. Stored Procedure: sp_DailyTransaction Output Validation
-------------------------------------------------------------------------------------
PRINT '--- SP Validation: sp_DailyTransaction ---';

DECLARE @sp_result_count INT;

CREATE TABLE #DailyTxn (
    [Date] DATE,
    TotalTransactions INT,
    TotalAmount MONEY
);

-- Execute SP with a broad date range to capture all data
INSERT INTO #DailyTxn
EXEC sp_DailyTransaction @start_date = '2020-01-01', @end_date = '2030-12-31';

SELECT @sp_result_count = COUNT(*) FROM #DailyTxn;

-- Cross-validate: manual aggregation should match SP output
DECLARE @manual_agg_count INT;

SELECT @manual_agg_count = COUNT(DISTINCT CAST(TransactionDate AS DATE))
FROM [DWH].dbo.FactTransaction
WHERE CAST(TransactionDate AS DATE) BETWEEN '2020-01-01' AND '2030-12-31';

PRINT 'sp_DailyTransaction returned rows: ' + CAST(@sp_result_count AS VARCHAR);
PRINT 'Manual aggregation distinct dates: ' + CAST(@manual_agg_count AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @sp_result_count = @manual_agg_count THEN 'PASS' ELSE 'FAIL - SP output does not match manual aggregation!' END;

DROP TABLE #DailyTxn;
PRINT '';

-------------------------------------------------------------------------------------
-- 7. Stored Procedure: sp_BalancePerCustomer Output Validation
-------------------------------------------------------------------------------------
PRINT '--- SP Validation: sp_BalancePerCustomer ---';

CREATE TABLE #BalanceResult (
    CustomerName VARCHAR(100),
    AccountType VARCHAR(50),
    InitialBalance MONEY,
    CurrentBalance MONEY
);

-- Test with a known customer (using wildcard match)
DECLARE @test_customer VARCHAR(100);
SELECT TOP 1 @test_customer = CustomerName FROM [DWH].dbo.DimCustomer;

IF @test_customer IS NOT NULL
BEGIN
    INSERT INTO #BalanceResult
    EXEC sp_BalancePerCustomer @customer_name = @test_customer;

    DECLARE @sp_balance_count INT;
    SELECT @sp_balance_count = COUNT(*) FROM #BalanceResult;

    -- Validate: CurrentBalance = InitialBalance + SUM(Deposits) - SUM(Withdrawals)
    DECLARE @balance_mismatch INT;

    SELECT @balance_mismatch = COUNT(*)
    FROM #BalanceResult br
    JOIN [DWH].dbo.DimAccount da ON br.AccountType = da.AccountType
    JOIN [DWH].dbo.DimCustomer dc ON da.CustomerID = dc.CustomerID
        AND dc.CustomerName = br.CustomerName
    WHERE da.Status = 'active'
      AND br.CurrentBalance <> da.Balance + ISNULL((
          SELECT SUM(CASE WHEN ft.TransactionType = 'Deposit' THEN ft.Amount ELSE -ft.Amount END)
          FROM [DWH].dbo.FactTransaction ft
          WHERE ft.AccountID = da.AccountID
      ), 0);

    PRINT 'Test Customer: ' + @test_customer;
    PRINT 'sp_BalancePerCustomer returned rows: ' + CAST(@sp_balance_count AS VARCHAR);
    PRINT 'Balance calculation mismatches: ' + CAST(ISNULL(@balance_mismatch, 0) AS VARCHAR);
    PRINT 'Status: ' + CASE WHEN ISNULL(@balance_mismatch, 0) = 0 THEN 'PASS' ELSE 'FAIL - Balance calculation error!' END;
END
ELSE
BEGIN
    PRINT 'SKIP - No customer data available for testing.';
END

DROP TABLE #BalanceResult;
PRINT '';

-------------------------------------------------------------------------------------
-- 8. NULL Value Checks on Required Fields
-------------------------------------------------------------------------------------
PRINT '--- Data Quality: NULL Checks on Required Fields ---';

DECLARE @null_violations INT = 0;

SELECT @null_violations = @null_violations + COUNT(*)
FROM [DWH].dbo.DimBranch WHERE BranchID IS NULL OR BranchName IS NULL;

SELECT @null_violations = @null_violations + COUNT(*)
FROM [DWH].dbo.DimAccount WHERE AccountID IS NULL OR CustomerID IS NULL;

SELECT @null_violations = @null_violations + COUNT(*)
FROM [DWH].dbo.DimCustomer WHERE CustomerID IS NULL OR CustomerName IS NULL;

SELECT @null_violations = @null_violations + COUNT(*)
FROM [DWH].dbo.FactTransaction WHERE TransactionID IS NULL OR AccountID IS NULL OR Amount IS NULL;

PRINT 'Total NULL violations on required fields: ' + CAST(@null_violations AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @null_violations = 0 THEN 'PASS' ELSE 'FAIL - NULL values in required fields!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 9. Transaction Amount Validation (no negative amounts)
-------------------------------------------------------------------------------------
PRINT '--- Data Quality: Transaction Amounts ---';

DECLARE @negative_amounts INT;

SELECT @negative_amounts = COUNT(*)
FROM [DWH].dbo.FactTransaction
WHERE Amount < 0;

PRINT 'Transactions with negative amounts: ' + CAST(@negative_amounts AS VARCHAR);
PRINT 'Status: ' + CASE WHEN @negative_amounts = 0 THEN 'PASS' ELSE 'WARNING - Negative transaction amounts found (review business rules)' END;
PRINT '';

PRINT '=== Business Rules Validation Complete ===';
GO
