/************************************************************************************
-- VALIDATION SCRIPT: Checksum Validation
-- Purpose: Verify data integrity by comparing checksums between source and target
--          tables after ADF pipeline execution.
-- Usage:   Execute against the DWH database after each ETL run.
************************************************************************************/

USE DWH;
GO

PRINT '=== Checksum Validation Report ===';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-------------------------------------------------------------------------------------
-- 1. DimBranch Checksum
-------------------------------------------------------------------------------------
PRINT '--- DimBranch Checksum ---';

DECLARE @src_branch_chk BIGINT, @tgt_branch_chk BIGINT;

SELECT @src_branch_chk = SUM(CAST(CHECKSUM(BranchID, BranchName, BranchLocation) AS BIGINT))
FROM [sample].dbo.branch;

SELECT @tgt_branch_chk = SUM(CAST(CHECKSUM(BranchID, BranchName, BranchLocation) AS BIGINT))
FROM [DWH].dbo.DimBranch;

PRINT 'Source Checksum: ' + CAST(ISNULL(@src_branch_chk, 0) AS VARCHAR);
PRINT 'Target Checksum: ' + CAST(ISNULL(@tgt_branch_chk, 0) AS VARCHAR);
PRINT 'Status: ' + CASE WHEN ISNULL(@src_branch_chk, 0) = ISNULL(@tgt_branch_chk, 0) THEN 'PASS' ELSE 'FAIL - Checksum mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 2. DimAccount Checksum
-------------------------------------------------------------------------------------
PRINT '--- DimAccount Checksum ---';

DECLARE @src_account_chk BIGINT, @tgt_account_chk BIGINT;

SELECT @src_account_chk = SUM(CAST(CHECKSUM(AccountID, CustomerID, AccountType, Balance, DateOpened, Status) AS BIGINT))
FROM [sample].dbo.account;

SELECT @tgt_account_chk = SUM(CAST(CHECKSUM(AccountID, CustomerID, AccountType, Balance, DateOpened, Status) AS BIGINT))
FROM [DWH].dbo.DimAccount;

PRINT 'Source Checksum: ' + CAST(ISNULL(@src_account_chk, 0) AS VARCHAR);
PRINT 'Target Checksum: ' + CAST(ISNULL(@tgt_account_chk, 0) AS VARCHAR);
PRINT 'Status: ' + CASE WHEN ISNULL(@src_account_chk, 0) = ISNULL(@tgt_account_chk, 0) THEN 'PASS' ELSE 'FAIL - Checksum mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 3. DimCustomer Checksum
-- Note: Checksum is computed on transformed (UPPER) values to match ADF output
-------------------------------------------------------------------------------------
PRINT '--- DimCustomer Checksum ---';

DECLARE @src_customer_chk BIGINT, @tgt_customer_chk BIGINT;

SELECT @src_customer_chk = SUM(CAST(CHECKSUM(
    c.customer_id,
    UPPER(c.customer_name),
    UPPER(c.address),
    UPPER(ci.city_name),
    UPPER(s.state_name),
    c.age,
    c.gender,
    LOWER(c.email)
) AS BIGINT))
FROM [sample].dbo.customer c
LEFT JOIN [sample].dbo.city ci ON c.city_id = ci.city_id
LEFT JOIN [sample].dbo.state s ON ci.state_id = s.state_id;

SELECT @tgt_customer_chk = SUM(CAST(CHECKSUM(
    CustomerID,
    CustomerName,
    Address,
    CityName,
    StateName,
    Age,
    Gender,
    Email
) AS BIGINT))
FROM [DWH].dbo.DimCustomer;

PRINT 'Source Checksum (transformed): ' + CAST(ISNULL(@src_customer_chk, 0) AS VARCHAR);
PRINT 'Target Checksum:               ' + CAST(ISNULL(@tgt_customer_chk, 0) AS VARCHAR);
PRINT 'Status: ' + CASE WHEN ISNULL(@src_customer_chk, 0) = ISNULL(@tgt_customer_chk, 0) THEN 'PASS' ELSE 'FAIL - Checksum mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 4. FactTransaction Checksum
-- Note: Computed on deduplicated transaction set
-------------------------------------------------------------------------------------
PRINT '--- FactTransaction Checksum ---';

DECLARE @src_fact_chk BIGINT, @tgt_fact_chk BIGINT;

WITH DeduplicatedTransactions AS (
    SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
    FROM (
        SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id FROM [sample].dbo.transaction_sql
        UNION ALL
        SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id FROM [sample].dbo.transaction_excel
        UNION ALL
        SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id FROM [sample].dbo.transaction_csv
    ) AS combined
)
SELECT @src_fact_chk = SUM(CAST(CHECKSUM(transaction_id, account_id, amount, transaction_type, branch_id) AS BIGINT))
FROM DeduplicatedTransactions
WHERE rn = 1;

SELECT @tgt_fact_chk = SUM(CAST(CHECKSUM(TransactionID, AccountID, Amount, TransactionType, BranchID) AS BIGINT))
FROM [DWH].dbo.FactTransaction;

PRINT 'Source Checksum (deduplicated): ' + CAST(ISNULL(@src_fact_chk, 0) AS VARCHAR);
PRINT 'Target Checksum:                ' + CAST(ISNULL(@tgt_fact_chk, 0) AS VARCHAR);
PRINT 'Status: ' + CASE WHEN ISNULL(@src_fact_chk, 0) = ISNULL(@tgt_fact_chk, 0) THEN 'PASS' ELSE 'FAIL - Checksum mismatch!' END;
PRINT '';

-------------------------------------------------------------------------------------
-- 5. Summary
-------------------------------------------------------------------------------------
PRINT '=== Checksum Summary ===';

SELECT
    'DimBranch' AS [Table],
    ISNULL(@src_branch_chk, 0) AS [Source_Checksum],
    ISNULL(@tgt_branch_chk, 0) AS [Target_Checksum],
    CASE WHEN ISNULL(@src_branch_chk, 0) = ISNULL(@tgt_branch_chk, 0) THEN 'PASS' ELSE 'FAIL' END AS [Status]
UNION ALL
SELECT 'DimAccount', ISNULL(@src_account_chk, 0), ISNULL(@tgt_account_chk, 0),
    CASE WHEN ISNULL(@src_account_chk, 0) = ISNULL(@tgt_account_chk, 0) THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'DimCustomer', ISNULL(@src_customer_chk, 0), ISNULL(@tgt_customer_chk, 0),
    CASE WHEN ISNULL(@src_customer_chk, 0) = ISNULL(@tgt_customer_chk, 0) THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'FactTransaction', ISNULL(@src_fact_chk, 0), ISNULL(@tgt_fact_chk, 0),
    CASE WHEN ISNULL(@src_fact_chk, 0) = ISNULL(@tgt_fact_chk, 0) THEN 'PASS' ELSE 'FAIL' END;
GO
