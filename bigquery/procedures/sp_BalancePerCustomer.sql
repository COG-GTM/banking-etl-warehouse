-- ===================================================================================
-- BigQuery Routine: sp_BalancePerCustomer
-- Migrated from: sql_scripts/02_create_procedures.sql (T-SQL)
-- Description: Calculates the current balance of each active account for a
--              specific customer by applying transaction logic (deposits add,
--              all other types subtract).
-- ===================================================================================

CREATE OR REPLACE PROCEDURE `dataset.sp_BalancePerCustomer`(
    customer_name STRING
)
BEGIN
    SELECT
        c.CustomerName,
        a.AccountType,
        a.Balance AS InitialBalance,
        a.Balance + IFNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
    FROM
        DimCustomer c
    JOIN
        DimAccount a ON c.CustomerID = a.CustomerID
    LEFT JOIN (
        SELECT
            AccountID,
            SUM(
                CASE
                    WHEN TransactionType = 'Deposit' THEN Amount
                    ELSE -Amount
                END
            ) AS TotalTransactionAmount
        FROM
            FactTransaction
        GROUP BY
            AccountID
    ) AS ts ON a.AccountID = ts.AccountID
    WHERE
        c.CustomerName LIKE CONCAT('%', customer_name, '%')
        AND a.Status = 'active';
END;
