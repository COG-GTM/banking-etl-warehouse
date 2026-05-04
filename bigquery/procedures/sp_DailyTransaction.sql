-- ===================================================================================
-- BigQuery Routine: sp_DailyTransaction
-- Migrated from: sql_scripts/02_create_procedures.sql (T-SQL)
-- Description: Generates a daily summary of transaction volume and total amount
--              for a given date range.
-- ===================================================================================

CREATE OR REPLACE PROCEDURE `dataset.sp_DailyTransaction`(
    start_date DATE,
    end_date DATE
)
BEGIN
    SELECT
        CAST(TransactionDate AS DATE) AS `Date`,
        COUNT(TransactionID) AS TotalTransactions,
        SUM(Amount) AS TotalAmount
    FROM
        FactTransaction
    WHERE
        CAST(TransactionDate AS DATE) BETWEEN start_date AND end_date
    GROUP BY
        CAST(TransactionDate AS DATE)
    ORDER BY
        `Date`;
END;
