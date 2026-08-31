-- DuckDB translation of sp_BalancePerCustomer (sql_scripts/02_create_procedures.sql).
-- Changes: @customer_name becomes a bind parameter; LIKE becomes ILIKE because
-- SQL Server's default collation (SQL_Latin1_General_CP1_CI_AS) is
-- case-insensitive while DuckDB's LIKE is not; ISNULL -> COALESCE; a
-- deterministic ORDER BY is appended so results can be diffed (the procedure
-- itself returns rows in an unspecified order).
WITH "TransactionSummary" AS (
    SELECT
        "AccountID",
        SUM(CASE WHEN "TransactionType" = 'Deposit' THEN "Amount" ELSE -"Amount" END)
            AS "TotalTransactionAmount"
    FROM "FactTransaction"
    GROUP BY "AccountID"
)
SELECT
    c."CustomerName",
    a."AccountType",
    a."Balance" AS "InitialBalance",
    a."Balance" + COALESCE(ts."TotalTransactionAmount", 0) AS "CurrentBalance"
FROM "DimCustomer" c
JOIN "DimAccount" a ON c."CustomerID" = a."CustomerID"
LEFT JOIN "TransactionSummary" ts ON a."AccountID" = ts."AccountID"
WHERE c."CustomerName" ILIKE '%' || $customer_name || '%'
  AND a."Status" = 'active'
ORDER BY c."CustomerName", a."AccountType", a."Balance";
