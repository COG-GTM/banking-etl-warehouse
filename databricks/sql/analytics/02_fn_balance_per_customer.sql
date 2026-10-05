-- gold.fn_balance_per_customer: port of DWH.dbo.sp_BalancePerCustomer (sql_scripts/02_create_procedures.sql).
-- Unity Catalog SQL table-valued function: SELECT * FROM <catalog>.<gold>.fn_balance_per_customer('shelly').
-- Placeholders are substituted by banking_etl.analytics.functions from banking_etl.config.Settings.
-- SQL Server parity (default collation SQL_Latin1_General_CP1_CI_AS):
--   * CustomerName LIKE '%' + @customer_name + '%' is case-insensitive -> ILIKE. % and _ stay wildcards;
--     T-SQL LIKE has no escape character, so backslashes are doubled to stay literal under Spark's default
--     escape (char(92) = backslash). @customer_name is VARCHAR(100), so longer input is truncated to 100 chars.
--     Runs of % are collapsed to one (same matches): Spark compiles LIKE to a Java regex, and a run of
--     wildcards backtracks exponentially on non-matching names.
--     NULL -> no rows ('%' + NULL is NULL); '' -> every customer with a non-NULL name.
--   * Status = 'active' and TransactionType = 'Deposit' are case-insensitive and ignore trailing spaces
--     -> lower(rtrim(...)) = '...'.
--   * Balances stay MONEY: DECIMAL(19,4) (a.Balance + ISNULL(sum, 0) is MONEY in T-SQL).
-- Known gap: T-SQL [...] character classes are matched literally here (ILIKE has no bracket classes).
${create} ${fn_balance_per_customer}(
  customer_name STRING COMMENT 'Case-insensitive substring of CustomerName; LIKE wildcards % and _ allowed (T-SQL @customer_name VARCHAR(100)).'
)
RETURNS TABLE (
  CustomerName STRING COMMENT 'gold.dim_customer.CustomerName.',
  AccountType STRING COMMENT 'gold.dim_account.AccountType.',
  InitialBalance DECIMAL(19,4) COMMENT 'gold.dim_account.Balance.',
  CurrentBalance DECIMAL(19,4) COMMENT 'Balance + signed sum of transactions (Deposit +, anything else -).'
)
COMMENT 'Current balance of every active account of the customers whose name matches. Port of sp_BalancePerCustomer.'
RETURN
  WITH TransactionSummary AS (
    SELECT
      AccountID,
      SUM(CASE WHEN lower(rtrim(TransactionType)) = 'deposit' THEN Amount ELSE -Amount END) AS TotalTransactionAmount
    FROM ${fact_transaction}
    GROUP BY AccountID
  )
  SELECT
    c.CustomerName,
    a.AccountType,
    a.Balance AS InitialBalance,
    CAST(a.Balance + COALESCE(ts.TotalTransactionAmount, 0) AS DECIMAL(19,4)) AS CurrentBalance
  FROM ${dim_customer} c
  JOIN ${dim_account} a ON c.CustomerID = a.CustomerID
  LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
  WHERE c.CustomerName ILIKE regexp_replace(
      concat('%', replace(left(fn_balance_per_customer.customer_name, 100), char(92), repeat(char(92), 2)), '%'),
      '%+', '%')
    AND lower(rtrim(a.Status)) = 'active';
