-- gold.fn_balance_per_customer: port of DWH.dbo.sp_BalancePerCustomer (sql_scripts/02_create_procedures.sql).
-- Usage: SELECT * FROM migration_demo.banking_mig_gold.fn_balance_per_customer('shelly');
-- ${...} placeholders are substituted by banking_etl.analytics.functions.
-- SQL Server parity (default collation SQL_Latin1_General_CP1_CI_AS, see sql/analytics/README.md):
--   * CustomerName LIKE '%' + @customer_name + '%' is case-insensitive -> ILIKE. % and _ stay wildcards;
--     T-SQL LIKE has no escape character, so backslashes are doubled to stay literal under Spark's default
--     escape (char(92)). @customer_name is VARCHAR(100), so longer input is truncated to 100 chars.
--     Runs of % are collapsed (same matches): Spark compiles LIKE to a Java regex, and a run of
--     wildcards backtracks exponentially. NULL -> no rows; '' -> every customer with a non-NULL name.
--   * Status = 'active' / TransactionType = 'Deposit' ignore case and trailing spaces -> lower(rtrim(...)).
--   * Balances stay MONEY: DECIMAL(19,4). ISNULL(sum, 0) -> COALESCE(sum, 0).
-- The parameter shares its name with the customer_name column, so it is always referenced qualified.
${create} ${fn_balance_per_customer}(
  customer_name STRING COMMENT 'Case-insensitive substring of customer_name; LIKE wildcards % and _ allowed (T-SQL @customer_name VARCHAR(100)).'
)
RETURNS TABLE (
  customer_name STRING COMMENT 'gold.dim_customer.customer_name (T-SQL CustomerName).',
  account_type STRING COMMENT 'gold.dim_account.account_type (T-SQL AccountType).',
  initial_balance DECIMAL(19,4) COMMENT 'gold.dim_account.balance (T-SQL InitialBalance).',
  current_balance DECIMAL(19,4) COMMENT 'balance + signed sum of transactions, Deposit +, anything else - (T-SQL CurrentBalance).'
)
COMMENT 'Current balance of every active account of the customers whose name matches. Port of sp_BalancePerCustomer.'
RETURN
  WITH transaction_summary AS (
    SELECT
      account_id,
      SUM(CASE WHEN lower(rtrim(transaction_type)) = 'deposit' THEN amount ELSE -amount END) AS total_transaction_amount
    FROM ${fact_transaction}
    GROUP BY account_id
  )
  SELECT
    CAST(c.customer_name AS STRING) AS customer_name,
    CAST(a.account_type AS STRING) AS account_type,
    CAST(a.balance AS DECIMAL(19,4)) AS initial_balance,
    CAST(a.balance + COALESCE(ts.total_transaction_amount, 0) AS DECIMAL(19,4)) AS current_balance
  FROM ${dim_customer} c
  JOIN ${dim_account} a ON c.customer_id = a.customer_id
  LEFT JOIN transaction_summary ts ON a.account_id = ts.account_id
  WHERE c.customer_name ILIKE regexp_replace(
      concat('%', replace(left(${fn_balance_per_customer_short}.customer_name, 100), char(92), repeat(char(92), 2)), '%'),
      '%+', '%')
    AND lower(rtrim(a.status)) = 'active';
