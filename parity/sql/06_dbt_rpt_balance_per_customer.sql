-- dbt model marts/reporting/rpt_balance_per_customer.sql, rendered for DuckDB.
-- `{{ var('customer_name') }}` becomes a bind parameter; an empty value
-- matches every customer, which is how the model stays usable unfiltered.
WITH transaction_summary AS (
    SELECT
        account_id,
        SUM(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE -amount END)
            AS total_transaction_amount
    FROM fct_transaction
    GROUP BY account_id
)
SELECT
    c.customer_name,
    a.account_type,
    a.balance AS initial_balance,
    a.balance + COALESCE(ts.total_transaction_amount, 0) AS current_balance
FROM dim_customer c
INNER JOIN dim_account a ON c.customer_id = a.customer_id
LEFT JOIN transaction_summary ts ON a.account_id = ts.account_id
WHERE a.status = 'active'
  -- Databricks string comparison is case-sensitive, so the legacy
  -- case-insensitive LIKE is reproduced by upper()-ing both sides.
  AND UPPER(c.customer_name) LIKE '%' || UPPER($customer_name) || '%'
ORDER BY c.customer_name, a.account_type, a.balance;
