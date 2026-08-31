-- DuckDB equivalents of the schema.yml tests that replace the legacy
-- PRIMARY KEY / FOREIGN KEY constraints (Databricks does not enforce them).
-- Each row is one dbt test; failures > 0 means the test would fail.
SELECT 'unique_dim_account_account_id' AS test_name,
       (SELECT COUNT(*) FROM (SELECT account_id FROM dim_account GROUP BY account_id HAVING COUNT(*) > 1)) AS failures
UNION ALL SELECT 'not_null_dim_account_account_id',
       (SELECT COUNT(*) FROM dim_account WHERE account_id IS NULL)
UNION ALL SELECT 'unique_dim_branch_branch_id',
       (SELECT COUNT(*) FROM (SELECT branch_id FROM dim_branch GROUP BY branch_id HAVING COUNT(*) > 1))
UNION ALL SELECT 'not_null_dim_branch_branch_id',
       (SELECT COUNT(*) FROM dim_branch WHERE branch_id IS NULL)
UNION ALL SELECT 'unique_dim_customer_customer_id',
       (SELECT COUNT(*) FROM (SELECT customer_id FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1))
UNION ALL SELECT 'not_null_dim_customer_customer_id',
       (SELECT COUNT(*) FROM dim_customer WHERE customer_id IS NULL)
UNION ALL SELECT 'unique_fct_transaction_transaction_id',
       (SELECT COUNT(*) FROM (SELECT transaction_id FROM fct_transaction GROUP BY transaction_id HAVING COUNT(*) > 1))
UNION ALL SELECT 'not_null_fct_transaction_transaction_id',
       (SELECT COUNT(*) FROM fct_transaction WHERE transaction_id IS NULL)
UNION ALL SELECT 'relationships_fct_transaction_account_id__dim_account',
       (SELECT COUNT(*) FROM fct_transaction f
         LEFT JOIN dim_account a ON f.account_id = a.account_id
        WHERE f.account_id IS NOT NULL AND a.account_id IS NULL)
UNION ALL SELECT 'relationships_fct_transaction_branch_id__dim_branch',
       (SELECT COUNT(*) FROM fct_transaction f
         LEFT JOIN dim_branch b ON f.branch_id = b.branch_id
        WHERE f.branch_id IS NOT NULL AND b.branch_id IS NULL)
UNION ALL SELECT 'relationships_dim_account_customer_id__dim_customer',
       (SELECT COUNT(*) FROM dim_account a
         LEFT JOIN dim_customer c ON a.customer_id = c.customer_id
        WHERE a.customer_id IS NOT NULL AND c.customer_id IS NULL)
ORDER BY test_name;
