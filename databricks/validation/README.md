# Validation harness

`parity_check.py` compares the Databricks lakehouse output against the legacy
SQL Server `DWH` database. It is wired into the `dwh_etl_job` workflow as the
`validation` task (running in parallel with `analytics` after
`build_fact_transaction`), and can also be run standalone during cutover.

## What it checks

| Check | Entity | Tolerance |
|-------|--------|-----------|
| `row_count` | `dim_branch`, `dim_account`, `dim_customer`, `fact_transaction` | `row_count_tolerance_pct` (default `0.0`) |
| `sum_amount` | `gold.fact_transaction` | `amount_tolerance_pct` (default `0.01`) |
| `count_distinct_transaction_id` | `gold.fact_transaction` | `row_count_tolerance_pct` |
| `transaction_type_count` / `transaction_type_amount` | per `transaction_type` | as above |
| `min_transaction_date` / `max_transaction_date` | `gold.fact_transaction` | exact |
| `row_hash_diff` | `gold.fact_transaction`, business key `transaction_id` | exact (0 diffs) |

The hash diff canonicalises both sides before hashing (amount as
`DECIMAL(19,4)` with 4 decimals, timestamps as `yyyy-MM-dd HH:mm:ss`, strings
trimmed and upper-cased, `NULL` as `<NULL>`) so pure formatting differences do
not register as mismatches. Every differing business key is classified as
`missing_in_databricks`, `missing_in_legacy` or `value_mismatch`.

## Parameters (notebook widgets / task `base_parameters`)

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `catalog` | `dwh` | Unity Catalog catalog holding the lakehouse tables |
| `environment` | `dev` | Recorded on every result row |
| `secret_scope` | `dwh` | Secret scope with the legacy JDBC credentials |
| `fail_on_mismatch` | `false` | When `true`, the task fails the run on any breach |
| `row_count_tolerance_pct` | `0.0` | Relative tolerance for count checks |
| `amount_tolerance_pct` | `0.01` | Relative tolerance for monetary checks |
| `hash_diff_sample_rows` | `50` | Number of differing keys printed to the task log |

## Required secrets

Stored in the `dwh` scope (`databricks secrets put-secret dwh <key>`):

- `legacy_jdbc_host`
- `legacy_jdbc_port` (defaults to `1433`)
- `legacy_jdbc_database` (defaults to `DWH`)
- `legacy_jdbc_user`
- `legacy_jdbc_password`

The cluster also needs the Microsoft SQL Server JDBC driver
(`com.microsoft.sqlserver:mssql-jdbc`), which the bundle installs as a job
library — DBR 14.3 LTS already ships a compatible driver, so no extra library
is required for a stock cluster.

## Output

Every run appends one row per check to `<catalog>.analytics.parity_results`,
keyed by `run_id` (UTC timestamp), and prints a JSON summary. The same JSON is
returned via `dbutils.notebook.exit`, so an orchestrating job can branch on it.

```sql
-- last run at a glance
SELECT check_name, entity, legacy_value, databricks_value, delta_pct, passed
FROM dwh.analytics.parity_results
WHERE run_id = (SELECT MAX(run_id) FROM dwh.analytics.parity_results)
ORDER BY passed, check_name;
```

## How to run the parity check during cutover

1. **Prepare** — confirm the legacy Talend batch and the Databricks job have
   both completed for the *same* business date, and that the legacy SQL Server
   `DWH` is reachable from the workspace (VNet/PrivateLink or SQL endpoint
   allowlist). Verify the secrets above exist:
   `databricks secrets list-secrets dwh`.
2. **Run in parallel-run mode** (advisory, does not fail the pipeline) — this
   is the default configuration of the `validation` task, so simply let the
   daily job run, or trigger it ad hoc:

   ```bash
   databricks bundle run dwh_etl_job -t prod --only validation
   ```

   or from a notebook:

   ```python
   dbutils.notebook.run(
       "../validation/parity_check",
       timeout_seconds=3600,
       arguments={"catalog": "dwh", "environment": "prod", "fail_on_mismatch": "false"},
   )
   ```
3. **Review** — query `dwh.analytics.parity_results` for the run. Investigate
   any row with `passed = false`; the `details` column on `row_hash_diff`
   carries a sample of differing `transaction_id` values, which is usually
   enough to identify the offending source (CSV vs. Excel vs. `transaction_db`)
   or a type/rounding difference.
4. **Sign off** — require a clean run (all checks `passed = true`) on at least
   three consecutive business days before cutover.
5. **Harden** — flip the `validation` task's `fail_on_mismatch` to `"true"` in
   `databricks/workflows/dwh_etl_job.yml` so the workflow fails loudly on any
   post-cutover regression, then unpause the schedule
   (`pause_status: UNPAUSED`) and decommission the Talend jobs.

If the legacy system has already been decommissioned, the JDBC checks cannot
run; keep the notebook for historical reproduction only.
