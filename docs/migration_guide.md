# Migration guide: SQL Server + Talend → dbt + Databricks SQL

Cutover runbook for replacing the Talend jobs and the T-SQL stored procedures
with the dbt project in `dbt/`. Column- and job-level detail lives in
[`legacy_to_dbt_mapping.md`](legacy_to_dbt_mapping.md); the parity evidence is
produced by [`../parity/README.md`](../parity/README.md).

## 0. Prerequisites

| Item | Value |
| --- | --- |
| Warehouse | Databricks SQL warehouse, Unity Catalog enabled |
| Catalog | `banking` (schemas `raw`, `dev`, and the `+schema` suffixes from `dbt/dbt_project.yml`) |
| dbt | `dbt-databricks` (Python 3.9+) |
| Grants | `CREATE SCHEMA` on `banking`, `MODIFY`/`SELECT` on `banking.raw` |

```bash
python3 -m venv ~/dbtvenv
~/dbtvenv/bin/pip install dbt-databricks
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
export DATABRICKS_HOST=... DATABRICKS_HTTP_PATH=... DATABRICKS_TOKEN=...
```

## 1. Land the raw data

Two landing paths, matching the two kinds of legacy source:

1. **SQL Server tables** (`customer`, `city`, `state`, `account`, `branch`,
   `transaction_db`) are exposed to Unity Catalog as external tables in
   `banking.raw` and declared in `dbt/models/staging/_sources.yml`. Any existing
   replication (Lakehouse Federation, Fivetran, ADF, a one-off `sample.bak`
   restore + export) is acceptable — no ingestion tool is built by this program.
   Verify with:

   ```sql
   SELECT table_name FROM banking.information_schema.tables WHERE table_schema = 'raw';
   ```

2. **File extracts** (`data_sources/transaction_csv.csv`,
   `data_sources/transaction_excel.xlsx`) land as dbt seeds in `dbt/seeds/`.
   Convert the workbook to CSV before seeding (dbt seeds CSV only) and keep the
   legacy header names and the `dd-MM-yyyy HH:mm:ss` date format — the staging
   model parses that mask.

## 2. Build the project

```bash
cd dbt
dbt deps           # installs dbt_utils
dbt seed           # loads the CSV/Excel extracts into banking.raw
dbt build          # runs models and their tests in DAG order
```

`dbt build` replaces the manual Talend run order
(`Load_DimBranch` → `Load_DimAccount` → `Load_DimCustomer` →
`Load_FactTransaction`): the order is derived from `ref()` and cannot drift.

Useful subsets:

```bash
dbt build --select staging          # sources renamed/typed only
dbt build --select +fct_transaction # the fact and everything it depends on
dbt build --select marts.reporting  # just the two ex-procedure reports
```

## 3. Validate

```bash
dbt test                      # all schema tests
dbt test --select dim_customer
dbt docs generate && dbt docs serve
```

Expect exactly one failing test on the current sample data:
`relationships_fct_transaction_account_id__dim_account` (3 rows —
`transaction_id` 23, 24, 25 reference `account_id` 22 and 23, which do not exist
in `account`). SQL Server's foreign key silently rejected those rows at load
time. Accepted handling: keep the test with `severity: warn` (or
`error_if: '>3'`) and raise a data-quality ticket against the source extract.
Do not delete the test, and do not filter the rows out of the fact table — the
warehouse should surface the defect rather than hide it.

Run the offline parity harness for logic-level evidence (no Databricks needed):

```bash
pip install -r parity/requirements.txt
python3 parity/run_parity.py
```

It replays the Talend jobs and both stored procedures on DuckDB and asserts they
return the same rows as the dbt logic for six parameter scenarios.

For a live comparison against the legacy warehouse, run the two procedures on
SQL Server and diff against the dbt tables:

```sql
-- legacy
EXEC sp_DailyTransaction @start_date = '2024-01-01', @end_date = '2024-12-31';
EXEC sp_BalancePerCustomer @customer_name = 'Shelly';
```

```sql
-- Databricks
SELECT * FROM banking.marts.rpt_daily_transaction ORDER BY transaction_day;
SELECT * FROM banking.marts.rpt_balance_per_customer
WHERE upper(customer_name) LIKE '%SHELLY%' ORDER BY customer_name, account_type;
```

Row counts, `total_amount` and `current_balance` must match to the cent. Two
expected presentational differences: column names are `snake_case`, and
`dim_customer.customer_name` is upper-cased exactly as the Talend job left it.

## 4. Run the reports with parameters

The procedures' parameters are dbt vars (defaults in `dbt/dbt_project.yml`):

```bash
dbt build --select rpt_daily_transaction \
  --vars '{start_date: "2024-01-18", end_date: "2024-01-20"}'

dbt build --select rpt_balance_per_customer \
  --vars '{customer_name: "Shelly"}'
```

With the default empty `customer_name` the model contains every active account,
so a BI tool can filter it instead of re-running dbt. Prefer that for
self-service; use `--vars` only for scheduled, pre-filtered extracts.

## 5. Parallel run

Run both stacks for one full reporting cycle:

1. Keep the Talend jobs on their existing schedule, writing to the legacy `DWH`.
2. Schedule `dbt build` (dbt Cloud job, Databricks Workflow, or Airflow) on the
   same cadence.
3. Each cycle, diff the two reports as in step 3 and record the result.
4. Sign-off criterion: zero unexplained differences across a full cycle, and the
   only failing dbt test is the known `relationships` one above.

## 6. Cut over and decommission

Once parallel run is signed off:

1. Repoint BI dashboards from `DWH.dbo.sp_*` result sets to
   `banking.marts.rpt_daily_transaction` and
   `banking.marts.rpt_balance_per_customer`.
2. Disable the Talend job schedule (do not delete yet): unschedule
   `Load_DimBranch`, `Load_DimAccount`, `Load_DimCustomer`,
   `Load_FactTransaction` in Talend Administration Center / cron.
3. Set the legacy `DWH` database to `READ_ONLY` for one retention period so
   historical comparisons stay possible.
4. Take a final `DWH` backup and archive it next to `data_sources/sample.bak`.
5. Drop the stored procedures (`sp_DailyTransaction`, `sp_BalancePerCustomer`)
   and remove the Talend job artifacts from `talend_jobs/` in a dedicated PR
   that also links the sign-off record.
6. Keep `sql_scripts/` and this document as the historical reference for the
   translation.

## 7. Rollback

Before cutover, rollback is "re-enable the Talend schedule" — nothing else is
touched, because the dbt project writes to a different platform. After cutover
and before the legacy `DWH` is dropped, rollback is: set `DWH` back to
`READ_WRITE`, re-enable the four job schedules, and repoint the dashboards. The
retention window in step 3 exists to keep this option open.

## 8. Operational notes

* **Idempotency.** Every model is a full rebuild (`table`/`view`), so re-running
  `dbt build` is safe and produces the same result — the legacy jobs were
  truncate-and-load, so the semantics are unchanged.
* **Incremental loads.** Not in scope. When the fact table outgrows a full
  rebuild, convert `fct_transaction` to `incremental` with
  `unique_key='transaction_id'`; `int_transactions_unioned` already carries
  `record_source` for auditing.
* **Cost.** All marts are Delta tables; staging and intermediate are views, so
  only the marts are persisted.
* **Secrets.** `DATABRICKS_TOKEN` is read from the environment via
  `profiles.yml`; never commit a filled-in `profiles.yml`.
