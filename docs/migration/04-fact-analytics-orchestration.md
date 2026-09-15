# Slice 4 — Gold fact table, analytics, orchestration

Maps the legacy `Load_FactTransaction` Talend job, the two T-SQL stored procedures and the
manual run order onto Databricks (Unity Catalog + Delta + PySpark + Workflows).

## Asset mapping

| Legacy asset | Databricks asset |
| --- | --- |
| `talend_jobs/Load_FactTransaction.zip` (`Load_FactTransaction_0.1.item`) | `databricks/gold/fact_transaction.py` (pure transforms) + `databricks/gold/jobs/load_fact_transaction.py` (Delta MERGE job) |
| `DWH.dbo.FactTransaction` | `banking.gold.fact_transaction` (Delta, managed) |
| `sp_DailyTransaction` | `banking.gold.daily_transaction(start_date DATE, end_date DATE)` SQL TVF + `GoldAnalytics.daily_transaction(...)` |
| `sp_BalancePerCustomer` | `banking.gold.balance_per_customer(customer_name STRING)` SQL TVF + `GoldAnalytics.balance_per_customer(...)` |
| Manual job run order (README) | `databricks/gold/orchestration/banking_medallion_job.yml` (Asset Bundle job) |

## A. `Load_FactTransaction` -> `gold.fact_transaction`

Talend graph read from the `.item` XML:

```
tMSSqlInput      (sample.dbo.transaction_db, mergeOrder 1) ─┐
tFileInputExcel  (transaction_excel.xlsx,    mergeOrder 2) ─┼─> tUnite -> tUniqRow -> tMap -> tMSSqlOutput
tFileInputDelimited (transaction_csv.csv,    mergeOrder 3) ─┘        key: transaction_id      TRUNCATE + INSERT
```

| Talend component | PySpark |
| --- | --- |
| 3 inputs | `normalize_source()` over the three bronze tables (names configurable in `GoldConfig`) |
| `tUnite` | `union_sources()` — `unionByName(allowMissingColumns=True)` |
| `tUniqRow` (key `transaction_id`) | `dedupe_transactions()` — `row_number()` over `transaction_id` |
| `tMap` (pass-through, PascalCase rename) | `select_fact_columns()`; snake_case naming is the target convention |
| `tMSSqlOutput` TRUNCATE + INSERT | Delta `MERGE` on `transaction_id` (`load_fact_transaction.py`) |

### Dedup winner (documented semantics)

`tUniqRow` keeps the **first** row it sees per key; "first" is `tUnite` arrival order, i.e. the
branch `mergeOrder`. Spark has no arrival order, so the winner is defined explicitly:

1. lowest `_source_priority` — `mssql` (1) < `excel` (2) < `csv` (3), matching `mergeOrder`;
2. then `_ingest_ts` ascending (nulls last);
3. then the payload columns (`account_id`, `transaction_date`, `amount`, `transaction_type`, `branch_id`).

Steps 2–3 make the result independent of partitioning and of the order the sources are passed in.
Losing rows are not discarded: they are returned as `FactLoadResult.duplicates` and written to
`gold.fact_transaction_duplicate` with their lineage columns.

### Types and parsing

- `transaction_date`: `dd-MM-yyyy HH:mm:ss` via `try_to_timestamp`, falling back to `dd-MM-yyyy`
  and then the default parser. Unparseable values become NULL rather than failing the run, which
  matches the Talend inputs (`Die on error` unchecked). Already-typed timestamp columns (the SQL
  Server extract) are cast directly.
- `amount`: `MONEY` -> `DECIMAL(19,4)`.
- ids: `INT`; `DATETIME` -> `TIMESTAMP`.

### Referential integrity

Unity Catalog FKs are `NOT ENFORCED`, so `split_orphans()` left-joins `gold.dim_account` and
`gold.dim_branch` and flags rows whose **non-NULL** key has no dimension row (a NULL key is not an
orphan — T-SQL FKs allow NULL). Orphans go to `gold.fact_transaction_orphan` with an
`_orphan_reason` (`missing_account_id` / `missing_branch_id`), never silently dropped;
`validate_analytics.py` fails the job if any exist.

### Not 1:1

- TRUNCATE+INSERT becomes an idempotent MERGE: reruns update in place instead of rebuilding, so a
  row deleted at source is no longer removed from the fact. Add a delete-detection step if the
  legacy full-refresh semantics are required.
- Talend wrote PascalCase columns; gold uses snake_case per the shared contract.
- Rejected/duplicate rows were invisible in Talend; they are now materialized tables.

## B. Stored procedures

`sp_DailyTransaction` and `sp_BalancePerCustomer` become Unity Catalog **table-valued SQL
functions** (`databricks/gold/sql/analytics_functions.sql`) plus pure PySpark functions in
`databricks/gold/analytics.py` with the same arguments.

Preserved: the `CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END` sign logic, the
`Status = 'active'` filter, `CustomerName LIKE '%' + @customer_name + '%'`, the inclusive
`BETWEEN` date range, and `Balance + ISNULL(net_change, 0)` arithmetic.

Semantic differences:

| T-SQL | Databricks |
| --- | --- |
| `CREATE PROCEDURE` with `@params` | `CREATE FUNCTION ... RETURNS TABLE` (TVF) — callable in `SELECT`, composable, no result-set side effect |
| `ISNULL(x, 0)` | `coalesce(x, 0)` |
| `SET NOCOUNT ON` | no equivalent and not needed — no rowcount messages |
| `'%' + @name + '%'` | `'%' || customer_name || '%'` (`+` is arithmetic in Spark SQL) |
| `ORDER BY` inside the procedure | ordering belongs to the caller; the TVF keeps it but a TVF's order is not guaranteed once composed |
| default `SQL_Latin1_General` case-insensitive `LIKE` | Spark `LIKE` is case-**sensitive**, so `balance_per_customer('john')` no longer matches `John`. Kept as-is rather than silently changing the filter — wrap both sides in `lower()` if case-insensitive matching is wanted |
| `MONEY` rounding | `DECIMAL(19,4)`; `SUM` widens to `DECIMAL(38,4)` |

## C. Orchestration

`databricks/gold/orchestration/banking_medallion_job.yml` (Databricks Asset Bundle) wires:

```
bronze_ingest -> dim_branch -> dim_account -> dim_customer -> fact_transaction -> analytics_validation
```

matching the legacy run order with an ingest stage in front and a validation stage at the end.
Each task has `max_retries`, timeouts and job-level parameters (`catalog`, `bronze_schema`,
`silver_schema`, `gold_schema`, plus `start_date` / `end_date` / `customer_name` for validation).
The bronze/silver entrypoints are owned by the other slices and are referenced by convention;
override them per target if those paths change.

### Runbook

```bash
# deploy / run
databricks bundle deploy -t dev --var="catalog=banking"
databricks bundle run banking_medallion -t dev

# one-off fact reload (skips the dimensions)
databricks bundle run banking_medallion -t dev --only fact_transaction

# register the analytics functions once per catalog
databricks sql -f databricks/gold/sql/analytics_functions.sql
```

Failure handling:

- `fact_transaction` fails -> check `gold.fact_transaction_orphan` for the run id; the MERGE is
  idempotent, so simply repair the dimensions and re-run the task.
- `analytics_validation` fails -> it reports duplicate `transaction_id`s or orphan rows; it never
  mutates data, so it is safe to re-run.
- Any task is safe to re-run from the Workflows UI; upstream tasks are not repeated.

## Local validation

```bash
cd databricks
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 python -m pytest tests/gold -q
```

16 tests cover dedup winner and determinism, timestamp parsing (including an unparseable value),
`DECIMAL(19,4)` precision, orphan quarantine, the fact schema contract, MERGE idempotency, and both
analytics functions against hand-computed expectations.

Delta Lake is not exercised locally (`delta-spark` has no build matching the installed Spark
version in this environment), so the MERGE is verified through `upsert_by_key()`, a DataFrame
equivalent of the `WHEN MATCHED UPDATE * / WHEN NOT MATCHED INSERT *` used by the job.
