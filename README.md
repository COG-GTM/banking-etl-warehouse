# Banking Data Warehouse on Databricks

An end-to-end lakehouse implementation of a banking analytics data warehouse,
built on **Databricks**, **Delta Lake**, **Unity Catalog** and **PySpark /
Spark SQL** using a medallion (bronze → silver → gold) architecture.

The warehouse consolidates operational banking data that is scattered across a
relational source system and flat-file exports into a single governed star
schema, and exposes analytics-ready aggregates for reporting.

This repository is a migration of an earlier Talend Open Studio + Microsoft SQL
Server solution; see [Legacy (Talend/SQL Server)](#legacy-talendsql-server) for
what the original looked like and where its artefacts still live.

---

## Architecture

```
┌────────────────────────┐
│ Sources                │
│  • SQL Server `sample` │   JDBC
│    (customer, city,    ├──────────┐
│     state, account,    │          │
│     branch,            │          │
│     transaction_db)    │          │
│  • transaction_csv.csv │  Auto    │
│  • transaction_        │  Loader  │
│    excel.xlsx          ├──────────┤
└────────────────────────┘          │
                                    ▼
                         ┌──────────────────────┐
                         │ BRONZE  dwh.bronze.* │  raw, append-only, typed as-is
                         │  customer, city,     │  + ingestion metadata
                         │  state, account,     │
                         │  branch,             │
                         │  transaction_db,     │
                         │  transaction_csv,    │
                         │  transaction_excel   │
                         └──────────┬───────────┘
                                    ▼
                         ┌──────────────────────┐
                         │ SILVER  dwh.silver.* │  cleansed & conformed
                         │  dim_branch          │  dimensions
                         │  dim_account         │
                         │  dim_customer        │  (customer ⨝ city ⨝ state)
                         └──────────┬───────────┘
                                    ▼
                         ┌──────────────────────┐
                         │ GOLD    dwh.gold.*   │  star-schema fact:
                         │  fact_transaction    │  union of the 3 transaction
                         │                      │  sources, deduplicated on
                         │                      │  transaction_id
                         └──────────┬───────────┘
                                    ▼
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
        ┌───────────────────────┐       ┌──────────────────────────┐
        │ ANALYTICS             │       │ VALIDATION               │
        │ dwh.analytics.*       │       │ dwh.analytics.           │
        │  daily_transaction    │       │   parity_results         │
        │  balance_per_customer │       │  legacy-vs-lakehouse     │
        └───────────────────────┘       └──────────────────────────┘
```

**Star schema (gold):** `fact_transaction` joins to `dim_account` on
`account_id` and `dim_branch` on `branch_id`; `dim_account` joins to
`dim_customer` on `customer_id`.

### Unity Catalog naming

| Layer | Namespace | Tables |
|-------|-----------|--------|
| Bronze | `dwh.bronze` | `customer`, `city`, `state`, `account`, `branch`, `transaction_db`, `transaction_csv`, `transaction_excel` |
| Silver | `dwh.silver` | `dim_branch`, `dim_account`, `dim_customer` |
| Gold | `dwh.gold` | `fact_transaction` |
| Analytics | `dwh.analytics` | `daily_transaction`, `balance_per_customer`, `parity_results` |

### Type mapping from the legacy T-SQL DDL

| T-SQL | Delta / Spark |
|-------|---------------|
| `MONEY` | `DECIMAL(19,4)` |
| `DATETIME` | `TIMESTAMP` |
| `DATE` | `DATE` |
| `VARCHAR(n)` | `STRING` |
| `INT` | `INT` |

---

## Repository layout

```
databricks/
  ddl/          Delta DDL — catalog/schema creation and the gold star schema
  bronze/       Raw ingestion notebooks (JDBC, CSV, Excel)
  silver/       Cleansed/conformed dimension builds
  gold/         Fact table build
  analytics/    Stored-procedure replacements (parameterised Spark SQL)
  validation/   Legacy-vs-lakehouse parity harness  ── see validation/README.md
  workflows/    Databricks Workflow job definitions (dwh_etl_job.yml)
docs/           Specs and source-to-target mapping documents
sql_scripts/    Legacy T-SQL DDL and stored procedures (reference only)
talend_jobs/    Legacy Talend job exports (reference only)
data_sources/   Sample source data: sample.bak, transaction_csv.csv, transaction_excel.xlsx
```

---

## Prerequisites

- A Databricks workspace with **Unity Catalog** enabled and permission to
  create the `dwh` catalog (or an existing catalog you can write to).
- **Databricks Runtime 14.3 LTS or newer** — the job cluster is pinned to
  `14.3.x-scala2.12` with `USER_ISOLATION` access mode.
- [**Databricks CLI v0.2xx+**](https://docs.databricks.com/dev-tools/cli/) with
  a configured profile (`databricks auth login --host <workspace-url>`).
- A secret scope named `dwh` holding the source and legacy credentials — no
  credentials are ever hard-coded in this repository:

  ```bash
  databricks secrets create-scope dwh
  # source SQL Server (`sample` database) — used by bronze ingestion
  databricks secrets put-secret dwh source_jdbc_host
  databricks secrets put-secret dwh source_jdbc_user
  databricks secrets put-secret dwh source_jdbc_password
  # legacy SQL Server DWH — used by the parity harness
  databricks secrets put-secret dwh legacy_jdbc_host
  databricks secrets put-secret dwh legacy_jdbc_user
  databricks secrets put-secret dwh legacy_jdbc_password
  ```

- The flat-file sources (`data_sources/transaction_csv.csv`,
  `data_sources/transaction_excel.xlsx`) uploaded to a Unity Catalog volume,
  e.g. `/Volumes/dwh/bronze/landing/`.

---

## Deploying the bundle

The project is packaged as a [Databricks Asset
Bundle](https://docs.databricks.com/dev-tools/bundles/). The workflow in
`databricks/workflows/dwh_etl_job.yml` is included under `resources.jobs`, so
deploying the bundle creates or updates the job.

```bash
# validate the bundle and every resource definition
databricks bundle validate -t dev

# deploy notebooks + job to the target workspace
databricks bundle deploy -t dev

# same for production
databricks bundle validate -t prod && databricks bundle deploy -t prod
```

---

## Running the workflow

```bash
# full refresh
databricks bundle run dwh_etl_job -t dev

# override job parameters
databricks bundle run dwh_etl_job -t dev -- --params load_mode=incremental,catalog=dwh
```

Job-level parameters: `environment`, `catalog`, `load_mode` (`full` |
`incremental`), `source_system`.

**Task graph** (enforced with `depends_on`):

```
ingest_sqlserver ─┐
                  ├─> build_dim_branch -> build_dim_account -> build_dim_customer -> build_fact_transaction ─┬─> analytics
ingest_files ─────┘                                                                                          └─> validation
```

The three dimension builds have no data dependency on each other and *could*
run in parallel. They are chained deliberately: the legacy runbook mandates
`DimBranch → DimAccount → DimCustomer → FactTransaction`, and preserving that
order keeps the Databricks run directly comparable to a legacy run during
parallel-run parity testing. The comment in the YAML records this, and the
dimensions can be flattened once cutover is signed off.

Operational settings baked into the job: shared job cluster (Photon,
autoscaling 2–8 workers), per-task retries with `min_retry_interval_millis`,
per-task and job-level timeouts, a run-duration health rule, email and webhook
failure notifications, and a **daily 02:00 Asia/Jakarta schedule that ships
`PAUSED`** — unpause it per environment after cutover sign-off.

---

## Analytics: the stored-procedure replacements

The two legacy T-SQL stored procedures are reimplemented as parameterised
Spark SQL in `databricks/analytics/`:

| Legacy procedure | Replacement | Parameters |
|------------------|-------------|------------|
| `sp_DailyTransaction` | `dwh.analytics.daily_transaction` — transaction count and total amount per calendar day | `start_date`, `end_date` |
| `sp_BalancePerCustomer` | `dwh.analytics.balance_per_customer` — current balance per active account = opening `balance` + Σ(`amount` signed by `transaction_type`) | `customer_name` (substring match) |

Query them directly, or re-run the parameterised notebooks:

```sql
SELECT * FROM dwh.analytics.daily_transaction
WHERE transaction_date BETWEEN DATE '2024-01-18' AND DATE '2024-01-20'
ORDER BY transaction_date;

SELECT * FROM dwh.analytics.balance_per_customer
WHERE upper(customer_name) LIKE '%SMITH%';
```

---

## Validation and cutover

`databricks/validation/parity_check.py` compares the lakehouse against the
legacy SQL Server DWH over JDBC and writes one row per check to
`dwh.analytics.parity_results`:

- per-table row counts for all three dimensions and the fact table,
- `SUM(amount)`, `COUNT(DISTINCT transaction_id)` and per-`transaction_type`
  count/amount on the fact table,
- `MIN`/`MAX(transaction_date)`,
- a row-level SHA-256 hash diff on the fact business key `transaction_id`,
  classifying every difference as `missing_in_databricks`, `missing_in_legacy`
  or `value_mismatch`.

Tolerances are configurable per run, and `fail_on_mismatch` controls whether a
breach fails the workflow (advisory during parallel run, enforcing after
cutover).

Cutover procedure, secret requirements and troubleshooting guidance:
**[`databricks/validation/README.md`](databricks/validation/README.md)**.

Summary of the cutover sequence:

1. Run the legacy Talend batch and the Databricks job for the same business date.
2. Let the `validation` task run in advisory mode (`fail_on_mismatch=false`).
3. Review `dwh.analytics.parity_results`; resolve every failing check.
4. Require three consecutive clean days before switching consumers over.
5. Set `fail_on_mismatch: "true"`, unpause the schedule, retire the Talend jobs.

---

## Legacy (Talend/SQL Server)

The original solution — built during a project-based internship with **ID/X
Partners** and **Rakamin Academy** — implemented the same star schema on
Microsoft SQL Server with Talend Open Studio for Data Integration as the ETL
engine:

- **Data warehouse:** a `DWH` SQL Server database with `DimCustomer`,
  `DimAccount`, `DimBranch` and `FactTransaction`, including PK/FK constraints
  — see [`sql_scripts/01_create_tables.sql`](sql_scripts/01_create_tables.sql).
- **Analytics:** two T-SQL stored procedures, `sp_DailyTransaction` and
  `sp_BalancePerCustomer` — see
  [`sql_scripts/02_create_procedures.sql`](sql_scripts/02_create_procedures.sql).
- **ETL:** four Talend jobs — `Load_DimBranch`, `Load_DimAccount`,
  `Load_DimCustomer` (multi-table joins in `tMap` plus uppercase cleansing) and
  `Load_FactTransaction` (`tUnite` across the SQL, CSV and Excel transaction
  streams, then `tUniqRow` deduplication on `transaction_id`). The exported job
  definitions are preserved as zip archives in
  [`talend_jobs/`](talend_jobs/); each contains the `.item` XML with the real
  component graph, `tMap` expressions and connection metadata, and remains the
  authoritative reference for the source-to-target mappings.
- **Sources:** 8 sources across 3 source types — the `sample` SQL Server
  database (restored from [`data_sources/sample.bak`](data_sources/)), a CSV
  export and an Excel workbook.
- **Runbook:** restore `sample.bak` in SSMS, execute the DDL script, import the
  Talend project, configure the `Sample_DB` and `DWH` connections, then run the
  four jobs in the order `Load_DimBranch → Load_DimAccount → Load_DimCustomer →
  Load_FactTransaction`, and finally deploy the stored procedures.

These artefacts are retained for provenance and for the parity harness; they
are no longer part of the running pipeline.

---

## License

Released under the MIT License — Copyright (c) 2025 Ryan Besto Saragih. See
[`LICENSE`](LICENSE) for the full text.
