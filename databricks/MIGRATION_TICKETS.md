# Migration tickets: SQL Server + Talend DWH → Databricks Lakehouse

Coordination tracker for the 10-ticket migration of `COG-GTM/banking-etl-warehouse` to Delta Lake +
Unity Catalog + Spark SQL + Databricks Workflows. **Tickets 1–2 are foundational** (UC namespace,
shared config, Silver/Gold contracts). They merge first, as the foundation PR. Tickets 3–10 run in
parallel as separate sessions on top of it.

Orchestrator session: https://devin-gtm.devinenterprise.com/sessions/88b1fbe4556d4614a048a15f6fa361e4
Ticket sessions (launched 2026-10-23):
[T3](https://devin-gtm.devinenterprise.com/sessions/a87534359a1042dd93e178586869606e) ·
[T4](https://devin-gtm.devinenterprise.com/sessions/95f8b4383f0c4f20a905da962c0038ec) ·
[T5](https://devin-gtm.devinenterprise.com/sessions/8ff0af30a01b465c9a8e20c753f53a5b) ·
[T6](https://devin-gtm.devinenterprise.com/sessions/d305ce1357dd47059212182842355343) ·
[T7](https://devin-gtm.devinenterprise.com/sessions/12c04aa9e6db4748ba4cc16f53dc1975) ·
[T8](https://devin-gtm.devinenterprise.com/sessions/1be8b79073ec4284ac6bc1034504749a) ·
[T9](https://devin-gtm.devinenterprise.com/sessions/ef95231284f94f07841a3fa3b018a328) ·
[T10](https://devin-gtm.devinenterprise.com/sessions/a6c69998fad14272a73e889389dfb949)
Foundation branch: `devin/1792762652-databricks-lakehouse-foundation`. Ticket branches are cut from it
and their PRs target it.

## Status board

| # | Ticket | Depends on | Owned paths (under `databricks/`) | Status | Owning session | PR |
|---|--------|-----------|------------------------------------|--------|----------------|----|
| 1 | Unity Catalog & environment setup | none | `config/`, `databricks.yml`, `src/banking_dwh/common/`, `notebooks/00_setup/00_config.py`, `01_setup_unity_catalog.py`, `ddl/01_unity_catalog_setup.sql` | Done (validated live, dev) | orchestrator | foundation PR |
| 2 | Delta DDL for star schema + RI DQ | 1 | `src/banking_dwh/contracts/`, `src/banking_dwh/dq/checks.py`, `notebooks/00_setup/02_*`, `03_*`, `ddl/02_*`, `ddl/03_*` | Done (validated live, dev) | orchestrator | foundation PR |
| 3 | SQL Server extraction (JDBC) → Bronze | 1, 2 | `src/banking_dwh/bronze/sqlserver.py`, `notebooks/bronze/01_*`, `tests/test_t3_*` | Not started | — | — |
| 4 | CSV Auto Loader → Bronze | 1, 2 | `src/banking_dwh/bronze/csv_autoloader.py`, `notebooks/bronze/02_*`, `tests/test_t4_*` | Not started | — | — |
| 5 | Excel → Bronze | 1, 2 | `src/banking_dwh/bronze/excel.py`, `notebooks/bronze/03_*`, `tests/test_t5_*` | Not started | — | — |
| 6 | `dim_branch` + `dim_account` Silver MERGE | 1, 2 (reads T3 Bronze contract) | `src/banking_dwh/silver/dim_branch.py`, `dim_account.py`, `notebooks/silver/01_*`, `02_*`, `tests/test_t6_*` | Not started | — | — |
| 7 | `dim_customer` Silver load (customer+city+state, uppercase) | 1, 2 (reads T3 Bronze contract) | `src/banking_dwh/silver/dim_customer.py`, `notebooks/silver/03_*`, `tests/test_t7_*` | Not started | — | — |
| 8 | `fact_transaction` Silver load (union + dedup + MERGE) | 1, 2 (reads T3/T4/T5 Bronze contracts) | `src/banking_dwh/silver/fact_transaction.py`, `notebooks/silver/04_*`, `tests/test_t8_*` | PR open | https://devin-gtm.devinenterprise.com/sessions/1be8b79073ec4284ac6bc1034504749a | https://github.com/COG-GTM/banking-etl-warehouse/pull/77 |
| 9 | Gold analytics (replace 2 stored procedures) | 1, 2 (reads Silver contract) | `src/banking_dwh/gold/`, `notebooks/gold/`, `ddl/04_gold_*`, `tests/test_t9_*` | Not started | — | — |
| 10 | Workflow, reconciliation DQ, runbook | 1, 2 (wires the notebook paths named above) | `resources/jobs/`, `notebooks/orchestration/`, `src/banking_dwh/orchestration/`, `src/banking_dwh/dq/reconciliation.py`, `fixtures/parity/`, `tests/test_t10_*`, root `README.md` "How to Run" | Not started | — | — |

Status values: Not started → In progress → PR open → Done. Each ticket session updates **only its own row**
(and appends to the log at the bottom), so concurrent PRs don't conflict beyond that one line.

Dependency graph: `T1 → T2 → {T3 … T10}`. T3–T10 code against the Bronze/Silver/Gold contracts in
`src/banking_dwh/contracts/tables.py` rather than each other's code, so they can all start at once. Each ticket
tests locally with fixtures that match the contract. Live runs use the ticket's own scratch schemas
(`cfg.ticket_schema(N)` → `migration_demo.bdwh_t<N>`, or `--schema_prefix bdwh_t<N>_`) so no ticket waits on
or overwrites another. The end-to-end run on the shared dev namespace is T10's job once 3–9 land.

## Repo facts (verified on `main` @ 057ca5d)

The original plan assumed some inputs were missing. They are committed:

* `talend_jobs/Load_Dim{Branch,Account,Customer}.zip`, `Load_FactTransaction.zip` contain the Talend `.item`
  job XML. Port behaviour from those, not from the README:
  * `Load_DimBranch` / `Load_DimAccount`: straight column copies, INSERT, `DIE_ON_ERROR=false`.
  * `Load_DimCustomer`: tMap with `customer` main, `city` and `state` lookups with no `innerJoin` set, so both are
    **LEFT OUTER**. `UPCASE` applies to name, address and gender only.
  * `Load_FactTransaction`: SQL `transaction_db` + Excel + CSV → `tUnite` → `tUniqRow` on `transaction_id` (first
    occurrence wins, in stream order SQL, Excel, CSV) → TRUNCATE + INSERT into `FactTransaction`.
* `data_sources/sample.bak`: SQL Server 2022 backup of DB `sample`: `branch` 5, `state` 9, `city` 52,
  `customer` 20, `account` 21, `transaction_db` 10 rows. `balance`/`amount` are `int`, dates are `datetime2`,
  `customer.age` is `varchar(3)`.
* `data_sources/transaction_excel.xlsx`: Sheet1, 7 rows. `data_sources/transaction_csv.csv`: 12 rows,
  `dd-MM-yyyy HH:mm:ss` dates.

## Shared conventions (from Tickets 1–2)

* **Names:** never hard-code catalog or schema. Use `cfg.table("silver", "dim_account")`, `cfg.schema("bronze")`,
  `cfg.landing_path("csv", "transaction")` and `cfg.checkpoint_path("csv_transaction")` from
  `banking_dwh.common.config.EnvConfig`. Targets live in `config/environments.yml`:
  * `prod` → `banking_dwh.{bronze,silver,gold,ops}`
  * `dev` → `migration_demo.bdwh_{bronze,silver,gold,ops}`. The dev workspace can't create catalogs.
* **Notebooks:** start with `%run ../00_setup/00_config`. That gives `cfg` plus the `target`, `catalog` and
  `schema_prefix` widgets. Return a JSON summary with `dbutils.notebook.exit`.
* **Tables:** create them from the contracts with `create_table_sql(cfg, CONTRACT)` from
  `banking_dwh.contracts.ddl`. Don't hand-write DDL. A table you need that has no contract yet: add it to
  `contracts/tables.py` in your PR.
* **Bronze tables** (contracts `BRONZE_*`): source columns plus `_source_system`, `_source_file`,
  `_ingested_at`, `_batch_id`, `_rescued_data`.
* **Silver tables** add `_loaded_at`.
* **Types:** `MONEY`/`int` money → `DECIMAL(19,4)`. `DATETIME`/`datetime2` → `TIMESTAMP`. `DATE` → `DATE`.
  `VARCHAR` → `STRING`. `INT` → `INT`.
* **Keys:** PK/FK constraints are informational in UC. `banking_dwh.dq.checks` enforces them as `pk_not_null.*`,
  `pk_unique.*`, `FK_FactTransaction_DimAccount` and `FK_FactTransaction_DimBranch`, with results in
  `ops.dq_results`.
* **Compute:** the dev workspace is serverless-only. Jobs use serverless notebook tasks with the
  environment in `config/compute/serverless_environment.json`. `config/compute/job_cluster.json` is for
  classic-compute workspaces.
* **Secrets:** the SQL Server connection comes from secret scope `banking-etl-sqlserver`, keys
  `jdbc-host|port|database|user|password`, via `banking_dwh.common.config.jdbc_options`.
* **Tests:** `cd databricks && pytest`. Local Spark 4.0.1 + Delta 4.0.0 on JDK 17/21 (`requirements-dev.txt`,
  `tests/conftest.py`). Every ticket adds `tests/test_t<N>_*.py`, and the full suite must stay green.
* **Live validation:** the jobs service principal can't use the SQL warehouse (statement API returns "no permission"), so validate through notebooks. Import `src/`, `config/` and `notebooks/` to
  `/Workspace/Shared/banking_dwh_migration/ticket_<N>/` and `databricks jobs submit` a serverless notebook run
  against your ticket's scratch prefix. No `bundle deploy` from ticket sessions, because they share a bundle root.

## Log

* 2026-10-23: T1 and T2 implemented on the foundation branch. Local suite 29/29. Live serverless run on
  `migration_demo.bdwh_*` created the namespace, volumes and star schema (all 13 tables, PK/FK constraints visible in `information_schema`), and the DQ gate passed. Run: `jobs submit` 932701605587220.
* 2026-10-23: T8 PR #77 open. Local suite 38/38. Live run 797540395994377 on `migration_demo.bdwh_t8_*`: union 29 -> dedup 25 -> 22 loaded, 3 FK_FactTransaction_DimAccount orphans quarantined in `ops.fact_transaction_rejects`; rerun MERGE 0/0/0.
