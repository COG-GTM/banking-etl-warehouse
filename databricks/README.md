# banking_dwh on Databricks

This directory holds the Lakehouse port of the SQL Server `DWH` + Talend jobs that live in the repo root. Ticket status
and ownership are tracked in [`MIGRATION_TICKETS.md`](MIGRATION_TICKETS.md). The end-to-end runbook ships with Ticket 10.

| Path | Contents |
|------|----------|
| `config/environments.yml` | Targets. `prod` → `banking_dwh.{bronze,silver,gold,ops}`. `dev` → `migration_demo.bdwh_*`. Also holds the volumes, landing folders and the SQL Server secret scope. |
| `config/compute/` | Configs for the serverless job environment, a classic job cluster, a cluster policy and the SQL warehouse. |
| `src/banking_dwh/` | Python package. `common/` holds config and UC setup. `contracts/` holds the table contracts and DDL generator. `dq/` holds the checks. `bronze|silver|gold|orchestration/` are filled in by Tickets 3–10. |
| `notebooks/00_setup/` | `00_config`: shared config, `%run` from every notebook. `01_setup_unity_catalog`. `02_create_star_schema`. `03_dq_star_schema`. |
| `ddl/` | Generated prod DDL, for review and for SQL-only deployment. Regenerate with `python -m banking_dwh.contracts.ddl` (see below). |
| `resources/jobs/` | Databricks Workflow definitions, included by `databricks.yml` (Ticket 10). |
| `tests/` | Local Spark 4 + Delta tests. |

## Naming conventions

* Catalog per environment, with one schema per medallion layer plus `ops`. A dev target can't create catalogs,
  so it uses a schema prefix instead (`bdwh_silver`). Scratch schemas per ticket: `cfg.ticket_schema(N)`.
* Tables are snake_case. Legacy `DimAccount` → `silver.dim_account`. `FactTransaction` → `silver.fact_transaction`.
  Bronze tables are `sqlserver_<source table>` or `file_transaction_<csv|excel>`.
* Lineage columns are `_`-prefixed (`_ingested_at`, `_source_file`, `_loaded_at`, ...).
* Identifiers coming from widgets or config are checked against `^[A-Za-z_][A-Za-z0-9_]*$` before they reach SQL.

## Setup

```bash
# Unity Catalog namespace + volumes + star schema (any target)
databricks workspace import-dir databricks /Workspace/Shared/banking_dwh/bundle/files --overwrite
# then run notebooks/00_setup/01_setup_unity_catalog, 02_create_star_schema with target=prod|dev
# or deploy with the bundle: databricks bundle deploy -t prod

# Local tests (JDK 17 or 21)
pip install -r requirements-dev.txt
pytest

# Regenerate the reviewed DDL after changing contracts/tables.py
cd src && python -m banking_dwh.contracts.ddl --target prod > ../ddl/02_star_schema.sql \
       && python -m banking_dwh.contracts.ddl --target prod --what bronze > ../ddl/03_bronze_tables.sql
```
