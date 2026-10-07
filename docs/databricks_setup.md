# Databricks workspace + Unity Catalog setup

Foundation for migrating the Talend + SQL Server banking DWH to Databricks
(Delta Lake, Unity Catalog, medallion bronze/silver/gold). Every other
migration job reads its names from `banking_etl.common.config.EnvConfig`
and assumes the objects below exist.

## What gets created

| Object | Default name | Purpose |
| --- | --- | --- |
| Schema | `migration_demo.banking_mig_bronze` | Raw copies: `sqlserver_<table>`, `file_transaction_excel`, `file_transaction_csv` |
| Schema | `migration_demo.banking_mig_silver` | Cleansed entities: `branch`, `account`, `customer`, `transaction` |
| Schema | `migration_demo.banking_mig_gold` | Star schema: `dim_branch`, `dim_account`, `dim_customer`, `fact_transaction` + analytics functions |
| Schema | `migration_demo.banking_mig_ops` | Run audit, rejects, data-quality results |
| Volume | `migration_demo.banking_mig_bronze.landing` | Raw files, at `/Volumes/migration_demo/banking_mig_bronze/landing/` with `transactions/csv/`, `transactions/excel/` and `sample_db/` pre-created |
| Grants | `account users` | `USE SCHEMA, SELECT, EXECUTE` on all four schemas, `READ VOLUME` on `landing`; `USE CATALOG` best-effort (see below) |

Everything is idempotent (`CREATE ... IF NOT EXISTS`, `COMMENT ON`, `GRANT`), so
re-running the setup is safe and also refreshes the schema comments.

## Files

| Path | What it is |
| --- | --- |
| `databricks/databricks.yml` | Asset Bundle: variables `catalog`, `schema_prefix`, `landing_volume`, `grant_principal`; `dev` target; `uc_setup` serverless job. Includes `resources/*.yml` so other jobs can be added without editing this file. |
| `databricks/setup/setup_unity_catalog.py` | Builds the statements from `EnvConfig` and applies them through a SQL warehouse (CLI) or a Spark session (notebook/tests). |
| `databricks/setup/setup_unity_catalog_notebook.py` | Notebook wrapper used by the bundle job / `jobs submit`; parameters come from widgets. |
| `databricks/setup/unity_catalog_setup.sql` | The rendered SQL for the default dev environment (generated with `--print-sql`; a test keeps it in sync). Paste it into the SQL editor if you prefer. |
| `databricks/src/banking_etl/common/config.py` | `EnvConfig`: catalog/prefix -> fully-qualified schema, table, volume and landing path names. |
| `databricks/tests/setup/` | Local pytest (PySpark 4 + delta-spark). |

## Running it

Auth uses the standard Databricks env vars (`DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` for the OAuth M2M service
principal).

```bash
# Preview the SQL (no workspace access needed)
python databricks/setup/setup_unity_catalog.py --print-sql

# Apply through a SQL warehouse (first warehouse, or --warehouse-id / DATABRICKS_WAREHOUSE_ID)
pip install databricks-sdk
python databricks/setup/setup_unity_catalog.py --catalog migration_demo --schema-prefix banking_mig_

# Or as a serverless job via the bundle (from databricks/)
databricks bundle validate -t dev
databricks bundle deploy -t dev && databricks bundle run -t dev uc_setup
```

The script prints a JSON report: each statement with `ok`/`skipped`, the landing
folders and the resulting `SHOW SCHEMAS`.

Tests:

```bash
cd databricks
pip install pyspark==4.0.1 delta-spark==4.0.0 pytest
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 pytest tests/setup
```

## Environments

An environment is a `(catalog, schema_prefix)` pair. Every job takes the same
four parameters and resolves names through `EnvConfig`, in this order:
explicit argument / job parameter > `BANKING_ETL_*` env var > default.

| Setting | Bundle variable | Env var | Default |
| --- | --- | --- | --- |
| Catalog | `catalog` | `BANKING_ETL_CATALOG` | `migration_demo` |
| Schema prefix | `schema_prefix` | `BANKING_ETL_SCHEMA_PREFIX` | `banking_mig_` |
| Landing volume | `landing_volume` | `BANKING_ETL_LANDING_VOLUME` | `landing` |
| Read principal | `grant_principal` | `BANKING_ETL_GRANT_PRINCIPAL` | `account users` |

Current environments:

- **dev** (bundle target `dev`, default): workspace `dbc-8bc9474f-40ae`,
  catalog `migration_demo`, prefix `banking_mig_`. Bundle `mode: development`,
  so deployed job names get a `[dev <user>]` prefix and schedules are paused.
- **Ticket scratch schemas**: while the migration tickets run in parallel,
  a ticket that needs upstream tables that don't exist yet seeds
  `migration_demo.banking_mig_t<N>` (`EnvConfig().ticket_schema(N)`) and never
  writes to another ticket's objects.

To add an environment (e.g. `staging`/`prod`), add a target to
`databricks.yml` that overrides `variables` (typically a dedicated catalog,
keeping the prefix) and `workspace.host`, then run the setup against it:

```yaml
targets:
  prod:
    mode: production
    workspace:
      host: https://<prod-workspace>.cloud.databricks.com
    variables:
      catalog: banking_prod
```

```python
from banking_etl.common.config import EnvConfig

cfg = EnvConfig.from_env()            # or EnvConfig.from_widgets(dbutils) in a notebook
cfg.table("gold", "fact_transaction") # migration_demo.banking_mig_gold.fact_transaction
cfg.landing_subpath("transactions", "csv")
```

## Workspace constraints (dev)

- Serverless only: job specs with `new_cluster` are rejected, so the bundle
  job and `jobs submit` runs have no cluster spec.
- The `DE-shared` service principal can create schemas/volumes in
  `migration_demo` but cannot create catalogs and has no `MANAGE` on the
  catalog. `GRANT USE CATALOG ... TO account users` is therefore marked
  optional and reported as `skipped` (PERMISSION_DENIED) instead of failing
  the run. A catalog owner needs to run it once for `account users` to browse
  the schemas.
- UC grants need account-level principals (`account users`); the workspace
  group `users` is rejected.
- Parallel sessions share the bundle root, so migration tickets validate with
  `databricks jobs submit` (notebooks under
  `/Workspace/Shared/banking_etl_migration_v2/ticket_<N>/`) or the SQL
  warehouse statement API rather than `bundle deploy`.
