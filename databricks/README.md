# Databricks lakehouse migration

This directory holds the Databricks replacement for the legacy stack described in the
repository root `README.md`: a SQL Server `DWH` star schema loaded by four Talend Open
Studio jobs, with two T-SQL stored procedures for reporting.

The target is a Unity Catalog lakehouse using a medallion (bronze / silver / gold)
layout, deployed as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/).

## Layout

```
databricks/
  ddl/         Delta DDL — 00_catalog_schemas.sql (this ticket) + gold star-schema tables
  bronze/      Raw ingestion notebooks (SQL Server JDBC, CSV, Excel)
  silver/      Cleansed / conformed dimension builds
  gold/        Fact table build
  analytics/   Stored-procedure replacements (parameterized Spark SQL)
  workflows/   Databricks Workflow (job) definitions, picked up by databricks.yml
  conf/        Per-environment config (dev.yml, prod.yml)
databricks.yml Asset Bundle definition (repo root)
docs/          Specs and mapping documents (repo root)
```

## Medallion layers

| Layer | Schema | Contents | Legacy equivalent |
| --- | --- | --- | --- |
| Bronze | `dwh.bronze` | Raw landed copies of each source object, one table per source: `customer`, `account`, `branch`, `city`, `state`, `transaction_db` (JDBC from the `sample` database), `transaction_csv`, `transaction_excel`. No business logic. | Talend source components / staging area |
| Silver | `dwh.silver` | Cleansed, conformed dimensions: `dim_branch`, `dim_account`, `dim_customer` (customer joined to city and state, text fields upper-cased). | `Load_DimBranch`, `Load_DimAccount`, `Load_DimCustomer` |
| Gold | `dwh.gold` | `fact_transaction` — the three transaction streams unioned and de-duplicated on `transaction_id`, conformed to the dimension keys. | `Load_FactTransaction` (`tUnite` + `tUniqRow`) |
| Analytics | `dwh.analytics` | Parameterized Spark SQL replacements for `sp_DailyTransaction` and `sp_BalancePerCustomer`. | Stored procedures in `sql_scripts/02_create_procedures.sql` |

Data flows strictly bronze → silver → gold → analytics; nothing reads upstream of its
own layer's inputs.

## Conventions

**Naming.** Unity Catalog three-level names everywhere: `<catalog>.<schema>.<table>`,
catalog `dwh` (`dwh_dev` in the dev target). Tables, columns and file names are
`snake_case`. Bronze tables are named after the source object (`dwh.bronze.customer`,
`dwh.bronze.transaction_csv`); silver dimensions are `dim_<entity>`; the gold fact is
`fact_transaction`.

**Type mapping** from the legacy T-SQL DDL (`sql_scripts/01_create_tables.sql`):

| T-SQL | Delta / Spark |
| --- | --- |
| `MONEY` | `DECIMAL(19,4)` |
| `DATETIME` | `TIMESTAMP` |
| `DATE` | `DATE` |
| `VARCHAR(n)` | `STRING` |
| `INT` | `INT` |

**Notebooks.** PySpark notebooks are checked in as `.py` files in Databricks notebook
source format — a `# Databricks notebook source` first line and `# COMMAND ----------`
cell separators. Python 3 / PySpark 3.5 API.

**Parameters and secrets.** Notebook parameters come from `dbutils.widgets`; catalog,
schema and source locations are passed in rather than hard-coded. Credentials are never
committed — they are read at runtime with
`dbutils.secrets.get(scope="dwh", key=...)`, using the keys listed in
`databricks/conf/<env>.yml`.

## Provisioning Unity Catalog

`ddl/00_catalog_schemas.sql` is idempotent (`CREATE ... IF NOT EXISTS`) and creates the
catalog plus the four schemas, each with a `COMMENT`. It takes two parameters:

- `${catalog}` — catalog name (`dwh`, or `dwh_dev` in the dev target)
- `${storage_root}` — cloud storage URL for `MANAGED LOCATION`

The `MANAGED LOCATION` clauses are commented out on purpose: Unity Catalog requires a
pre-existing external location and storage credential for that path. Either create the
external location and uncomment them (supplying `storage_root` in
`databricks/conf/<env>.yml`), or leave them off and inherit the metastore's default root
storage.

Run it from a SQL warehouse or notebook, e.g.:

```bash
databricks sql query --file databricks/ddl/00_catalog_schemas.sql \
  --parameter catalog=dwh --parameter storage_root=""
```

Creating a catalog requires `CREATE CATALOG` on the metastore.

## Secret scope

One scope, `dwh`, holds the source-system credentials:

```bash
databricks secrets create-scope dwh
databricks secrets put-secret dwh sqlserver_username
databricks secrets put-secret dwh sqlserver_password
```

## Deploying the bundle

```bash
# from the repository root
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev <job_name>
```

Targets are `dev` (default, `mode: development`, catalog `dwh_dev`, single-worker
cluster) and `prod` (`mode: production`, catalog `dwh`, Photon, shared root path and
group permissions). Bundle variables — `catalog`, `storage_root`, `secret_scope`,
`source_jdbc_url`, `source_files_root`, `job_cluster` — can be overridden per target or
on the command line with `--var`. The job cluster is DBR 14.3 LTS
(`14.3.x-scala2.12`) with `data_security_mode: SINGLE_USER`, which is
Unity-Catalog-enabled.

## Local development

```bash
pip install -r requirements.txt
ruff check .
ruff format --check .
pytest
```

`ruff` is configured in `pyproject.toml` at the repository root and understands the
notebook-source style (long magic comment lines, `dbutils`/`spark`/`display` injected by
the runtime).
