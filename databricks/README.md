# Databricks migration (PySpark + Delta Lake)

PySpark/Delta replacement for the four Talend jobs and the two T-SQL stored procedures
described in the top-level [README](../README.md). The original `sql_scripts/`,
`data_sources/` and `talend_jobs/` are unchanged.

| Original | Databricks replacement |
|---|---|
| `sql_scripts/01_create_tables.sql` (DWH database) | `ddl/01_create_tables.sql` (`dwh` schema, Delta) + optional `ddl/02_informational_constraints_uc.sql` |
| Talend `Load_DimBranch` | `jobs/load_dim_branch.py` |
| Talend `Load_DimAccount` | `jobs/load_dim_account.py` |
| Talend `Load_DimCustomer` | `jobs/load_dim_customer.py` |
| Talend `Load_FactTransaction` | `jobs/load_fact_transaction.py` |
| `sp_DailyTransaction` | `analytics/daily_transaction.py` |
| `sp_BalancePerCustomer` | `analytics/balance_per_customer.py` |
| Manual run order | `workflows/etl_job.json` (Databricks Workflow) |

```
databricks/
├── ddl/                       Delta DDL (dwh.dim_account, dim_branch, dim_customer, fact_transaction)
├── jobs/
│   ├── common.py              params, secrets, JDBC reader, Delta merge/overwrite writer
│   ├── create_tables.py       runs ddl/01_create_tables.sql against the configured catalog/schema
│   └── load_*.py              one loader per Talend job
├── analytics/                 stored-procedure equivalents (parameterised Spark SQL)
├── workflows/etl_job.json     Jobs API 2.1 definition
└── local/                     local Spark run + parity check against the original stored procs
```

## Type mapping

| SQL Server | Delta |
|---|---|
| `MONEY` | `DECIMAL(19,4)` |
| `DATETIME` | `TIMESTAMP` |
| `VARCHAR(n)` | `STRING` |
| `INT`, `DATE` | `INT`, `DATE` |
| `PRIMARY KEY` | `NOT NULL` column, plus an optional Unity Catalog `PRIMARY KEY ... NOT ENFORCED` |
| `FOREIGN KEY` | optional Unity Catalog `FOREIGN KEY ... NOT ENFORCED`; the fact loader enforces it (see below) |

Table names change from `DimAccount` to `dwh.dim_account` and so on. Column names stay the
same (`AccountID`, `CustomerName`, ...), so existing queries only need new table names.

## Setup

### 1. Secrets for the source SQL Server

Create a secret scope (default name `banking-etl`) holding the JDBC credentials:

```bash
databricks secrets create-scope banking-etl
databricks secrets put-secret banking-etl sqlserver-host       # e.g. sqlprod.example.com
databricks secrets put-secret banking-etl sqlserver-user
databricks secrets put-secret banking-etl sqlserver-password
```

The host can also be passed as the `jdbc_host` parameter instead of a secret. Credentials are
only ever read with `dbutils.secrets.get`. Nothing is hard-coded.

### 2. Cluster and libraries

* Databricks Runtime 14.3 LTS or newer (Spark 3.5; the analytics use named SQL parameters and
  `try_to_timestamp`). The workflow uses 15.4 LTS.
* The SQL Server JDBC driver (`com.microsoft.sqlserver.jdbc.SQLServerDriver`) is bundled with
  Databricks Runtime, so nothing extra is needed for JDBC.
* For the `.xlsx` transaction source install the Maven library
  `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` on the cluster (the workflow attaches it to
  the `load_fact_transaction` task).
* The cluster needs network access to the SQL Server host/port (VNet/VPC peering or private link,
  plus a firewall rule).

### 3. Upload the file sources

`data_sources/transaction_csv.csv` is in this repo. The Excel file
(`transaction_excel.xlsx`) is also in `data_sources/`, but in production it will usually come
from somewhere else, so its path is a parameter. Upload both to a Unity Catalog volume (or DBFS)
and pass the paths:

```bash
databricks fs cp data_sources/transaction_csv.csv   dbfs:/Volumes/main/banking_raw/landing/
databricks fs cp data_sources/transaction_excel.xlsx dbfs:/Volumes/main/banking_raw/landing/
```

### 4. Create the tables

Run `jobs/create_tables.py` (first task of the workflow), or open `ddl/01_create_tables.sql`
in the SQL editor after `USE CATALOG <catalog>`. On Unity Catalog you can also run
`ddl/02_informational_constraints_uc.sql` to declare the PK/FK relationships.

## Parameters

Every script reads its parameters from, in order: a Databricks widget / notebook parameter,
a `--name value` argument (spark_python_task), an upper-cased environment variable, then the
default.

| Parameter | Default | Used by |
|---|---|---|
| `catalog` | *(empty: current catalog)* | all |
| `target_schema` | `dwh` | all |
| `secret_scope` | `banking-etl` | loaders |
| `jdbc_host` | *(empty: read secret `jdbc_host_key`)* | loaders |
| `jdbc_host_key` / `jdbc_user_key` / `jdbc_password_key` | `sqlserver-host` / `sqlserver-user` / `sqlserver-password` | loaders |
| `jdbc_port` | `1433` | loaders |
| `jdbc_database` | `sample` | loaders |
| `jdbc_source_schema` | `dbo` | loaders |
| `jdbc_extra_options` | `encrypt=true;trustServerCertificate=true` | loaders |
| `write_mode` | `merge` (upsert on the primary key) or `overwrite` | loaders |
| `upper_columns` | `CustomerName,Address,CityName,StateName,Gender` | `load_dim_customer` |
| `source_transaction_table` | `transaction_db` | `load_fact_transaction` |
| `csv_path` | *(empty: skip source)* | `load_fact_transaction` |
| `excel_path` | *(empty: skip source)* | `load_fact_transaction` |
| `excel_data_address` | `'Sheet1'!A1` | `load_fact_transaction` |
| `reject_orphans` | `true` | `load_fact_transaction` |
| `start_date`, `end_date` | `2024-01-18`, `2024-01-20` | `daily_transaction` |
| `customer_name` | `Shelly` | `balance_per_customer` |
| `output_table` | *(empty: just display)* | analytics |

## Running

### As a Databricks Workflow

1. Add this repo as a Git folder (Repos) in the workspace.
2. In `workflows/etl_job.json`, replace `<user>` in the `python_file` paths with the Git folder
   location, and change `node_type_id` for your cloud (`i3.xlarge` is AWS; use for example
   `Standard_DS3_v2` on Azure).
3. Create and run the job:

```bash
databricks jobs create --json @databricks/workflows/etl_job.json
databricks jobs run-now <job_id> --json '{"job_parameters": {"catalog": "main", "jdbc_host": "sqlprod.example.com"}}'
```

Task graph: `create_tables` → `load_dim_branch` → `load_dim_account` → `load_dim_customer`
→ `load_fact_transaction` → (`daily_transaction`, `balance_per_customer`). This is the same
order as the Talend run sequence. The two analytics tasks are optional and can be removed.

### Single scripts

Each file can be run on its own as a Python file task, or opened from a Git folder and
run as a notebook (widgets are created automatically). For example:

```
jobs/load_fact_transaction.py --catalog main --csv_path /Volumes/main/banking_raw/landing/transaction_csv.csv \
    --excel_path /Volumes/main/banking_raw/landing/transaction_excel.xlsx
analytics/daily_transaction.py --catalog main --start_date 2024-01-18 --end_date 2024-01-20
analytics/balance_per_customer.py --catalog main --customer_name Shelly
```

The analytics are also importable from a notebook:

```python
from daily_transaction import daily_transaction
display(daily_transaction("2024-01-18", "2024-01-20"))
```

### Locally (no Databricks)

`local/run_local.py` runs the whole pipeline on local Spark + Delta. `local/parity_check.py`
then loads the Delta tables into a SQL Server `DWH` built from `sql_scripts/` and diffs both
stored procedures against the PySpark versions.

```bash
# SQL Server with data_sources/sample.bak restored as database "sample"
pip install pyspark==3.5.3 delta-spark==3.2.1 setuptools    # Java 17 required
export JDBC_HOST=localhost SQLSERVER_USER=sa SQLSERVER_PASSWORD=...
python databricks/local/run_local.py          # all steps, or pass specific script paths
python databricks/local/parity_check.py
```

## Behaviour notes

These come from the documented behaviour and the job definitions in `talend_jobs/*.zip`.

* **Dimensions**: the loaders do straight column mappings from `dbo.branch` / `dbo.account`.
  `write_mode=merge` (default) makes reruns idempotent; `overwrite` replaces all rows, matching
  Talend's truncate-and-insert.
* **DimCustomer**: `customer` LEFT JOIN `city` LEFT JOIN `state` (the Talend tMap lookups are
  not inner joins). `CustomerName`, `Address`, `CityName` and `StateName` are upper-cased as
  requested. The exported Talend tMap also upper-cases `Gender` but not city/state, so `Gender`
  is in the default `upper_columns` too. Change the parameter to match exactly.
  `age` is `VARCHAR` in the source and is cast to `INT`.
* **FactTransaction**:
  * Sources are unioned with `unionByName` in Talend's tUnite order (SQL Server, Excel, CSV).
    CSV dates are `dd-MM-yyyy HH:mm:ss`.
  * In the restored `sample.bak` the SQL Server transaction table is `dbo.transaction_db`,
    not `transaction`. Override with `source_transaction_table` if yours differs.
  * Deduplication replaces tUniqRow, which keeps the first row it sees. IDs 6/7 and 14/15
    appear in several sources (6 and 7 with different dates), so rows whose `TransactionID`
    already came from a higher-priority source are removed first and then `dropDuplicates`
    runs. A bare `dropDuplicates` would keep an arbitrary copy.
  * Delta does not enforce the original FKs. Talend's insert ran with die-on-error off, so
    SQL Server silently rejected rows with an unknown `AccountID`/`BranchID`. The loader does
    the same (`reject_orphans=true`) and logs the rejected IDs. With the sample data:
    29 rows unioned → 25 after dedupe → 22 loaded (transactions 23–25 reference account 22/23,
    which do not exist).
  * The loader fails fast if `dim_account`/`dim_branch` are empty, and the workflow runs the
    dimensions first.
* **sp_BalancePerCustomer**: SQL Server's default collation is case-insensitive, so the
  `LIKE` filter and `Status = 'active'` are made case-insensitive (`UPPER`/`LOWER`) to return
  the same rows.
* `sql_scripts/02_create_procedures.sql` puts a `PRINT` before each `CREATE PROCEDURE` in the
  same batch, which SQL Server rejects. `local/parity_check.py` strips those `PRINT` lines
  when building the reference DWH. The script itself is left as is.

## Informatica

No Informatica workflows, mappings or exports (`.xml`, `.ipc`, PowerCenter repository
exports) exist in this repository. Only the Talend jobs above were migrated. If Informatica
workflows exist elsewhere they must be supplied separately and migrated on their own.
