# banking-etl (PySpark / Delta Lake)

PySpark port of the Talend Open Studio jobs and T-SQL stored procedures. One package
(`banking_etl`) runs unchanged on Databricks (wheel tasks), on EKS (Spark Operator /
`spark-submit`) and locally.

| Legacy (Talend / T-SQL)    | PySpark job (`banking_etl.jobs.*`) | Target                 | Write mode               |
|----------------------------|------------------------------------|------------------------|--------------------------|
| `Load_DimBranch`           | `load_dim_branch`                  | `dwh.dim_branch`       | Delta `MERGE` on PK      |
| `Load_DimAccount`          | `load_dim_account`                 | `dwh.dim_account`      | Delta `MERGE` on PK      |
| `Load_DimCustomer`         | `load_dim_customer`                | `dwh.dim_customer`     | Delta `MERGE` on PK      |
| `Load_FactTransaction`     | `load_fact_transaction`            | `dwh.fact_transaction` | `INSERT OVERWRITE` (truncate + insert, as in Talend) |
| `sp_DailyTransaction`      | `daily_transaction`                | result / optional table | read-only              |
| `sp_BalancePerCustomer`    | `balance_per_customer`             | result / optional table | read-only              |

## Layout

```
pyspark/
  config/config.yaml        # sources, warehouse location, write modes (env-var interpolated)
  src/banking_etl/
    config.py               # config loader, AWS Secrets Manager lookup
    schemas.py              # source schemas + warehouse DDL (MONEY -> DECIMAL(19,4), DATETIME -> TIMESTAMP)
    readers.py              # SQL Server JDBC, Excel, CSV (explicit schema, dd-MM-yyyy HH:mm:ss)
    transforms.py           # tMap joins / UPCASE, tUnite + tUniqRow, FK checks
    writers.py              # Delta CREATE IF NOT EXISTS, MERGE, INSERT OVERWRITE
    analytics.py            # daily_transaction(), balance_per_customer()
    jobs/                   # one entry point per Talend job / stored procedure
  tests/                    # unit tests + SQL Server end-to-end/parity tests
  Dockerfile                # linux/amd64 Spark image for EKS
```

## Behaviour notes

- **Customer cleansing**: `customer LEFT JOIN city ON city_id LEFT JOIN state ON state_id`;
  `CustomerName`, `Address`, `Gender` are upper-cased (the Talend tMap expressions).
- **Fact de-duplication**: the three sources are unioned and de-duplicated on
  `transaction_id`. Talend's tUniqRow kept the first row seen; here the winner is
  deterministic: SQL Server, then Excel, then CSV (file order within a source).
- **Foreign keys**: `FactTransaction` had FKs to `DimAccount`/`DimBranch`. Delta does not
  enforce them, so the fact job checks them: `FACT_ORPHAN_POLICY=reject` (default, log and
  drop), `fail` (abort the run) or `keep`. In the sample data transactions 23-25 reference
  accounts 22/23, which do not exist, and are rejected.
- **Analytics**: string comparisons (`'Deposit'`, `'active'`, `LIKE`) are case-insensitive
  and ignore trailing spaces, matching the legacy `SQL_Latin1_General_CP1_CI_AS` collation.
  The integration tests run both stored procedures on SQL Server and assert identical output.
- **Excel**: `EXCEL_ENGINE=pandas` (default) loads the workbook through Spark's
  `binaryFile` source (local, `s3a://`, `dbfs:/`, `/Volumes`) and parses it with
  pandas/openpyxl; `EXCEL_ENGINE=spark_excel` uses `com.crealytics.spark.excel` (add the
  `com.crealytics:spark-excel_2.12` library to the cluster/image).

## Configuration

`config/config.yaml` is interpolated with `${VAR}` / `${VAR:-default}`. Relative paths are
resolved against the config file. `BANKING_ETL_CONFIG` or `--config` selects another file.

| Variable | Purpose |
|----------|---------|
| `DWH_STORAGE` | `path` (Delta at `DWH_BASE_PATH/<table>`, e.g. `s3a://bucket/dwh`) or `table` (`DWH_CATALOG.DWH_SCHEMA.<table>`, Unity Catalog) |
| `DWH_CATALOG`, `DWH_SCHEMA`, `DWH_BASE_PATH` | Warehouse location (`DWH_SCHEMA` defaults to `dwh`) |
| `SOURCE_JDBC_URL`, `SOURCE_DB_SCHEMA` | SQL Server source |
| `SOURCE_DB_SECRET_ID`, `AWS_REGION` | AWS Secrets Manager secret with `{"username": ..., "password": ...}` (preferred) |
| `SOURCE_DB_USER`, `SOURCE_DB_PASSWORD` | Fallback when no secret id is set (local dev only) |
| `TRANSACTION_EXCEL_PATH`, `TRANSACTION_CSV_PATH` | Landing-zone files |
| `EXCEL_ENGINE` | `pandas` or `spark_excel` |
| `FACT_ORPHAN_POLICY` | `reject`, `fail` or `keep` |

## Local development

Requires Python 3.10+ and Java 11/17.

```bash
cd pyspark
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt -e .
ruff check . && ruff format --check .
pytest                                   # unit tests (Delta jars resolved from Maven)
```

Run the jobs against a local SQL Server restored from `data_sources/sample.bak`
(Delta tables land in `_local_warehouse/dwh` at the repo root):

```bash
docker run -d --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" \
  -p 1433:1433 -v "$PWD/../data_sources:/backup" mcr.microsoft.com/mssql/server:2022-latest
docker exec mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q \
  "RESTORE DATABASE sample FROM DISK='/backup/sample.bak' WITH MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf'"

export SOURCE_DB_USER=sa SOURCE_DB_PASSWORD="$MSSQL_SA_PASSWORD"
load_dim_branch && load_dim_account && load_dim_customer && load_fact_transaction
daily_transaction --start-date 2024-01-18 --end-date 2024-01-22
balance_per_customer --customer-name shelly

BANKING_ETL_IT=1 pytest tests/integration   # end-to-end + stored-procedure parity
```

Set `BANKING_ETL_JARS` to a comma-separated list of local jars (Delta, mssql-jdbc) to skip
Maven resolution.
