# PySpark ETL jobs and reports

PySpark reimplementation of the four Talend jobs in `../talend_jobs/` and the two T-SQL
procedures in `../sql_scripts/02_create_procedures.sql`. It reads the `sample` source
database, `data_sources/transaction_csv.csv` and `data_sources/transaction_excel.xlsx`,
and writes the existing `DWH` star schema (`../sql_scripts/01_create_tables.sql`) over JDBC.
Column names and types match the DWH DDL.

```
pyspark_jobs/
├── common/
│   ├── config.py            env-driven config (source DB, DWH DB, file paths, Spark/JDBC packages)
│   ├── spark_session.py     shared SparkSession factory (SQL Server JDBC driver, session time zone)
│   ├── io.py                JDBC read/write, CSV/Excel reads, report destinations
│   └── jobs.py              logging + table load semantics (append / overwrite)
├── load_dim_branch.py       Load_DimBranch       branch                      -> DimBranch
├── load_dim_account.py      Load_DimAccount      account                     -> DimAccount
├── load_dim_customer.py     Load_DimCustomer     customer + city + state     -> DimCustomer
├── load_fact_transaction.py Load_FactTransaction transaction_db + Excel + CSV -> FactTransaction
├── run_all.py               runs the four jobs in README order
├── reports/
│   ├── daily_transaction.py     replaces sp_DailyTransaction
│   └── balance_per_customer.py  replaces sp_BalancePerCustomer
├── bin/submit.sh            spark-submit wrapper that adds the JDBC driver
└── tests/                   unit tests (local Spark) + SQL Server parity tests
```

## Requirements

- Java 8/11/17 and Python 3.9+.
- `pip install -r pyspark_jobs/requirements.txt` (pyspark 3.5, pandas/openpyxl for the Excel feed).
- **SQL Server JDBC driver** `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11`. It is a JVM
  dependency, not a pip package:
  - `pyspark_jobs/bin/submit.sh` passes it to `spark-submit --packages` for you;
  - with plain `spark-submit`, add `--packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11`
    (or `--jars /path/to/mssql-jdbc-12.8.1.jre11.jar` on offline clusters);
  - with `python pyspark_jobs/run_all.py`, the SparkSession factory sets `spark.jars.packages`.
- The `sample` database restored from `data_sources/sample.bak`, and `DWH` created with
  `sql_scripts/01_create_tables.sql` (the jobs load into the existing tables, keeping PKs/FKs).

## Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `SOURCE_DB_HOST` / `SOURCE_DB_PORT` | `localhost` / `1433` | Source SQL Server |
| `SOURCE_DB_NAME` | `sample` | Source database |
| `SOURCE_DB_USER` / `SOURCE_DB_PASSWORD` | – | Source credentials |
| `SOURCE_DB_ENCRYPT` / `SOURCE_DB_TRUST_SERVER_CERTIFICATE` | `true` / `false` | JDBC TLS settings (set trust to `true` for self-signed dev servers) |
| `SOURCE_JDBC_URL` | – | Full JDBC URL, overrides host/port/name/TLS |
| `DWH_DB_HOST`, `DWH_DB_PORT`, `DWH_DB_USER`, `DWH_DB_PASSWORD`, `DWH_DB_ENCRYPT`, `DWH_DB_TRUST_SERVER_CERTIFICATE` | same as `SOURCE_DB_*` | Target SQL Server |
| `DWH_DB_NAME` | `DWH` | Target database |
| `DWH_JDBC_URL` | – | Full JDBC URL for the DWH |
| `DATA_SOURCES_DIR` | `<repo>/data_sources` | Base directory for the file feeds |
| `TRANSACTION_CSV_PATH` | `$DATA_SOURCES_DIR/transaction_csv.csv` | CSV transactions (any Spark path, e.g. `abfss://`, `s3a://`) |
| `TRANSACTION_EXCEL_PATH` | `$DATA_SOURCES_DIR/transaction_excel.xlsx` | Excel transactions |
| `TRANSACTION_EXCEL_SHEET` | first sheet | Excel sheet name |
| `EXCEL_READER` | `pandas` | `pandas` (driver-side openpyxl read) or `crealytics` (`com.crealytics.spark.excel`) |
| `CSV_TIMESTAMP_FORMAT` | `dd-MM-yyyy HH:mm:ss` | `transaction_date` format in the CSV (Talend pattern) |
| `SPARK_JDBC_PACKAGES` | `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11` | Maven coordinate of the JDBC driver |
| `SPARK_JDBC_JARS` | – | Local driver jar(s); replaces `SPARK_JDBC_PACKAGES` |
| `SPARK_EXCEL_PACKAGE` | `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` | Used when `EXCEL_READER=crealytics` |
| `SPARK_JARS_REPOSITORIES` | – | Extra Maven repositories / mirror |
| `SPARK_MASTER` | – | e.g. `local[*]`, `yarn` |
| `SPARK_SQL_TIMEZONE` | JVM default | Spark session time zone (kept equal to the JVM zone so `to_date(TransactionDate)` matches SQL Server) |

## Running the ETL

From the repository root:

```bash
export SOURCE_DB_HOST=localhost SOURCE_DB_USER=sa SOURCE_DB_PASSWORD=...   # DWH_* default to these

# All four jobs in order: DimBranch -> DimAccount -> DimCustomer -> FactTransaction
pyspark_jobs/bin/submit.sh pyspark_jobs/run_all.py

# Rebuild the whole DWH (empties FactTransaction, then the dimensions, then reloads)
pyspark_jobs/bin/submit.sh pyspark_jobs/run_all.py --full-refresh

# A single job
pyspark_jobs/bin/submit.sh pyspark_jobs/load_dim_customer.py [--mode append|overwrite]

# Equivalent plain spark-submit
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 pyspark_jobs/run_all.py
```

On a cluster in `cluster` deploy mode, ship the package with
`cd pyspark_jobs/.. && zip -r pyspark_jobs.zip pyspark_jobs -x '*/tests/*'` and add
`--py-files pyspark_jobs.zip`. The Excel and CSV paths must then be reachable from the cluster.

### Load semantics (Talend parity)

| Job | Talend output | PySpark default |
|---|---|---|
| Dimensions | `CREATE_IF_NOT_EXISTS` + `INSERT`, die-on-error off (duplicate keys rejected silently) | `--mode append`: insert rows whose key is not in the table yet, skip existing keys. Re-runs are idempotent. `--mode overwrite`: `DELETE` + insert (SQL Server forbids `TRUNCATE` on tables referenced by FactTransaction's FKs; only works once FactTransaction no longer references the rows, which `run_all --full-refresh` takes care of). |
| FactTransaction | `TRUNCATE` + `INSERT`, die-on-error off | `--mode overwrite`: Spark JDBC overwrite with `truncate=true` (keeps the DDL). `--mode append` inserts new TransactionIDs only. |

Transformation details:

- **Load_DimCustomer**: `customer` LEFT JOIN `city` on `city_id` LEFT JOIN `state` on `state_id`
  (Talend tMap lookups are outer joins), `upper()` on `customer_name`, `address`, `gender`
  (the columns Talend wraps in `StringHandling.UPCASE`), `age` cast to `INT`.
- **Load_FactTransaction**: the three sources are normalized to one schema and combined with
  `unionByName`. Talend's tUniqRow keeps the *first* row per `transaction_id` in tUnite merge
  order (SQL Server, Excel, CSV); a bare `dropDuplicates` keeps an arbitrary row, so the job
  ranks rows by source priority before deduplicating. In the sample data transactions 6 and 7
  exist in both SQL Server and Excel with different dates, and the SQL Server rows win.
  Rows that do not parse in the file feeds, and rows whose `AccountID`/`BranchID` are not in
  DimAccount/DimBranch, are dropped and logged, matching what Talend's die-on-error-off output
  did silently (transactions 23–25 in the CSV reference unknown accounts 22/23).

## Running the reports

Both read the DWH over JDBC and accept `--output console|csv|jdbc` (default `console`),
`--output-path` (csv), `--output-table` (jdbc, written to the DWH) and `--output-mode overwrite|append`.

```bash
# EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-20';
pyspark_jobs/bin/submit.sh pyspark_jobs/reports/daily_transaction.py \
  --start-date 2024-01-18 --end-date 2024-01-20

# EXEC sp_BalancePerCustomer @customer_name = 'Shelly';
pyspark_jobs/bin/submit.sh pyspark_jobs/reports/balance_per_customer.py --customer-name Shelly

# Publish for downstream consumers
pyspark_jobs/bin/submit.sh pyspark_jobs/reports/daily_transaction.py \
  --start-date 2024-01-01 --end-date 2024-01-31 --output csv --output-path /data/reports/daily
pyspark_jobs/bin/submit.sh pyspark_jobs/reports/balance_per_customer.py \
  --customer-name Shelly --output jdbc --output-table dbo.rpt_BalancePerCustomer
```

`balance_per_customer` matches names and `Status = 'active'` case-insensitively by default,
like the procedure under the DWH's `SQL_Latin1_General_CP1_CI_AS` collation (DimCustomer
names are stored upper-case). `--case-sensitive` switches to a literal `contains()`.
The search text is matched literally; `%`/`_` are not treated as LIKE wildcards.

## Tests

```bash
pip install -r pyspark_jobs/requirements.txt
python -m pytest pyspark_jobs/tests            # unit tests, local Spark, no database

# Parity against SQL Server: runs run_all --full-refresh, then compares both reports with
# EXEC sp_DailyTransaction / sp_BalancePerCustomer for several parameter sets.
RUN_SQLSERVER_TESTS=1 SOURCE_DB_USER=sa SOURCE_DB_PASSWORD=... SOURCE_DB_TRUST_SERVER_CERTIFICATE=true \
  python -m pytest pyspark_jobs/tests
```

Local SQL Server for the parity tests (restore the backup, then apply both scripts; strip the
`PRINT` lines that precede `CREATE PROCEDURE` in `02_create_procedures.sql`, because
`CREATE PROCEDURE` must start its batch):

```bash
docker run -d --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD -p 1433:1433 \
  -v "$PWD/data_sources:/data:ro" mcr.microsoft.com/mssql/server:2022-latest
docker exec mssql /opt/mssql-tools18/bin/sqlcmd -C -U sa -P "$MSSQL_SA_PASSWORD" -Q \
  "RESTORE DATABASE sample FROM DISK='/data/sample.bak' WITH MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf'"
```
