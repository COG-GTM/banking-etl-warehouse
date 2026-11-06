# PySpark ETL jobs

PySpark re-implementation of the four Talend jobs in `talend_jobs/` plus the two
reporting stored procedures. Jobs read the `sample` source database and the files
in `data_sources/`, and write into the existing `DWH` star schema created by
`sql_scripts/01_create_tables.sql` — table and column names are unchanged, so
`sp_DailyTransaction` and `sp_BalancePerCustomer` keep working on the loaded data.

| Talend job | PySpark module | Source | Target | Write |
|---|---|---|---|---|
| `Load_DimBranch` | `load_dim_branch.py` | `sample.dbo.branch` | `DimBranch` | full reload (DELETE + INSERT) |
| `Load_DimAccount` | `load_dim_account.py` | `sample.dbo.account` | `DimAccount` | full reload (DELETE + INSERT) |
| `Load_DimCustomer` | `load_dim_customer.py` | `sample.dbo.customer` ⟕ `city` ⟕ `state` | `DimCustomer` | full reload (DELETE + INSERT) |
| `Load_FactTransaction` | `load_fact_transaction.py` | `sample.dbo.transaction_db` + `transaction_excel.xlsx` + `transaction_csv.csv` | `FactTransaction` | TRUNCATE + INSERT |

`run_all.py` runs them in the README order: DimBranch → DimAccount → DimCustomer → FactTransaction.

```
pyspark_jobs/
├── common/
│   ├── config.py          # env-driven source/target JDBC + file settings
│   ├── io.py              # JDBC read/write, CSV/Excel readers
│   └── spark_session.py   # SparkSession factory (SQL Server JDBC driver)
├── load_dim_branch.py
├── load_dim_account.py
├── load_dim_customer.py
├── load_fact_transaction.py
├── run_all.py
├── reports/
│   ├── daily_transaction.py      # sp_DailyTransaction
│   └── balance_per_customer.py   # sp_BalancePerCustomer
├── tests/                 # local-Spark unit tests (no database needed)
└── requirements.txt
```

## Behaviour

- **Writes never recreate tables.** Spark's JDBC `overwrite` normally drops and
  recreates the table (losing PKs/FKs and the `MONEY`/`DATETIME` types). Here
  `FactTransaction` uses `overwrite` with `truncate=true`, and the dimensions use
  `DELETE FROM` + append because SQL Server refuses `TRUNCATE` on tables referenced
  by a foreign key. `run_all.py` clears `FactTransaction` first so the dimension
  reloads don't hit `FK_FactTransaction_*`; the fact load repopulates it at the end.
  Running `load_dim_branch.py` / `load_dim_account.py` on their own while
  `FactTransaction` has rows fails with an explanatory error.
- **DimCustomer**: customer is joined to city and state with left joins (Talend
  tMap lookups), so a customer with an unknown city keeps NULL `CityName`/`StateName`.
  `CustomerName`, `Address`, `CityName`, `StateName` and `Gender` are uppercased.
  The Talend tMap uppercased only `customer_name`, `address` and `gender`; set
  `DIM_CUSTOMER_UPPERCASE_COLUMNS=CustomerName,Address,Gender` for exact Talend output.
- **FactTransaction**: the three sources are normalised to one schema and combined
  with `unionByName` in Talend's tUnite order (SQL Server → Excel → CSV). For each
  `transaction_id` the row from the highest-priority source is kept, then
  `dropDuplicates(["transaction_id"])` (tUniqRow). The CSV `transaction_date` is
  `dd-MM-yyyy HH:mm:ss`.
- **Foreign keys**: Talend's tMSSqlOutput ran with die-on-error off, so rows whose
  `AccountID`/`BranchID` has no dimension row were rejected by SQL Server and
  skipped. The PySpark job filters those rows out before writing and logs their IDs
  (with the bundled data: transactions 23, 24, 25 reference accounts 22/23, which
  don't exist). Set `ENFORCE_FACT_FOREIGN_KEYS=false` to send every row to SQL Server.
- **Reports**: `LIKE '%name%'` and `Status = 'active'` are compared
  case-insensitively, matching SQL Server's default `*_CI_AS` collation.

## Requirements

- Python 3.9–3.12, Java 8/11/17 (`JAVA_HOME` set), Spark 3.5 (`pip install -r pyspark_jobs/requirements.txt` provides `pyspark` and `spark-submit`).
- **SQL Server JDBC driver** `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11`. Pick one:
  - `spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 ...`
  - `spark-submit --jars /path/to/mssql-jdbc-12.8.1.jre11.jar ...`
  - when launching with plain `python pyspark_jobs/<job>.py` (no `spark-submit`), set
    `MSSQL_JDBC_JAR=/path/to/mssql-jdbc-12.8.1.jre11.jar`, or let `common/spark_session.py`
    add `MSSQL_JDBC_PACKAGE` to `spark.jars.packages` itself (needs Maven access;
    `SPARK_JARS_REPOSITORIES` adds a mirror). Under `spark-submit` the JVM is already
    started, so jars/packages must be passed on the command line.
- The `sample` database restored from `data_sources/sample.bak`, and the `DWH`
  database created with `sql_scripts/01_create_tables.sql` (and
  `sql_scripts/02_create_procedures.sql` for the stored procedures).

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `MSSQL_HOST` / `MSSQL_PORT` | `localhost` / `1433` | SQL Server for both databases |
| `MSSQL_USER` / `MSSQL_PASSWORD` | `sa` / – | Credentials for both databases |
| `MSSQL_ENCRYPT` / `MSSQL_TRUST_SERVER_CERTIFICATE` | `true` / `true` | JDBC TLS settings |
| `MSSQL_JDBC_PROPERTIES` | – | Extra `;key=value` JDBC URL properties (e.g. `integratedSecurity=true`) |
| `SOURCE_DB_*` / `TARGET_DB_*` | fall back to `MSSQL_*` | Per-database overrides: `HOST`, `PORT`, `USER`, `PASSWORD`, `ENCRYPT`, `TRUST_SERVER_CERTIFICATE`, `JDBC_PROPERTIES` |
| `SOURCE_DB_DATABASE` / `TARGET_DB_DATABASE` | `sample` / `DWH` | Database names |
| `SOURCE_DB_SCHEMA` / `TARGET_DB_SCHEMA` | `dbo` / `dbo` | Schemas |
| `SOURCE_TRANSACTION_TABLE` | `transaction_db` | Source transaction table |
| `DATA_SOURCES_DIR` | `<repo>/data_sources` | Folder holding the CSV and Excel files |
| `TRANSACTION_CSV_PATH` / `TRANSACTION_EXCEL_PATH` | `$DATA_SOURCES_DIR/transaction_csv.csv` / `…/transaction_excel.xlsx` | File sources (local, HDFS, S3, ABFS…) |
| `TRANSACTION_EXCEL_SHEET` | `Sheet1` | Excel sheet |
| `TRANSACTION_CSV_TIMESTAMP_FORMAT` | `dd-MM-yyyy HH:mm:ss` | CSV date format |
| `EXCEL_READER` | `pandas` | `pandas` (openpyxl on the driver) or `crealytics` (`com.crealytics.spark.excel`; with `spark-submit` add `--packages com.crealytics:spark-excel_2.12:3.5.1_0.20.4`, with plain `python` it is added automatically) |
| `DIM_WRITE_MODE` / `FACT_WRITE_MODE` | `overwrite` | `overwrite` (full reload) or `append` |
| `ENFORCE_FACT_FOREIGN_KEYS` | `true` | Drop fact rows with unknown AccountID/BranchID |
| `DIM_CUSTOMER_UPPERCASE_COLUMNS` | `CustomerName,Address,CityName,StateName,Gender` | DimCustomer columns to uppercase |
| `MSSQL_JDBC_JAR` / `MSSQL_JDBC_PACKAGE` | – / `…mssql-jdbc:12.8.1.jre11` | Driver jar path or Maven coordinate |
| `SPARK_MASTER`, `SPARK_JARS_REPOSITORIES`, `SPARK_LOG_LEVEL` | – | Spark session overrides |

## Running

From the repository root:

```bash
pip install -r pyspark_jobs/requirements.txt
export MSSQL_HOST=localhost MSSQL_USER=sa MSSQL_PASSWORD=...   # your SQL Server login

# Package the modules for executors (needed on a cluster; harmless locally)
zip -r pyspark_jobs.zip pyspark_jobs -x '*/tests/*' '*__pycache__*'

# All four jobs in dependency order
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 \
  --py-files pyspark_jobs.zip pyspark_jobs/run_all.py

# A subset (still run in dependency order)
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 \
  --py-files pyspark_jobs.zip pyspark_jobs/run_all.py --jobs dim_customer,fact_transaction

# A single job
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 \
  --py-files pyspark_jobs.zip pyspark_jobs/load_fact_transaction.py

# Reports (same output as the stored procedures)
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 \
  --py-files pyspark_jobs.zip pyspark_jobs/reports/daily_transaction.py \
  --start-date 2024-01-18 --end-date 2024-01-20
spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 \
  --py-files pyspark_jobs.zip pyspark_jobs/reports/balance_per_customer.py --customer-name Shelly
```

On a cluster, ship the source files with `--files data_sources/transaction_csv.csv,data_sources/transaction_excel.xlsx`
or point `TRANSACTION_CSV_PATH`/`TRANSACTION_EXCEL_PATH` at shared storage.

## Tests

```bash
python -m pytest pyspark_jobs/tests
```

The tests run the transforms and both reports on local Spark DataFrames and parse
the real CSV/Excel files; no database or JDBC driver is needed.
