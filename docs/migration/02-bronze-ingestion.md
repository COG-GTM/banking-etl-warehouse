# Slice 2 — Bronze ingestion (Talend extract stage -> Databricks)

Replaces the extract half of the Talend jobs (`tMSSqlInput`, `tFileInputDelimited`,
`tFileInputExcel`) with Databricks bronze ingestion. Bronze is raw and append-only:
every column is stored as `STRING` exactly as it arrives, plus ingest metadata.
Typing, dedup (`tUniqRow`), joins (`tMap`) and casing rules stay in silver/gold.

Source of truth for every column list below is the committed Talend XML
(`talend_jobs/*.zip` -> `IDX_INTERNSHIP/process/<Job>_0.1.item` and
`IDX_INTERNSHIP/metadata/connections/Sample_DB_Connection_0.1.item`), not the README.

## Asset mapping

| Legacy asset | Databricks asset |
| --- | --- |
| `tFileInputDelimited_1` (`C:/Data Rakamin/transaction_csv.csv`, `,` sep, 1 header row) | Auto Loader (`cloudFiles`, csv) -> `banking.bronze.file_transaction_csv` |
| `tFileInputExcel_1` (`C:/Data Rakamin/transaction_excel.xlsx`, sheet 1, 1 header row) | `com.crealytics.spark.excel` (or pandas/openpyxl fallback) -> `banking.bronze.file_transaction_excel` |
| `tMSSqlInput` on `dbo.transaction_db` (`Load_FactTransaction`) | JDBC -> `banking.bronze.sqlserver_transaction_db` |
| `tMSSqlInput` on `dbo.account` (`Load_DimAccount`) | JDBC -> `banking.bronze.sqlserver_account` |
| `tMSSqlInput` on `dbo.customer` (`Load_DimCustomer`) | JDBC -> `banking.bronze.sqlserver_customer` |
| `tMSSqlInput` on `dbo.city` (`Load_DimCustomer`) | JDBC -> `banking.bronze.sqlserver_city` |
| `tMSSqlInput` on `dbo.state` (`Load_DimCustomer`) | JDBC -> `banking.bronze.sqlserver_state` |
| `tMSSqlInput` on `dbo.branch` (`Load_DimBranch`) | JDBC -> `banking.bronze.sqlserver_branch` |
| `Sample_DB_Connection` (integrated security, `localhost:1433`, db `sample`) | `JdbcConnection` + `dbutils.secrets` (scope `banking-etl`) |
| `tUnite_1`, `tUniqRow_1`, `tMap_1`, `tMSSqlOutput` | **not** in bronze — silver/gold slices |

Six source tables are consumed by the Talend jobs; `Sample_DB_Connection` exposes
exactly those six, so the bronze list is complete.

### Columns (raw `STRING` in bronze)

- `file_transaction_csv`, `file_transaction_excel`, `sqlserver_transaction_db`:
  `transaction_id, account_id, transaction_date, amount, transaction_type, branch_id`
- `sqlserver_account`: `account_id, customer_id, account_type, balance, date_opened, status`
- `sqlserver_customer`: `customer_id, customer_name, address, city_id, age, gender, email`
- `sqlserver_city`: `city_id, city_name, state_id`
- `sqlserver_state`: `state_id, state_name`
- `sqlserver_branch`: `branch_id, branch_name, branch_location`

### Metadata columns

| Column | File sources | JDBC sources |
| --- | --- | --- |
| `_ingest_ts` | `current_timestamp()` | `current_timestamp()` |
| `_source_file` | `_metadata.file_path` (or the explicit path for the pandas reader) | — |
| `_source_table` | — | e.g. `dbo.account` |
| `_batch_id` | `<utc ts>-<uuid8>`, one per notebook run | same |
| `_rescued_data` | CSV only (Auto Loader `rescuedDataColumn`) | — |

## Code layout

```
databricks/bronze/
  config.py                  # source definitions from the Talend XML
  metadata.py                # _ingest_ts / _source_file / _source_table / _batch_id
  file_ingest.py             # Auto Loader + batch CSV + spark-excel + pandas fallback
  jdbc_ingest.py             # query/option builders, secret resolver, single JDBC call
  writer.py                  # append-only Delta batch / stream writes
  notebooks/ingest_files.py      # widgets -> file ingestion
  notebooks/ingest_sqlserver.py  # widgets -> JDBC ingestion
databricks/tests/bronze/     # pytest suite (runs off-cluster)
```

Everything is plain importable Python; the only Databricks-bound calls are
`read_csv_autoloader`, `read_excel_spark`, `read_jdbc`, `write_bronze_stream` and
`dbutils_secret_resolver`, each a one-liner the tests replace with a local reader.

## Auto Loader (CSV)

```python
{
  "cloudFiles.format": "csv",
  "cloudFiles.schemaLocation": "<checkpoint>/_schema",
  "cloudFiles.inferColumnTypes": "false",
  "cloudFiles.schemaEvolutionMode": "rescue",
  "cloudFiles.includeExistingFiles": "true",
  "rescuedDataColumn": "_rescued_data",
  "header": "true",
  "mode": "PERMISSIVE",
}
```

The reader is given the explicit all-`STRING` schema, so inference never changes
types between runs and anything unexpected lands in `_rescued_data`. The stream
writes with `trigger(availableNow=True)` and a checkpoint per table, which makes
re-runs idempotent at the file level (Talend re-read the whole file every run).

## Excel

`spark-excel` is not part of Databricks Runtime. Install
`com.crealytics:spark-excel_2.12:3.5.1_0.20.4` (Maven) as a cluster/job library,
matching the runtime's Spark/Scala version. Auto Loader has no `xlsx` format, so
the Excel table is a batch read rather than a stream.

`read_excel_pandas` is the fallback used for local runs and tests (single small
file, driver-side). It renders Excel's native datetime cells with the
`dd-MM-yyyy HH:mm:ss` pattern declared on `tFileInputExcel_1`, so the bronze string
matches the CSV source character for character and silver can parse both with one
format. This is a formatting decision, not a cast — worth knowing when comparing
bronze to the raw workbook.

## JDBC (SQL Server)

- Credentials only via `dbutils.secrets.get(scope="banking-etl", key=...)`; the
  option builder takes a resolver function and has no way to accept a literal.
- Query mirrors the Talend `SELECT`, with every column wrapped in
  `CAST(... AS NVARCHAR(4000))` so bronze stores source text and never depends on
  JDBC type coercion.
- Full refresh by default; `watermark_value` switches to an incremental
  `WHERE <watermark> > '<value>'` (`transaction_date` for `transaction_db`,
  `date_opened` for `account`; the lookup tables have no usable watermark and are
  always full refreshes).
- Partitioned reads on the numeric key (`partitionColumn`/`numPartitions`/
  `lowerBound`/`upperBound`); bounds come from a `MIN`/`MAX` probe query. Partial
  partition arguments raise instead of silently degrading to a single-task read.

## Does not map 1:1

- **Windows-local file paths** (`C:/Data Rakamin/...`) become a Unity Catalog
  volume landing path supplied by a widget.
- **Integrated security** in `Sample_DB_Connection` (no username/password in the
  Talend metadata) cannot be used from Databricks; a SQL login read from a secret
  scope replaces it.
- **`tUnite` of CSV + Excel + SQL Server** is deliberately *not* reproduced:
  bronze keeps one table per source so lineage survives; the union and the
  `tUniqRow` dedup on `transaction_id` happen in silver.
- **Typed reads.** Talend declared `id_Integer` / `id_Date` on extract and the
  target DWH used `MONEY`/`DATETIME`. Bronze keeps strings; the contract's
  `DECIMAL(19,4)` / `TIMESTAMP` typing happens downstream.
- **`TRUNCATE` + `INSERT`** in `Load_FactTransaction`'s output is replaced by
  append-only bronze; full-refresh semantics are re-established downstream.
- **Auto Loader file notification mode** is not configured (directory listing is
  enough at this volume); switch via `extra` options if the landing zone grows.

## Local validation

No Databricks workspace or SQL Server was available, so the suite runs on local
PySpark against the committed files and substitutes a local reader for JDBC.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r databricks/tests/bronze/requirements.txt
python -m pytest databricks/tests/bronze -q
```

The Delta jars are resolved from Maven by `spark.jars.packages`. On a machine
without Maven access, download `delta-spark_2.12-3.2.0.jar` and
`delta-storage-3.2.0.jar` and point the suite at them:

```bash
DELTA_JARS=/path/delta-spark_2.12-3.2.0.jar,/path/delta-storage-3.2.0.jar \
  python -m pytest databricks/tests/bronze -q
```

Covered: real CSV (12 rows) and XLSX (7 rows) ingested into local Delta tables,
column names/order, all-string types, populated metadata columns, append-only
behaviour across two batches, `_rescued_data` capture, and the JDBC query/option
builders (full refresh, incremental, partitioned, secret handling).
