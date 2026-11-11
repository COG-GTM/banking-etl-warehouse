# Bronze ingestion

Code: `databricks/src/banking_etl/ingestion/`. Job: `databricks/resources/bronze_ingestion.yml`
(notebook `databricks/src/notebooks/bronze_ingestion.py`). Tests: `databricks/tests/bronze_ingestion/`.

## Sources

`sample.bak` restores to database `sample`, schema `dbo`, with six tables (confirmed by restoring
the backup in SQL Server 2022 and by the `tMSSqlInput` queries in `talend_jobs/*.zip`):

| Source table | Rows | Bronze table | Talend job |
|---|---|---|---|
| `dbo.account` | 21 | `<catalog>.bronze.account` | Load_DimAccount |
| `dbo.branch` | 5 | `<catalog>.bronze.branch` | Load_DimBranch |
| `dbo.customer` | 20 | `<catalog>.bronze.customer` | Load_DimCustomer |
| `dbo.city` | 52 | `<catalog>.bronze.city` | Load_DimCustomer |
| `dbo.state` | 9 | `<catalog>.bronze.state` | Load_DimCustomer |
| `dbo.transaction_db` | 10 | `<catalog>.bronze.transaction_db` | Load_FactTransaction |

Files (Load_FactTransaction): `transaction_csv.csv` -> `bronze.transaction_csv` (timestamps
`dd-MM-yyyy HH:mm:ss`), `transaction_excel.xlsx` -> `bronze.transaction_excel`.

Catalog, bronze schema, landing volume and checkpoint root all come from `BronzeConfig`
(job parameters `catalog` = bundle `${var.catalog}`, `bronze_schema`, `landing_root`, `checkpoint_root`).

## sample.bak paths (`sample_db_mode`)

- `jdbc`: reads each table live over JDBC. Secret scope `jdbc_secret_scope` (default
  `banking_sample_db`) holds `host`, `user`, `password`, optional `port`, `database`,
  `trust_server_certificate`.
- `federation`: reads `<foreign_catalog>.dbo.<table>` through Lakehouse Federation. With
  `create_foreign_catalog=true` the notebook runs `CREATE CONNECTION` / `CREATE FOREIGN CATALOG`
  from `federation_ddl()` using the same secret scope.
- `files` (default): fallback when the SQL Server is not reachable. Extract each table to CSV
  and drop it at `<landing>/sample_db/<table>/`; it is ingested like the other file sources.
  The extracts of the real backup are committed under `data_sources/sample_db/`.

Live snapshots append one full copy of the table per run (tagged with `_ingest_run_id`); the
files path appends each new extract file once (checkpointed).

To regenerate the extracts:

```bash
MSSQL_SA_PASSWORD='<strong password>' databricks/scripts/restore_sample_bak.sh
SAMPLE_DB_JDBC_URL='jdbc:sqlserver://localhost:14333;databaseName=sample;encrypt=true;trustServerCertificate=true' \
SAMPLE_DB_USER=sa SAMPLE_DB_PASSWORD='<strong password>' \
python databricks/scripts/extract_sample_bak.py --out data_sources/sample_db
```

Stage the sample files into the landing volume with `databricks/scripts/stage_landing_files.sh`.

## Bronze table shape

Source columns (typed per the source DDL) plus:

| Column | Meaning |
|---|---|
| `_source_system` | `sqlserver:sample.dbo.<table>`, `federation:<catalog>.dbo.<table>` or `file:<source>` |
| `_source_file` | file path (NULL for live reads) |
| `_file_modification_time` | file modification time (NULL for live reads) |
| `_ingested_at` | load timestamp |
| `_ingest_run_id` | job run id |
| `_rescued_data` | JSON of raw values that failed to parse, plus malformed-row / missing-column info |

`transaction_excel` also has `_excel_sheet` and `_excel_row`.

## Parsing and rescue

File sources are append-only ingestion history: Auto Loader (`cloudFiles`) with a checkpoint
and `availableNow`, so each file lands once and re-runs only pick up new files.

Every file value is read as STRING and parsed with `try_cast` / `try_to_timestamp` (SQL
expressions, so they work on serverless Spark Connect). A non-blank value that fails to parse
becomes NULL in the typed column and its raw text is kept in `_rescued_data`, so an impossible
date such as `31-02-2023 10:00:00` is never silently NULLed.

Excel is not read with the native Auto Loader Excel format (one bad typed cell fails the whole
file). Files are read as `binaryFile` and parsed with openpyxl in `mapInPandas`: cells are
converted to text, headers matched case-insensitively, blank rows skipped, missing/unexpected
columns and unreadable workbooks recorded in `_rescued_data` instead of failing the run.

Talend reads the CSV as ISO-8859-15; Spark 4 CSV does not accept that charset, so the default is
ISO-8859-1 (identical except for 8 symbols such as the euro sign).
