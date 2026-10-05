# Ticket 3: transaction CSV + Excel files → bronze (Auto Loader)

Replaces the `tFileInputDelimited` (`transaction_csv.csv`) and `tFileInputExcel` (`transaction_excel.xlsx`)
inputs of the Talend `Load_FactTransaction` job. Ticket 8 unions these bronze tables with
`bronze.sqlserver_transaction_db` (the Talend `tUnite`) in silver.

| Piece | Path |
|---|---|
| Logic | `src/banking_etl/bronze/files.py` |
| Notebook | `notebooks/bronze/ingest_transaction_files.py` (widgets `catalog`, `schema_prefix`, `sources`, `land_from`) |
| Job | `resources/ticket-03-file-ingestion.yml` → `bronze_ingest_transaction_files` (serverless notebook task) |
| Landing helper (laptop/CI) | `scripts/bronze/land_transaction_files.py` |
| Tests | `tests/bronze/test_files.py` |

## Flow
```
data_sources/transaction_csv.csv    ──land──▶ /Volumes/<cat>/<p>bronze/landing/transactions/csv/
data_sources/transaction_excel.xlsx ──land──▶ /Volumes/<cat>/<p>bronze/landing/transactions/excel/
                         Auto Loader (cloudFiles, trigger availableNow)
transactions/csv/   ──▶ <cat>.<p>bronze.file_transaction_csv
transactions/excel/ ──▶ <cat>.<p>bronze.file_transaction_excel
schema + checkpoint ──▶ /Volumes/<cat>/<p>ops/checkpoints/bronze/<table>/{_schema,_checkpoint}
```

## Tables
Both tables are append-only. Source columns keep their snake_case names, followed by the metadata columns.

| Column | `file_transaction_csv` | `file_transaction_excel` |
|---|---|---|
| `transaction_id` | INT (inferred) | BIGINT (inferred) |
| `account_id` | INT | BIGINT |
| `transaction_date` | **STRING**, raw `dd-MM-yyyy HH:mm:ss` (schema hint) | TIMESTAMP_NTZ (real Excel date cell) |
| `amount` | INT | BIGINT |
| `transaction_type` | STRING | STRING |
| `branch_id` | INT | BIGINT |
| `_rescued_data` | STRING: JSON of values that didn't fit the schema (+ `_file_path`) | STRING, always NULL (the Excel reader has no rescued-data column) |
| `_ingested_at` | TIMESTAMP (`current_timestamp()` of the micro-batch) | same |
| `_source` | `file_csv` | `file_excel` |
| `_source_file` | `_metadata.file_path`, e.g. `/Volumes/migration_demo/banking_etl_bronze/landing/transactions/csv/transaction_csv.csv` | same |

Silver (ticket 8) must parse the CSV `transaction_date` with `to_timestamp(col, 'dd-MM-yyyy HH:mm:ss')`
(the Talend pattern) and cast both sources' numeric columns to the silver types.

## Mapping decisions
- **CSV options** match the Talend component: header row, `,` separator. Talend reads ISO-8859-15, but Spark 4's
  CSV reader rejects that charset, so we use **ISO-8859-1**. The two are identical except for 8 code points (`€`, `Š`, …).
  The file is ASCII.
- **CSV schema**: `cloudFiles.inferColumnTypes=true` plus `cloudFiles.schemaHints = "transaction_date STRING"`, so
  the non-ISO date isn't mis-typed. `rescuedDataColumn=_rescued_data` captures malformed values (e.g. `amount=12abc`):
  the typed column becomes NULL and the raw value is kept as JSON.
- **Excel**: Databricks has native Excel support (`.xlsx`/`.xls`, DBR 17.1+, batch + Auto Loader `cloudFiles.format=excel`;
  [docs](https://docs.databricks.com/aws/en/query/formats/excel), page last updated 2026-09-11, no preview label).
  A probe on the dev serverless compute (Spark 4.2) read `Sheet1` correctly, and `openpyxl` is *not* installed there.
  So we use the **native reader**, not the `binaryFile` + openpyxl fallback. Options: `headerRows=1`, `dataAddress=Sheet1`
  (Talend `SHEETLIST="Sheet1"`, `HEADER=1`). The docs say Auto Loader Excel streams don't support schema evolution, so
  `cloudFiles.schemaEvolutionMode=none`. The schema is inferred from the first files and then frozen in the schema location.
- **Incremental + idempotent**: the Auto Loader checkpoint records every ingested file. `trigger(availableNow=True)`
  processes only new files and then stops, so re-running the job (or re-landing a file with the same name, because
  `cloudFiles.allowOverwrites` defaults to false) does not duplicate rows. The Delta sink commits each micro-batch
  exactly once per checkpoint.
- **Local tests** can't run Auto Loader or the Excel reader (OSS Spark). `read_transaction_csv` (infer, apply the same
  hints, re-read) and `read_transaction_excel` (native reader if present, else openpyxl with the same type inference)
  produce the same columns, types and metadata. `write_available_now` is tested with the OSS file stream source to show
  that re-runs are no-ops.

## Schema evolution behaviour
- **CSV (`addNewColumns`)**: when a new file has an extra column (e.g. `channel`), Auto Loader records the column in the
  schema location and stops the stream with `UnknownFieldException` (no rows of that batch are committed). The next
  run writes the file with the new column (`mergeSchema=true` on the Delta sink). In a Databricks notebook a failed
  stream fails the whole command even if Python catches the exception, so the restart comes from the job task retry
  (`max_retries: 2` in `resources/ticket-03-file-ingestion.yml`; serverless one-time runs also retried automatically in
  the live check). Outside notebooks (scripts, Databricks Connect) `ingest_source(..., schema_change_retries=1)` restarts
  in-process; the notebook passes `schema_change_retries=0`. If you run the notebook by hand, run it again after a
  schema-change failure.
  Earlier rows get NULL for the new column. Type changes are not evolved: values that don't fit the recorded type go to
  `_rescued_data`. A file with a *missing* column gives NULLs for it.
- **Excel (`none`)**: new columns are ignored and type mismatches give NULL. To change the Excel schema on purpose, delete
  `/Volumes/<cat>/<p>ops/checkpoints/bronze/file_transaction_excel/` and drop/rename the table, then re-run (this re-ingests
  every file in the folder).

## Adding new files
1. Drop a new file with a **new file name** into `transactions/csv/` or `transactions/excel/`. A file whose name was
   already ingested is ignored, so use a new name for corrected data. For example, from a laptop:
   ```bash
   databricks fs cp new_batch_2024_02.csv dbfs:/Volumes/migration_demo/banking_etl_bronze/landing/transactions/csv/
   ```
   To land the repo's sample files, run `python scripts/bronze/land_transaction_files.py --catalog migration_demo --schema-prefix banking_etl_`
   (existing files are skipped), or set the notebook's `land_from` widget to a folder that holds them.
2. Run the job / notebook (`sources=csv,excel` or just one). Only the new files are appended.
3. For a full re-ingest (e.g. in a test catalog), drop the bronze table and delete its checkpoint folder.

## Live validation (dev: `migration_demo`, `schema_prefix=banking_etl_`)
See the PR description for run URLs. Run 1 landed both files and appended 12 CSV + 7 Excel rows. Run 2 (same widgets,
landing skipped) added 0 rows to both tables.

Schema-evolution scratch check (table `bronze._t3_evolution_check_csv` and its own landing/checkpoint folders, all
dropped afterwards): load the 12-row CSV (+12), re-run (+0), then land `transaction_csv_new_col.csv` (extra `channel`
column) and `transaction_csv_bad_amount.csv` (`amount=12abc`). The first attempt failed with
`UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE [channel]`; the automatic task retry succeeded (12 -> 14 rows). The table
gained `channel STRING` (`ATM` for row 26, NULL for older rows) and row 27 had `amount=NULL` with
`{"amount":"12abc",...}` in `_rescued_data`. Another run added 0 rows. An earlier attempt that restarted in-process
in the same notebook command failed with "Some streams terminated before this command could finish", which is why
the notebook relies on task retries.
