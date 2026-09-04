# FactTransaction loader (PySpark)

PySpark/Delta port of the Talend job `Load_FactTransaction` (`talend_jobs/Load_FactTransaction.zip`,
project `IDX_INTERNSHIP`). It builds the gold `FactTransaction` table of the medallion warehouse
from three sources: the bronze SQL Server `transaction_db` table, `data_sources/transaction_excel.xlsx`
and `data_sources/transaction_csv.csv`.

## Component mapping

| Talend component | Config in the exported job | PySpark equivalent |
| --- | --- | --- |
| `tMSSqlInput_1` (`tDBInput_1`) | `sample` DB, `SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id FROM dbo.transaction_db` | `readers.read_table_transactions` (`spark.table("<catalog>.<bronze_schema>.transaction_db")`) |
| `tFileInputExcel_1` | `transaction_excel.xlsx`, sheet `Sheet1`, header 1 row, date pattern `dd-MM-yyyy HH:mm:ss` | `readers.read_excel_transactions` (pandas + openpyxl → `spark.createDataFrame`; optional `com.crealytics.spark.excel`) |
| `tFileInputDelimited_1` | `transaction_csv.csv`, field sep `,`, row sep `\n`, header 1 row, date pattern `dd-MM-yyyy HH:mm:ss` | `readers.read_csv_transactions` (`spark.read.csv` with explicit schema + `to_timestamp`) |
| `tUnite_1` | merge order `row1` (DB) → `row2` (Excel) → `row3` (CSV) | `transforms.unify_transactions` (`unionByName`) |
| `tUniqRow_1` | unique key `transaction_id`, other columns not keys | `transforms.dedupe_transactions` (`row_number` over `TransactionID`) |
| `tMap_1` | renames `row5.*` to `TransactionID`, `AccountID`, `TransactionDate`, `Amount`, `TransactionType`, `BranchID` | `transforms.build_fact_transaction` |
| `tMSSqlOutput_1` (`tDBOutput_1`) | DWH DB, table from context `namaTabelFakta` = `FactTransaction`, action `TRUNCATE` + `INSERT` | `load_fact_transaction.write` (Delta `overwrite`, or `--format parquet --path`) |
| FK constraints in `sql_scripts/01_create_tables.sql` | enforced by SQL Server | `transforms.validate_referential_integrity` (Delta does not enforce FKs) |

## Normalized schema

Every reader returns the same columns so the union is safe:

| Column | Type |
| --- | --- |
| `TransactionID` | `int` |
| `AccountID` | `int` |
| `TransactionDate` | `timestamp` |
| `Amount` | `decimal(19,4)` (`MONEY` in T-SQL; the Talend schema used `INT`) |
| `TransactionType` | `string` |
| `BranchID` | `int` |
| `_source` | `string` — `db` / `excel` / `csv`, dropped by `build_fact_transaction` |

## Deduplication precedence

Talend's `tUniqRow` keeps the *first* row it receives for a duplicate key, which makes the
result depend on the `tUnite` merge order (DB, then Excel, then CSV). `dedupe_transactions`
reproduces that ordering explicitly via `SOURCE_PRIORITY` (`db` > `excel` > `csv`), with
`TransactionDate` then `_source` as tie-breakers, so the outcome is deterministic regardless of
Spark partitioning. The sample data overlaps: Excel and CSV both contain transactions 14 and 15.

## Running

Locally, from `databricks/jobs` (or with that directory on `PYTHONPATH`):

```bash
python -m fact_transaction.load_fact_transaction \
    --bronze-table "" \
    --csv-path ../../data_sources/transaction_csv.csv \
    --excel-path ../../data_sources/transaction_excel.xlsx \
    --format parquet --path /tmp/fact_transaction
```

On Databricks:

```bash
python -m fact_transaction.load_fact_transaction \
    --catalog main --schema gold --bronze-schema bronze \
    --csv-path dbfs:/mnt/raw/transaction_csv.csv \
    --excel-path dbfs:/mnt/raw/transaction_excel.xlsx
```

In a notebook, `params_from_widgets(dbutils)` returns the same `JobParams` from widgets.

## Tests

```bash
pip install "pyspark>=3.5" pandas openpyxl pytest
pytest tests/fact_transaction
```

The tests exercise the real `data_sources/` CSV and Excel files plus a hand-built bronze
DataFrame, and require no Databricks workspace or Delta jar.
