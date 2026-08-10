# Bronze file-source schemas

Reference copies of the explicit `StructType` schemas declared in
`databricks/bronze/ingest_files.py`. The notebook keeps the schemas inline (so it needs no
workspace file access at runtime); these JSON files exist for review and diffing, and are the
output of `StructType.jsonValue()`.

Derivation (see the notebook header for detail):

| source | evidence |
|---|---|
| `transaction_csv.json` | header + 12 data rows of `data_sources/transaction_csv.csv`; Talend `tFileInputDelimited_1` schema in `talend_jobs/Load_FactTransaction.zip` |
| `transaction_excel.json` | `openpyxl` inspection of `data_sources/transaction_excel.xlsx` (`Sheet1`, `A1:F8`, header row 1); Talend `tFileInputExcel_1` schema in the same archive |

Both schemas are the six source columns; the CSV schema additionally carries `_rescued_data`
(Auto Loader / PERMISSIVE rescue column). The audit columns `_ingested_at`, `_source_system` and
`_source_file` are appended by the notebook and are not part of these read schemas.

Deviations from the Talend column types: `amount` is widened from `Integer` to `DECIMAL(19,4)`
(the agreed `MONEY -> DECIMAL(19,4)` mapping of `FactTransaction.Amount`), and `transaction_id` is
nullable so malformed rows are rescued rather than failing the batch.
