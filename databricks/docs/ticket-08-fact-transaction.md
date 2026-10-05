# Ticket 8: Load_FactTransaction -> silver.transaction -> gold.fact_transaction

Source job: `talend_jobs/Load_FactTransaction.zip` (`process/Load_FactTransaction_0.1.item`).
Code: `src/banking_etl/facts/transaction.py`. Notebooks: `notebooks/silver/silver_transaction.py`,
`notebooks/gold/load_fact_transaction.py`. Job: `load_fact_transaction` in `resources/ticket-08-fact-transaction.yml`.

```
bronze.sqlserver_transaction_db ─┐ row1, mergeOrder=1
bronze.file_transaction_excel ───┤ row2, mergeOrder=2   conform (dates, amount DECIMAL(19,4))
bronze.file_transaction_csv ─────┘ row3, mergeOrder=3   unparseable rows -> ops.fact_transaction_rejects
        │  tUnite (SQL Server, Excel, CSV) -> tUniqRow on transaction_id (first wins)
        └─> silver.transaction (overwrite)
silver.transaction ── left join dim_account (AccountID), dim_branch (BranchID)
        ├─> gold.fact_transaction          (full refresh: INSERT OVERWRITE = TRUNCATE + INSERT)
        └─> ops.fact_transaction_rejects   (FK misses, reject_reason)
```

## Talend -> Databricks mapping

| Talend (`.item`) | Databricks |
|---|---|
| `tDBInput_1` (`tMSSqlInput`, `SELECT ... FROM dbo.transaction_db`), `transaction_date` `datetime2` | `conform_sqlserver`: bronze TIMESTAMP kept, `amount` INT -> `DECIMAL(19,4)` |
| `tFileInputExcel_1` (`Sheet1`, header 1, `transaction_date` id_Date `dd-MM-yyyy HH:mm:ss`, `DIE_ON_ERROR=false`) | `conform_excel`: date cells (bronze `TIMESTAMP_NTZ`) keep their wall-clock time; text cells are parsed with the pattern |
| `tFileInputDelimited_1` (`,`, header 1, `transaction_date` id_Date `dd-MM-yyyy HH:mm:ss`, `DIE_ON_ERROR=false`, no REJECT link) | `conform_csv`: `try_to_timestamp(transaction_date, 'dd-MM-yyyy HH:mm:ss')` (strict; empty -> NULL). No `Column.try_cast` (missing from the serverless Spark Connect client) |
| File inputs drop unparseable rows silently (no REJECT link) before `tUnite` | Same rows (`NULL_TRANSACTION_ID`, `INVALID_TRANSACTION_DATE`, `MALFORMED_RECORD` from `_rescued_data`) are removed before dedup, so they never shadow a valid duplicate, and written to `ops.fact_transaction_rejects` |
| `tUnite_1`: `row1` mergeOrder 1 (SQL Server), `row2` 2 (Excel), `row3` 3 (CSV) | `unite`: `unionByName` in that order; each row carries `_source_order` 1/2/3 |
| `tUniqRow_1`: key `transaction_id` only, case-sensitive, `ONLY_ONCE_EACH_DUPLICATED_KEY=false`; `UNIQUE` -> tMap, `DUPLICATE` not connected | `dedup_first`: `row_number()` over `transaction_id` ordered by `_source_order` (then `_ingested_at`, `_source_file`, values for in-source ties). First occurrence wins: SQL Server > Excel > CSV. Duplicates dropped |
| `tMap_1` `to_FactTransaction`: `row5.<col>` -> `<Col>`, no expressions/filters/lookups | `to_fact_rows` (`TMAP_TO_FACT`) |
| `Amount` id_Integer -> `MONEY` | `DECIMAL(19,4)` (README type map) |
| `tDBOutput_1`: `TABLE_ACTION=TRUNCATE`, `DATA_ACTION=INSERT`, `IDENTITY_INSERT=false` | `insertInto(gold.fact_transaction, overwrite=True)`: atomic full replace, table metadata/constraints kept |
| `FK_FactTransaction_DimAccount` / `FK_FactTransaction_DimBranch` (SQL Server-enforced) | Informational in UC; `lookup_dim_keys` left-joins `AccountKey` / `BranchKey` on the natural keys; misses go to `ops.fact_transaction_rejects` (`MISSING_DIM_ACCOUNT`, `MISSING_DIM_BRANCH`, both `;`-joined). NULL AccountID/BranchID passes (SQL Server does not check FKs on NULL) and loads with a NULL key |
| — (legacy table had no surrogate keys) | `AccountKey`, `BranchKey` BIGINT added per ticket 4 DDL; dims are never written by this job |

## Legacy behaviour for account_id 22 / 23 (`DIE_ON_ERROR=false`)
`data_sources/transaction_csv.csv` rows 23, 24, 25 reference `account_id` 22, 23 which are not in `sample.dbo.account`
(so not in `DimAccount`). `tDBOutput_1` has `DIE_ON_ERROR=false`, `USE_BATCH_SIZE=true`, `BATCH_SIZE=10000`,
`COMMIT_EVERY=10000` and no REJECT link. With 25 rows everything goes in one JDBC batch; the FK violations surface as a
`BatchUpdateException` that Talend logs to the console (no reject flow, no die), and the commit at the end of the
component persists the rows that SQL Server accepted.

Replayed on SQL Server 2022 (`sample.bak` restored, `sql_scripts/01_create_tables.sql`, dims loaded, the 25 deduped rows
inserted in tUnite/tUniqRow order as one batch with mssql-jdbc 12.8.1 and autocommit off): **22 rows committed, 3 rows
failed (TransactionID 23, 24, 25, `FK_FactTransaction_DimAccount`)**. `fixtures/parity/fact_transaction.csv` is the
resulting `DWH.dbo.FactTransaction`. The failing rows are the last three in the stream, so the outcome does not depend on
whether the driver stops at the first failing statement of a batch or continues.

Databricks matches that outcome (same 22 fact rows) and deviates only in visibility: the 3 rows land in
`ops.fact_transaction_rejects` with `reject_reason = 'MISSING_DIM_ACCOUNT'` instead of a console stack trace. If an FK
failure occurred mid-batch, legacy behaviour would depend on driver batch semantics; here it is deterministic (every
violating row is rejected, every valid row is loaded).

## Tables
- `silver.transaction`: `transaction_id INT NOT NULL, account_id INT, transaction_date TIMESTAMP, amount DECIMAL(19,4), transaction_type STRING, branch_id INT, _source_system STRING ('sqlserver'|'excel'|'csv'), _source_order INT (1|2|3), _source STRING, _source_file STRING, _ingested_at TIMESTAMP`. Unique on `transaction_id`. Full overwrite.
- `gold.fact_transaction`: ticket 4 DDL, unchanged (`TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID, AccountKey, BranchKey`). Full refresh.
- `ops.fact_transaction_rejects`: ticket 4 DDL, unchanged (fact columns nullable + `reject_reason`, `rejected_at`). Each step replaces only its own rows (`replaceWhere` on `reject_reason`): the gold step owns `FK_REJECT_REASONS`, the silver step everything else, so either step can be rerun alone.

## Run order
After bronze (tickets 2, 3) and `gold.dim_branch` / `gold.dim_account` (tickets 5, 6). `load_fact_transaction` raises if a
dim is empty instead of rejecting every row. Both steps are idempotent.

## Counts on the sample data
| | rows |
|---|---|
| bronze sqlserver / excel / csv | 10 / 7 / 12 |
| duplicates dropped by tUniqRow | 4 (ids 6, 7 Excel; 14, 15 CSV) |
| silver.transaction | 25 (sqlserver 10, excel 5, csv 10) |
| gold.fact_transaction | 22 (= SQL Server parity) |
| ops.fact_transaction_rejects | 3 (`MISSING_DIM_ACCOUNT`: 23, 24, 25) |

## Live validation (Databricks, serverless)
Catalog `migration_demo`, prefix `banking_etl_` (README dev location). Code imported to
`/Workspace/Shared/banking_etl_migration/ticket_8/` (`src/`, `notebooks/silver/silver_transaction`,
`notebooks/gold/load_fact_transaction`), one-time `jobs submit` with the two tasks (silver -> gold), no bundle deploy.
Upstream bronze (10 / 7 / 12 rows) and `gold.dim_account` (21) / `gold.dim_branch` (5) already existed.

| Run | URL | silver | fact | rejects |
|---|---|---|---|---|
| 1 | [889658070082029](https://dbc-8bc9474f-40ae.cloud.databricks.com/?o=7474651138173478#job/927892185320786/run/889658070082029) | 25 | 22 | 3 `MISSING_DIM_ACCOUNT` |
| 2 (rerun) | [553680481387018](https://dbc-8bc9474f-40ae.cloud.databricks.com/?o=7474651138173478#job/607827462166931/run/553680481387018) | 25 | 22 | 3 (one `rejected_at` batch: replaced, not appended) |

After the runs: `migration_demo.banking_etl_gold.fact_transaction` equals `fixtures/parity/fact_transaction.csv`
row for row (22 rows); 0 fact rows whose `AccountKey`/`BranchKey` differ from `dim_account`/`dim_branch`; rejects are
TransactionID 23 (AccountID 22), 24 and 25 (AccountID 23), each with `AccountKey` NULL and `BranchKey` 1.
