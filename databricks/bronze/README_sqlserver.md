# Bronze ingestion — SQL Server `sample` database

`ingest_sqlserver.py` is the Databricks replacement for the `tMSSqlInput` components of the
four Talend jobs. It lands the six `dbo` tables of the legacy `sample` database (restored
from `data_sources/sample.bak`) into `dwh.bronze.<source_table>` as raw Delta.

## Source of truth for the schemas

Everything in the manifest was read out of the Talend project archives, not guessed:

```bash
unzip -o talend_jobs/Load_DimCustomer.zip -d /tmp/talend
```

* `IDX_INTERNSHIP/process/<Job>_0.1.item` — the `tMSSqlInput` components carry the exact
  `SELECT` issued by each job.
* `IDX_INTERNSHIP/metadata/connections/Sample_DB_Connection_0.1.item` — carries the whole
  `dbo` catalog (tables, columns, `sourceType`, lengths, PK flags, nullability) plus the
  connection metadata (`com.microsoft.sqlserver.jdbc.SQLServerDriver`, port 1433,
  `UiSchema="dbo"`).

## Manifest

| source table | Talend job | target | PK | partition column | watermark |
| --- | --- | --- | --- | --- | --- |
| `dbo.branch` | `Load_DimBranch` | `dwh.bronze.branch` | `branch_id` | `branch_id` | — |
| `dbo.account` | `Load_DimAccount` | `dwh.bronze.account` | `account_id` | `account_id` | `date_opened` |
| `dbo.customer` | `Load_DimCustomer` | `dwh.bronze.customer` | `customer_id` | `customer_id` | — |
| `dbo.city` | `Load_DimCustomer` (tMap lookup) | `dwh.bronze.city` | `city_id` | `city_id` | — |
| `dbo.state` | `Load_DimCustomer` (tMap lookup) | `dwh.bronze.state` | `state_id` | `state_id` | — |
| `dbo.transaction_db` | `Load_FactTransaction` (tUnite input 1) | `dwh.bronze.transaction_db` | `transaction_id` | `transaction_id` | `transaction_date` |

Source column types as recorded by Talend:

```
account        account_id INT PK, customer_id INT, account_type VARCHAR(10),
               balance INT, date_opened DATETIME2, status VARCHAR(10)
branch         branch_id INT PK, branch_name VARCHAR(50), branch_location VARCHAR(50)
city           city_id INT PK, city_name VARCHAR(50), state_id INT NOT NULL
customer       customer_id INT PK, customer_name VARCHAR(50), address VARCHAR(MAX),
               city_id INT, age VARCHAR(3), gender VARCHAR(10), email VARCHAR(50)
state          state_id INT PK, state_name VARCHAR(50)
transaction_db transaction_id INT PK, account_id INT, transaction_date DATETIME2,
               amount INT, transaction_type VARCHAR(50), branch_id INT
```

Two of those are worth flagging for the silver tickets, because bronze deliberately does
not fix them:

* `customer.age` is `VARCHAR(3)` in the source but `INT` in `DimCustomer`.
* `account.balance` and `transaction_db.amount` are `INT` in the source but `MONEY` in the
  DWH DDL (`DECIMAL(19,4)` per the agreed type mapping).

Bronze keeps the driver's types verbatim; the cast is silver's job.

The CSV and Excel transaction streams (`tFileInputDelimited` / `tFileInputExcel` in
`Load_FactTransaction`) are **not** handled here — they are ticket 5 and land as
`dwh.bronze.transaction_csv` / `dwh.bronze.transaction_excel`.

## Secrets

Read from `dbutils.secrets.get(scope="dwh", key=...)`, per environment (`<key>-<env>` with a
fallback to the unsuffixed key):

| key | required | default |
| --- | --- | --- |
| `sqlserver-host-<env>` | yes | — |
| `sqlserver-port-<env>` | no | `1433` |
| `sqlserver-database-<env>` | no | the `source_database` widget |
| `sqlserver-user-<env>` | yes | — |
| `sqlserver-password-<env>` | yes | — |
| `sqlserver-encrypt-<env>` | no | `true` |
| `sqlserver-trust-server-certificate-<env>` | no | `false` |

```bash
databricks secrets create-scope dwh
databricks secrets put --scope dwh --key sqlserver-host-dev
```

The Talend connection used `trustServerCertificate=true;integratedSecurity=true` against
`localhost:1433`. That is a laptop configuration; here TLS certificate validation is on by
default and disabling it is an explicit per-environment opt-in that logs a warning.

## Parallel reads

For each table the notebook probes `SELECT COUNT_BIG(*), MIN(pk), MAX(pk)` (with the
incremental predicate applied, if any) and derives `lowerBound`/`upperBound`/`numPartitions`
from the result — nothing is hard-coded. It falls back to a **single-partition read** when:

* the `num_partitions` widget is `1`;
* the table has no numeric partition column;
* the probe returns degenerate bounds (empty table, or `MIN == MAX`);
* the row count is below the table's `parallel_read_threshold` (1M by default, 200k for
  `transaction_db`) — splitting a small table costs more in connections than it saves.

`fetchsize` defaults to 10000. The SQL Server driver's default of 0 lets the driver pick,
which is effectively row-at-a-time for large result sets and makes wide scans network-bound.
Lower it if executors show memory pressure on wide rows.

## Load modes

* `full_refresh` — `overwrite` with `mergeSchema` and `overwriteSchema` both off, so an
  unexpected source schema change fails the run instead of silently reshaping bronze.
* `incremental` — reads `MAX(<watermark>)` back from the bronze table (no external state,
  so a re-run after a failure resumes from what actually landed) and `MERGE`s on the
  primary key. The predicate is `>=`, not `>`; same-timestamp rows are re-read and
  collapsed by the merge, which is cheaper than risking a dropped row. Tables with no
  watermark column are fully refreshed even in this mode, and say so in the log.

`incremental_start` overrides the derived watermark for backfills.

`account`'s watermark (`date_opened`) only advances for *new* accounts, so an incremental
run will not pick up balance or status changes on existing rows. Run `full_refresh` for
`account` when those matter — the source has no modified-at column to do better.

## Audit columns

`_ingested_at` (one timestamp for the whole run), `_source_system` = `sqlserver_sample`,
`_source_table` = `sample.dbo.<table>`.

## Failure behaviour

A missing source table or a missing manifest column raises with the table name, what was
found, and what to do about it. Table failures are collected and re-raised together at the
end, so one bad table does not hide the state of the other five. JDBC probes and reads are
retried three times with exponential backoff; writes twice.

## Validation status

No SQL Server, Databricks workspace, or Spark cluster was available in the session that
wrote this. What was validated locally:

* `python -m py_compile databricks/bronze/ingest_sqlserver.py`.
* Executed end-to-end under local PySpark 3.5.1 with stub `dbutils`/`spark` in `dry_run`
  mode: widgets, manifest, table selection and the run loop all execute.
* `add_audit_columns` against a small local DataFrame — verified the three audit columns and
  that the source columns and types are untouched.
* `plan_partitions` / `build_select` exercised for the small, large and degenerate-bounds
  cases.

Not validated: any actual JDBC connection, the `INFORMATION_SCHEMA` probes, the Delta
writes, and the `MERGE`.
