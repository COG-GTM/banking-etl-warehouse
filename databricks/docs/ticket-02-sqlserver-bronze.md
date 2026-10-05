# Ticket 2 — SQL Server → bronze

Full-snapshot extract of the six SQL Server tables read by the Talend jobs into
`bronze.sqlserver_<table>`, in two source modes selected by `Settings.source_mode`.

| Source (`sample.dbo`) | Talend job / component | Bronze table | Rows (`sample.bak`) |
|---|---|---|---|
| `customer` | `Load_DimCustomer` / `tDBInput_1` | `bronze.sqlserver_customer` | 20 |
| `city` | `Load_DimCustomer` / `tDBInput_2` | `bronze.sqlserver_city` | 52 |
| `state` | `Load_DimCustomer` / `tDBInput_3` | `bronze.sqlserver_state` | 9 |
| `account` | `Load_DimAccount` / `tDBInput_1` | `bronze.sqlserver_account` | 21 |
| `branch` | `Load_DimBranch` / `tDBInput_1` | `bronze.sqlserver_branch` | 5 |
| `transaction_db` | `Load_FactTransaction` / `tDBInput_1` | `bronze.sqlserver_transaction_db` | 10 |

## Code
| Path | What |
|---|---|
| `src/banking_etl/bronze/sqlserver.py` | `SOURCE_TABLES` (column lists/types), `ingest_sqlserver`, `ingest_table`, `read_jdbc`, `read_fixture`, `JdbcConnection`, `partition_hint` |
| `src/banking_etl/bronze/fixtures.py` | `stage_fixtures(dest_root, tables)` copies `fixtures/sample_db/<t>.csv` → `<dest_root>/<t>/<t>.csv` |
| `src/banking_etl/bronze/federation.py` + `sql/setup/20_federation_sqlserver.sql` | Guarded Lakehouse Federation DDL (prod alternative) |
| `notebooks/bronze/ingest_sqlserver.py` | Widgets `catalog`, `schema_prefix`, `secret_scope`, `source_mode`, `tables` (default all six), `num_partitions` (default 1), `stage_fixtures` (default false). Exits with `{"table": rows}` JSON. |
| `notebooks/bronze/upload_sqlserver_fixtures.py` | Stages fixtures into the landing volume from inside the workspace |
| `scripts/bronze/upload_fixtures.py` | Same from a laptop via `databricks fs cp` |
| `scripts/bronze/create_federation.py` | Prints (default) or runs (`--execute --warehouse-id`) the federation DDL |

## Mapping decisions (Talend `tMSSqlInput` → Spark)
- **Exact Talend queries.** `SourceTable.query` reproduces each `tMSSqlInput` QUERY (`SELECT dbo.<t>.<col>, … FROM dbo.<t>`) with the same column list and order. It is sent as a JDBC `dbtable` subquery (not the `query` option) so `partitionColumn` hints work.
- **Types preserved from the source** (`INFORMATION_SCHEMA.COLUMNS` of the restored `sample.bak`, identical to the Talend metadata): `int → INT`, `varchar(n)/varchar(max) → STRING`, `datetime2(0) → TIMESTAMP`. `customer.age` stays STRING (`varchar(3)`), and `account.balance` / `transaction_db.amount` are `int` at the source, so they stay INT in bronze; silver casts age to INT and gold casts money to `DECIMAL(19,4)`.
- **No `VARCHAR(n)` in bronze.** Spark 4 JDBC tags varchar columns with `__CHAR_VARCHAR_TYPE_STRING` metadata, which would turn the Delta columns into length-checked `VARCHAR(n)`; aliasing with empty metadata does not remove it. `read_jdbc()` therefore resolves the JDBC schema with `spark.sql.legacy.charVarcharAsString=true` scoped to the `load()` call (restored afterwards), so both modes produce identical STRING columns. `conform()` casts only on type mismatch, because a redundant cast over JDBC columns triggers a Spark 4.0 `SimplifyCasts` plan-validation error.
- **Full snapshots.** Talend re-reads the whole table each run, so every run overwrites the bronze table (`mode=overwrite`, `overwriteSchema=true`). Delta time travel keeps earlier snapshots.
- **Audit columns.** `_ingested_at` is `current_timestamp()`. `_source` is `sqlserver:dbo.<table>` (jdbc) or `fixture:<landing>/sample_db/<table>/` (fixture). Host and database are left out on purpose because they come from the secret scope.
- **Connection.** Secrets `jdbc-host/port/database/user/password` in `Settings.secret_scope`. URL `jdbc:sqlserver://host:port;databaseName=db;encrypt=true;trustServerCertificate=true` mirrors Talend's `trustServerCertificate=true`. User and password go in reader options, never in the URL. Talend's `integratedSecurity=true` (Windows auth) has no equivalent on Databricks, so SQL auth is used.
- **Partitioning hints.** With `num_partitions > 1`, `partition_hint` fetches `MIN/MAX` of the table's integer PK and sets `partitionColumn/lowerBound/upperBound/numPartitions`. At the current volumes (≤ 52 rows) the default is 1.
- **Fixture mode.** CSVs are read with an explicit source-typed schema, `header=true`, `mode=FAILFAST` and `timestampFormat=yyyy-MM-dd HH:mm:ss` from `<landing>/sample_db/<table>/*.csv`. `stage_fixtures` empties other `*.csv` files out of each table folder so the folder holds exactly one snapshot.

## Lakehouse Federation (prod alternative)
`sql/setup/20_federation_sqlserver.sql` creates a `sqlserver` connection (user/password via `secret(...)` from the same scope) and a foreign catalog over the `sample` database. It is **guarded**: `banking_etl.setup.provision` / the setup job never read it, and `scripts/bronze/create_federation.py` only prints the SQL unless you pass `--execute`. Creating it needs metastore `CREATE CONNECTION` + `CREATE FOREIGN CATALOG`, which the dev SP does not have.

To switch the extract to federation, read `<foreign_catalog>.dbo.<table>` (`federation.foreign_table`), select the same `SOURCE_TABLES` column list, and pass it through `conform` / `with_audit_columns` / `write_snapshot`.

**Prefer federation when:** you want governed, lineage-tracked, read-only access in UC without handling JDBC credentials in job code. It also suits ad-hoc analyst queries against the live source, and it fits small/medium tables where predicate pushdown and a SQL-warehouse read are enough, letting you skip snapshotting entirely (or simply `CREATE TABLE … AS SELECT` from the foreign catalog).

**Prefer JDBC (this ticket's default) when:** you need parallel partitioned reads of large tables, custom `fetchsize`/session options, or bulk extracts on job compute. Also use it when the workspace lacks metastore privileges for connections, or when the SQL Server is only reachable from classic job compute networking. Snapshots must be reproducible from a notebook/job without UC admin setup.

## Running
```bash
# dev fixture mode (laptop): stage fixtures, then run the notebook with source_mode=fixture
python scripts/bronze/upload_fixtures.py --catalog migration_demo --schema-prefix banking_etl_
# or set the notebook widget stage_fixtures=true
```

## Tests
- `tests/bronze/test_sqlserver_fixture.py`: fixture mode end to end (row counts, names/types, values, audit columns, overwrite on re-run, mode validation, connection/partition helpers).
- `tests/bronze/test_sqlserver_federation.py`: federation DDL rendering, injection guards, and a check that provisioning does not include it.
- `tests/bronze/test_sqlserver_jdbc.py`: env-gated JDBC run against Docker SQL Server with `sample.bak` restored (row counts, exact equality with the CSV fixtures, 4-partition read, bad-credential failure). It is skipped unless `BANKING_ETL_JDBC_*` is set, the server is reachable, and `SPARK_EXTRA_PACKAGES` includes `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11`. See the module docstring for the Docker commands.
