# SQL Server -> Delta Lake type mapping (Ticket 2)

Port of `sql_scripts/01_create_tables.sql` (SQL Server DWH star schema) to Delta Lake on
Unity Catalog. The single source of truth is
[`databricks/src/banking_etl/gold/ddl.py`](../databricks/src/banking_etl/gold/ddl.py); the
UC SQL scripts in [`databricks/sql/ddl/`](../databricks/sql/ddl/) are generated from it.

## Data types

| SQL Server | Delta / Spark | Notes |
| --- | --- | --- |
| `INT` | `INT` | 1:1 (32-bit signed). |
| `MONEY` | `DECIMAL(19,4)` | Same scale (4) and covers the full MONEY range (+/-922,337,203,685,477.5807); values round-trip exactly (tested). Spark widens precision for `+ - *` and rounds HALF_UP on `/`, whereas T-SQL keeps MONEY at 4 dp; overflow returns an error under ANSI mode instead of an arithmetic overflow message. Analytics should `CAST(... AS DECIMAL(19,4))` final aggregates when parity to the last digit matters. |
| `DATETIME` | `TIMESTAMP` | T-SQL DATETIME is time-zone naive with ~3.33 ms granularity; Spark TIMESTAMP is microsecond precision with instant semantics rendered in `spark.sql.session.timeZone`. Convention: ingest and query with session time zone `UTC` so wall-clock values (and `CAST(transaction_date AS DATE)` day buckets in `sp_DailyTransaction`) match the legacy DWH. |
| `DATE` | `DATE` | 1:1. |
| `VARCHAR(n)` | `STRING` + `CHECK (col IS NULL OR length(col) <= n)` | Delta has no length-limited string type in the logical schema. The legacy limit is kept as a Delta CHECK constraint (`ck_<table>_<column>_len`, enforced on write) and in the column comment (`Max length n (CHECK). Legacy: <Col> VARCHAR(n).`). Behaviour change: SQL Server truncates/raises "String or binary data would be truncated"; Delta rejects the whole write with `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`. |

## Keys and constraints

| SQL Server | Delta on Unity Catalog | Local OSS Delta (`unity_catalog=False`) |
| --- | --- | --- |
| `PRIMARY KEY` (enforced, clustered index) | `CONSTRAINT pk_<table> PRIMARY KEY (...) NOT ENFORCED` (informational) + `NOT NULL` (enforced) | `NOT NULL` + table property `banking_etl.primary_key` |
| `FOREIGN KEY ... REFERENCES` (enforced) | `CONSTRAINT fk_... FOREIGN KEY (...) REFERENCES <dim> (...) NOT ENFORCED` (informational) | table property `banking_etl.foreign_keys` |
| — | CHECK length constraints (enforced) | same |

PK/FK are **not enforced** on Databricks: uniqueness of the natural key (`tUniqRow` on
`transaction_id`) and referential integrity (fact -> dims) must be guaranteed by the ETL
(MERGE on key, reject/quarantine unmatched keys). `RELY` is intentionally not set so the
optimizer does not assume the constraints hold.

## Naming

Catalog `migration_demo`, schemas `<schema_prefix><layer>` (default prefix `banking_mig_`),
columns are snake_case of the legacy PascalCase names.

| Legacy table | Gold table | Silver table |
| --- | --- | --- |
| `DWH.dbo.DimBranch` | `banking_mig_gold.dim_branch` | `banking_mig_silver.branch` |
| `DWH.dbo.DimAccount` | `banking_mig_gold.dim_account` | `banking_mig_silver.account` |
| `DWH.dbo.DimCustomer` | `banking_mig_gold.dim_customer` | `banking_mig_silver.customer` |
| `DWH.dbo.FactTransaction` | `banking_mig_gold.fact_transaction` | `banking_mig_silver.transaction` |

| Legacy column | Delta column | Delta type |
| --- | --- | --- |
| DimAccount.AccountID (PK) | account_id | INT NOT NULL |
| DimAccount.CustomerID | customer_id | INT |
| DimAccount.AccountType VARCHAR(50) | account_type | STRING, CHECK len <= 50 |
| DimAccount.Balance MONEY | balance | DECIMAL(19,4) |
| DimAccount.DateOpened DATE | date_opened | DATE |
| DimAccount.Status VARCHAR(50) | status | STRING, CHECK len <= 50 |
| DimBranch.BranchID (PK) | branch_id | INT NOT NULL |
| DimBranch.BranchName VARCHAR(100) | branch_name | STRING, CHECK len <= 100 |
| DimBranch.BranchLocation VARCHAR(255) | branch_location | STRING, CHECK len <= 255 |
| DimCustomer.CustomerID (PK) | customer_id | INT NOT NULL |
| DimCustomer.CustomerName VARCHAR(100) | customer_name | STRING, CHECK len <= 100 |
| DimCustomer.Address VARCHAR(255) | address | STRING, CHECK len <= 255 |
| DimCustomer.CityName VARCHAR(100) | city_name | STRING, CHECK len <= 100 |
| DimCustomer.StateName VARCHAR(100) | state_name | STRING, CHECK len <= 100 |
| DimCustomer.Age INT | age | INT |
| DimCustomer.Gender VARCHAR(10) | gender | STRING, CHECK len <= 10 |
| DimCustomer.Email VARCHAR(100) | email | STRING, CHECK len <= 100 |
| FactTransaction.TransactionID (PK) | transaction_id | INT NOT NULL |
| FactTransaction.AccountID (FK DimAccount) | account_id | INT |
| FactTransaction.TransactionDate DATETIME | transaction_date | TIMESTAMP |
| FactTransaction.Amount MONEY | amount | DECIMAL(19,4) |
| FactTransaction.TransactionType VARCHAR(50) | transaction_type | STRING, CHECK len <= 50 |
| FactTransaction.BranchID (FK DimBranch) | branch_id | INT |

Silver tables carry exactly the gold columns (same types, constraints and PK) plus two
lineage columns: `_source_system STRING NOT NULL` (`sqlserver` / `excel` / `csv`) and
`_ingested_at TIMESTAMP`.

## Source-side types (for silver casts)

From the Talend `Sample_DB` connection metadata (source `sample` DB), the silver
loaders must cast: `account.balance INT` and `transaction_db.amount INT` -> `DECIMAL(19,4)`;
`account.date_opened DATETIME2` -> `DATE`; `customer.age VARCHAR(3)` -> `INT`;
`transaction_db.transaction_date DATETIME2` -> `TIMESTAMP`; CSV `transaction_date`
(`dd-MM-yyyy HH:mm:ss` string) -> `TIMESTAMP`.

## Delta table properties

| Property | Value | Why |
| --- | --- | --- |
| `delta.enableChangeDataFeed` | `true` | Row-level change feed so gold/incremental consumers read only changed rows from silver/gold. |
| `delta.autoOptimize.optimizeWrite` | `true` | Right-sizes files written by MERGE/append loads. |
| `delta.autoOptimize.autoCompact` | `true` | Compacts small files after writes without a scheduled OPTIMIZE. |

## Idempotency

`CREATE TABLE IF NOT EXISTS` never alters an existing table; `apply_star_schema` adds only
missing CHECK constraints. Schema changes to an existing table need an explicit
`ALTER TABLE` (or `CREATE OR REPLACE` for a rebuild).
