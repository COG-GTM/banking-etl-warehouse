# Talend → Databricks mapping specification

Reverse-engineered from the committed Talend exports, not from the README. Each
archive in `talend_jobs/` was unzipped and the job XML read directly:

```
talend_jobs/<Job>.zip
  IDX_INTERNSHIP/process/<Job>_0.1.item          # components, schemas, tMap expressions
  IDX_INTERNSHIP/metadata/connections/*.item     # Sample_DB_Connection, DWH_DB_Connection
```

Studio version: `Talend Open Studio for Data Integration-8.0.1.20211109_1610`.
Project: `IDX_INTERNSHIP`. Every component runs with encoding `ISO-8859-15`.

This document is the contract the bronze / silver / gold slices are checked
against. Expressions are quoted verbatim from the `.item` XML (trailing spaces
inside Talend expressions are preserved as they appear).

---

## 1. Connection metadata

### `Sample_DB_Connection` (source)

| Property | Value |
| --- | --- |
| Database type | Microsoft SQL Server (`ProductId=SQL_SERVER`, `DbmsId=id_MSSQL`) |
| Driver | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| URL | `jdbc:sqlserver://localhost:1433;DatabaseName=sample;noDatetimeStringSync=true;trustServerCertificate=true;integratedSecurity=true` |
| Server / Port | `localhost` / `1433` |
| Database (SID) | `sample`, UI schema `dbo` |
| Auth | `integratedSecurity=true` (Windows auth); `Username`/`Password` empty |
| Reported server version | `16.00.1000` (SQL Server 2022) |

Table metadata carried by the connection (schema `dbo`):

| Table | Column | Source type | Length | Talend type | Key / nullable |
| --- | --- | --- | --- | --- | --- |
| `account` | `account_id` | INT | 10 | `id_Integer` | key, not null |
| | `customer_id` | INT | 10 | `id_Integer` | |
| | `account_type` | VARCHAR | 10 | `id_String` | |
| | `balance` | INT | 10 | `id_Integer` | |
| | `date_opened` | DATETIME2 | 19 | `id_Date` | pattern `"dd-MM-yyyy"` |
| | `status` | VARCHAR | 10 | `id_String` | |
| `branch` | `branch_id` | INT | 10 | `id_Integer` | key, not null |
| | `branch_name` | VARCHAR | 50 | `id_String` | |
| | `branch_location` | VARCHAR | 50 | `id_String` | |
| `city` | `city_id` | INT | 10 | `id_Integer` | key, not null |
| | `city_name` | VARCHAR | 50 | `id_String` | |
| | `state_id` | INT | 10 | `id_Integer` | not null |
| `customer` | `customer_id` | INT | 10 | `id_Integer` | key, not null |
| | `customer_name` | VARCHAR | 50 | `id_String` | |
| | `address` | VARCHAR | 2147483647 (`VARCHAR(MAX)`) | `id_String` | |
| | `city_id` | INT | 10 | `id_Integer` | |
| | `age` | VARCHAR | 3 | `id_String` | text in the source |
| | `gender` | VARCHAR | 10 | `id_String` | |
| | `email` | VARCHAR | 50 | `id_String` | |
| `state` | `state_id` | INT | 10 | `id_Integer` | key, not null |
| | `state_name` | VARCHAR | 50 | `id_String` | |
| `transaction_db` | `transaction_id` | INT | 10 | `id_Integer` | key, not null |
| | `account_id` | INT | 10 | `id_Integer` | |
| | `transaction_date` | DATETIME2 | 19 | `id_Date` | pattern `"dd-MM-yyyy"` |
| | `amount` | INT | 10 | `id_Integer` | |
| | `transaction_type` | VARCHAR | 50 | `id_String` | |
| | `branch_id` | INT | 10 | `id_Integer` | |

### `DWH_DB_Connection` (target)

| Property | Value |
| --- | --- |
| URL | `jdbc:sqlserver://localhost:1433;DatabaseName=DWH;noDatetimeStringSync=true;trustServerCertificate=true;integratedSecurity=true` |
| Server / Port | `localhost` / `1433` |
| Database (SID) | `DWH`, no UI schema set (components pass `DB_SCHEMA = ""`) |

The connection carries only the catalog/schema list of `DWH`
(`dbo`, `guest`, `sys`, `INFORMATION_SCHEMA`, the fixed `db_*` roles) — no table
metadata. The target tables are described by each job's `tMSSqlOutput` schema and
by `sql_scripts/01_create_tables.sql`.

Databricks equivalent: both connections disappear. The `sample` database becomes
bronze ingestion (Lakehouse Federation or a JDBC read), and `DWH` becomes the
`banking` Unity Catalog catalog.

---

## 2. `Load_DimBranch`

Flow: `tMSSqlInput (tDBInput_1)` → *row1* → `tMap_1` → *to_DimBranch* → `tMSSqlOutput (tDBOutput_1)`.

Context parameter: `namaTabel = "DimBranch"` (used as the output table name).

Source query (`tDBInput_1`, `sample.dbo`, table `branch`):

```sql
SELECT dbo.branch.branch_id,
		dbo.branch.branch_name,
		dbo.branch.branch_location
FROM	dbo.branch
```

`tMap_1` — single input `row1`, no lookups, no filter, no variables, no reject
output. Output table `to_DimBranch`:

| Output column | Expression | Type |
| --- | --- | --- |
| `BranchID` | `row1.branch_id ` | `id_Integer`, not null |
| `BranchName` | `row1.branch_name ` | `id_String`, nullable |
| `BranchLocation` | `row1.branch_location ` | `id_String`, nullable |

Target `tDBOutput_1`: `DWH`, table `context.namaTabel` (`DimBranch`),
**Action on table `CREATE_IF_NOT_EXISTS`**, **Action on data `INSERT`**,
commit every 10000, batch size 10000, `Die on error = false` (a reject schema
with `errorCode` / `errorMessage` exists but is not wired anywhere).

Databricks: `silver.branch` → `gold.dim_branch`. The insert-only,
create-if-missing behaviour becomes an idempotent `MERGE`/overwrite on
`branch_id` (a re-run of the Talend job duplicates rows; the Delta load must not).

---

## 3. `Load_DimAccount`

Flow: `tMSSqlInput (tDBInput_1)` → *row1* → `tMap_1` → *to_DimAccount* → `tMSSqlOutput (tDBOutput_1)`.

Context parameter: `namaTabelTujuan = "DimAccount"`.

Source query (`sample.dbo`, table `account`):

```sql
SELECT dbo.account.account_id,
		dbo.account.customer_id,
		dbo.account.account_type,
		dbo.account.balance,
		dbo.account.date_opened,
		dbo.account.status
FROM	dbo.account
```

`tMap_1` — single input, no lookups, no filter, pure passthrough:

| Output column | Expression | Type (Talend) |
| --- | --- | --- |
| `AccountID` | `row1.account_id ` | `id_Integer` (INT, not null) |
| `CustomerID` | `row1.customer_id ` | `id_Integer` (INT) |
| `AccountType` | `row1.account_type ` | `id_String` (VARCHAR(10)) |
| `Balance` | `row1.balance ` | `id_Integer` (INT) |
| `DateOpened` | `row1.date_opened ` | `id_Date` (DATETIME2, pattern `"dd-MM-yyyy"`) |
| `Status` | `row1.status ` | `id_String` (VARCHAR(10)) |

Target: `DWH`, table `context.namaTabelTujuan` (`DimAccount`),
`CREATE_IF_NOT_EXISTS` + `INSERT`, `Die on error = false`.

Type drift to be aware of (job schema vs. `sql_scripts/01_create_tables.sql`):
`Balance` travels as `id_Integer` but lands in `MONEY`; `DateOpened` travels as
a 19-char `DATETIME2` but lands in `DATE` — i.e. SQL Server silently truncates
the time part. The Databricks contract keeps that truncation
(`gold.dim_account.date_opened DATE`).

---

## 4. `Load_DimCustomer`

Flow:

```
tMSSqlInput tDBInput_1 (customer) --row1--> tMap_1 --to_DimCustomer--> tMSSqlOutput tDBOutput_1
tMSSqlInput tDBInput_2 (city)     --row2--> tMap_1   (lookup)
tMSSqlInput tDBInput_3 (state)    --row3--> tMap_1   (lookup)
```

Context parameter: `namaTabelCustomer = "DimCustomer"` (note: the output
component hard-codes the literal `"DimCustomer"`, it does **not** use the context
variable, unlike the other three jobs).

Source queries:

```sql
SELECT dbo.customer.customer_id,
		dbo.customer.customer_name,
		dbo.customer.address,
		dbo.customer.city_id,
		dbo.customer.age,
		dbo.customer.gender,
		dbo.customer.email
FROM	dbo.customer
```

```sql
SELECT dbo.city.city_id,
		dbo.city.city_name,
		dbo.city.state_id
FROM	dbo.city
```

```sql
SELECT dbo.state.state_id,
		dbo.state.state_name
FROM	dbo.state
```

`tMap_1` joins:

| Lookup | Join expression | Operator | Match model | Load model | Join type |
| --- | --- | --- | --- | --- | --- |
| `row2` (city) | `row1.city_id ` | `=` | `UNIQUE_MATCH` | `LOAD_ONCE` | left outer (no `innerJoin` attribute set) |
| `row3` (state) | `row2.state_id ` | `=` | `UNIQUE_MATCH` | `LOAD_ONCE` | left outer (chained off the city lookup) |

No `expressionFilter` on any table, no `reject` / `rejectInnerJoin` output, no
`tMap` variables. Output table `to_DimCustomer`:

| Output column | Expression (verbatim) | Type |
| --- | --- | --- |
| `CustomerID` | `row1.customer_id ` | `id_Integer` (INT, not null) |
| `CustomerName` | `StringHandling.UPCASE(row1.customer_name) ` | `id_String` VARCHAR(50) |
| `Address` | `StringHandling.UPCASE(row1.address) ` | `id_String` VARCHAR(MAX) |
| `Age` | `row1.age ` | `id_String` VARCHAR(3) |
| `Gender` | `StringHandling.UPCASE(row1.gender) ` | `id_String` VARCHAR(10) |
| `Email` | `row1.email ` | `id_String` VARCHAR(50) |
| `CityName` | `row2.city_name ` | `id_String` VARCHAR(50) |
| `StateName` | `row3.state_name ` | `id_String` VARCHAR(50) |

Semantics to preserve:

* **Exactly three fields are uppercased**: `CustomerName`, `Address`, `Gender`.
  `Email`, `CityName` and `StateName` are **not** uppercased.
* `StringHandling.UPCASE(null)` returns `null` in Talend, so the uppercasing is
  null-safe — `upper()` in Spark behaves identically.
* `UNIQUE_MATCH` means the *last* matching lookup row wins when a lookup key is
  duplicated; `city_id` / `state_id` are the source PKs, so at most one match
  exists in practice.
* Unmatched customers keep the row with `CityName` / `StateName` null (left
  outer), and a customer whose `city_id` is null also reaches the output.

Target: `DWH.DimCustomer`, `CREATE_IF_NOT_EXISTS` + `INSERT`, `Die on error = false`.
The `tMap` output column order is *CustomerID, CustomerName, Address, Age,
Gender, Email, CityName, StateName*; `DWH.DimCustomer` declares *CityName,
StateName* before *Age, Gender, Email*. Talend writes by name, so the physical
order difference is harmless; `gold.dim_customer` follows the T-SQL order.

---

## 5. `Load_FactTransaction`

Flow:

```
tMSSqlInput tDBInput_1        --row1--\
tFileInputExcel_1             --row2---> tUnite_1 --row4--> tUniqRow_1 --row5 (UNIQUE)--> tMap_1 --> tMSSqlOutput
tFileInputDelimited_1         --row3--/
```

Context parameter: `namaTabelFakta = "FactTransaction"`.

**`tDBInput_1`** (`sample.dbo.transaction_db`):

```sql
SELECT dbo.transaction_db.transaction_id,
		dbo.transaction_db.account_id,
		dbo.transaction_db.transaction_date,
		dbo.transaction_db.amount,
		dbo.transaction_db.transaction_type,
		dbo.transaction_db.branch_id
FROM	dbo.transaction_db
```

Its `transaction_date` is `DATETIME2` with Talend pattern `"dd-MM-yyyy"`.

**`tFileInputDelimited_1`** — `"C:/Data Rakamin/transaction_csv.csv"`
(committed as `data_sources/transaction_csv.csv`), field separator `","`, row
separator `"\n"`, header 1, footer 0, CSV options off, `Die on error = false`.
Schema: `transaction_id` INT, `account_id` INT, `transaction_date` Date with
pattern **`"dd-MM-yyyy HH:mm:ss"`**, `amount` INT, `transaction_type` String,
`branch_id` INT.

**`tFileInputExcel_1`** — `"C:/Data Rakamin/transaction_excel.xlsx"`
(committed as `data_sources/transaction_excel.xlsx`), sheet `"Sheet1"`
(`USE_REGEX=true`, `ALL_SHEETS=false`), header 1, footer 0, first column 1,
`Die on error = false`. Same schema and same `"dd-MM-yyyy HH:mm:ss"` date
pattern as the CSV input.

**`tUnite_1`** — appends the three flows in the order the connections are
declared (`row1` SQL Server, `row2` Excel, `row3` CSV) onto the
`transaction_db` schema. Talend's `tUnite` is a positional `UNION ALL`: it does
not deduplicate and does not reorder columns by name.

**`tUniqRow_1`** — key columns:

| Column | Key | Case sensitive |
| --- | --- | --- |
| `transaction_id` | **yes** | false |
| `account_id`, `transaction_date`, `amount`, `transaction_type`, `branch_id` | no | false |

`ONLY_ONCE_EACH_DUPLICATED_KEY = false`. Only the `UNIQUE` output is wired
(→ `row5`); the `DUPLICATE` output is not consumed, so duplicates are silently
dropped. `tUniqRow` keeps the **first** row it sees for a key, so the winner
depends on the order `tUnite_1` emits its inputs — the connection order in the
XML is `row1` (SQL Server), `row2` (Excel), `row3` (CSV). Duplicate rows that
differ in the non-key columns therefore resolve to the SQL Server version. The
Delta equivalent must make that precedence explicit (e.g. a source-priority
column plus `row_number()`) rather than relying on input order.

**`tMap_1`** — single input `row5`, no lookups/filters, passthrough:

| Output column | Expression | Type |
| --- | --- | --- |
| `TransactionID` | `row5.transaction_id ` | `id_Integer` |
| `AccountID` | `row5.account_id ` | `id_Integer` |
| `TransactionDate` | `row5.transaction_date ` | `id_Date` |
| `Amount` | `row5.amount ` | `id_Integer` |
| `TransactionType` | `row5.transaction_type ` | `id_String` |
| `BranchID` | `row5.branch_id ` | `id_Integer` |

Target `tDBOutput_1`: `DWH`, table `context.namaTabelFakta` (`FactTransaction`),
**Action on table `TRUNCATE`** (the fact table is fully reloaded on each run —
unlike the three dimension jobs), **Action on data `INSERT`**,
`Die on error = false`.

Databricks: truncate + insert becomes a Delta `INSERT OVERWRITE` (or a
`replaceWhere` overwrite) of `gold.fact_transaction`.

---

## 6. Job → Databricks asset map

| Legacy asset | Databricks asset | Owner slice |
| --- | --- | --- |
| `Sample_DB_Connection` (`sample` DB) | `banking.bronze.mssql_*` tables | bronze |
| `transaction_csv.csv` / `transaction_excel.xlsx` | `banking.bronze.csv_transaction` / `banking.bronze.excel_transaction` | bronze |
| `Load_DimBranch` tMap | `silver.branch` → `gold.dim_branch` | silver |
| `Load_DimAccount` tMap | `silver.account` → `gold.dim_account` | silver |
| `Load_DimCustomer` tMap + 2 lookups | `silver.city`, `silver.state`, `silver.customer` → `gold.dim_customer` | silver |
| `tUnite_1` + `tUniqRow_1` + tMap | `silver.transaction` → `gold.fact_transaction` | gold |
| `DWH_DB_Connection` (`DWH` DB) | catalog `banking` | ddl |
| `sql_scripts/01_create_tables.sql` | `databricks/ddl/*.sql` | ddl |
| `sp_DailyTransaction`, `sp_BalancePerCustomer` | gold analytics views / functions | gold |

---

## 7. Type mapping (T-SQL → Delta)

| T-SQL type (legacy) | Talend type | Delta type | Notes |
| --- | --- | --- | --- |
| `INT` | `id_Integer` | `INT` | identical 32-bit range |
| `MONEY` | `id_Integer` (in the jobs) | `DECIMAL(19,4)` | `MONEY` is a scaled 64-bit integer with 4 decimal places; `DECIMAL(19,4)` is the exact equivalent |
| `DATETIME` | `id_Date` | `TIMESTAMP` | Delta `TIMESTAMP` is microsecond, session-time-zone aware; `DATETIME` is 3.33 ms and zone-less |
| `DATETIME2(n)` | `id_Date` | `TIMESTAMP` | same caveat; use `TIMESTAMP_NTZ` if zone-less semantics must be exact |
| `DATE` | `id_Date` | `DATE` | identical |
| `VARCHAR(n)` | `id_String` | `STRING` | Delta has no length limit; the legacy length is recorded in the column comment |
| `VARCHAR(MAX)` | `id_String` (length 2147483647) | `STRING` | |
| `VARCHAR(3)` holding a number (`customer.age`) | `id_String` | `INT` in gold | cast in silver, matching `DWH.DimCustomer.Age INT` |
| `INT PRIMARY KEY` | key column | `INT NOT NULL` + `PRIMARY KEY` informational constraint | not enforced by Databricks |
| `FOREIGN KEY` | n/a | `FOREIGN KEY ... NOT ENFORCED` | informational only |

## 8. Things that do not map 1:1

* **Enforced constraints.** SQL Server rejects a `FactTransaction` row whose
  `AccountID` is missing from `DimAccount`. Unity Catalog constraints are
  `NOT ENFORCED` metadata — referential integrity has to be asserted by the
  pipeline (data-quality checks in the gold slice), not by the engine. The same
  applies to `PRIMARY KEY`: uniqueness of `transaction_id` comes from the
  `tUniqRow` equivalent (`dropDuplicates` / `MERGE`), not from the catalog.
* **`MONEY`.** No Delta equivalent; represented as `DECIMAL(19,4)`. Arithmetic
  differs subtly: SQL Server rounds `MONEY` arithmetic to 4 decimals, Spark
  widens the scale/precision of `DECIMAL` results and can return `NULL`
  (or throw under `spark.sql.ansi.enabled`) on overflow instead of erroring.
* **`IDENTITY`.** The legacy DDL has no `IDENTITY` column (all keys come from
  the source), and the `tMSSqlOutput` components ship with
  `SPECIFY_IDENTITY_FIELD = false`. Delta has no `IDENTITY` in the SQL Server
  sense either; where a surrogate key is ever needed the Databricks equivalent
  is `GENERATED ALWAYS AS IDENTITY`, which guarantees uniqueness but **not**
  gap-free monotonic values.
* **Stored procedures.** `sp_DailyTransaction(@start_date, @end_date)` and
  `sp_BalancePerCustomer(@customer_name)` have no procedural equivalent in
  Unity Catalog. They become parameterised views / SQL UDFs / notebook queries
  in the gold slice. `SET NOCOUNT ON`, `PRINT` and `@@ROWCOUNT`-style side
  effects have no counterpart.
* **`GO` batching.** `GO` is a SQL Server Management Studio batch separator, not
  T-SQL. Databricks executes one statement per `spark.sql()` call; the runner in
  `databricks/ddl/run_ddl.py` splits the `.sql` files on `;` and submits them in
  order.
* **`CREATE DATABASE DWH` / `USE DWH`.** Replaced by a Unity Catalog catalog plus
  three schemas. There is no session-level `USE` in the DDL — every object is
  addressed with its full three-level name.
* **`IF NOT EXISTS (SELECT * FROM sys.databases ...)`.** Replaced by native
  `CREATE ... IF NOT EXISTS`; `sys.*` catalog views become
  `system.information_schema.*`.
* **`CREATE_IF_NOT_EXISTS` + `INSERT` (dimension jobs).** Re-running a Talend
  dimension job appends duplicate rows. The Delta loads are idempotent
  (`MERGE` on the business key), which is a deliberate behaviour change.
* **`Die on error = false` + unwired reject flows.** Bad rows vanished silently
  in Talend. In Databricks the equivalent is an explicit quarantine table or a
  DQ expectation; nothing in this slice reproduces "silently drop".
* **Windows integrated security** (`integratedSecurity=true`) does not exist in
  Databricks; bronze ingestion uses a Unity Catalog connection / secret scope.
* **Absolute Windows paths** (`C:/Data Rakamin/...`) become volume or cloud
  storage paths.
* **Encoding `ISO-8859-15`.** Spark readers are UTF-8 by default; the CSV reader
  in the bronze slice must set `encoding` explicitly if the committed file
  contains non-ASCII bytes.

## 9. Known inconsistencies in the legacy solution

Called out, not silently fixed:

1. `customer.age` is `VARCHAR(3)` in the source and `INT` in `DWH.DimCustomer`,
   while the Talend job passes it through as a string — the cast is implicit in
   the JDBC insert. The Databricks contract casts explicitly in silver and will
   produce `NULL` (not an error) for non-numeric text.
2. `balance` and `amount` are `INT` in the source and in every Talend schema,
   but `MONEY` in `DWH`. Values are whole rupiah; widening to `DECIMAL(19,4)`
   is lossless.
3. `date_opened` is `DATETIME2` in the source and `DATE` in `DWH` — the time
   component is dropped by SQL Server. `gold.dim_account.date_opened` is `DATE`
   per the agreed contract, so silver must cast rather than rely on implicit
   truncation.
4. `transaction_db.transaction_date` carries the Talend pattern `"dd-MM-yyyy"`
   while the two file inputs use `"dd-MM-yyyy HH:mm:ss"`. The pattern is only
   used for text parsing/formatting, so the database rows keep their time part;
   the file rows must be parsed with the seconds-precision pattern.
5. `Load_DimCustomer`'s output component hard-codes `"DimCustomer"` instead of
   using its `namaTabelCustomer` context parameter.
6. There is no `DimCustomer` foreign key anywhere in the legacy schema:
   `DimAccount.CustomerID` is an unconstrained column. The Databricks DDL adds
   `fk_dim_account_dim_customer` as an informational constraint so BI tools can
   see the relationship; it changes no data.

---

## 10. Databricks DDL delivered by this slice

| File | Contents |
| --- | --- |
| `databricks/ddl/00_catalog_and_schemas.sql` | `banking` catalog, `bronze` / `silver` / `gold` schemas |
| `databricks/ddl/01_bronze.sql` | 8 raw tables: 3 transaction sources + 5 SQL Server entities, each with `_ingest_ts` / `_source_file` |
| `databricks/ddl/02_silver.sql` | 6 conformed entities (`account`, `branch`, `city`, `state`, `customer`, `transaction`) |
| `databricks/ddl/03_gold.sql` | the 4 star-schema tables from the contract |
| `databricks/ddl/04_constraints.sql` | PKs and `NOT ENFORCED` FKs (Unity Catalog only) |
| `databricks/ddl/run_ddl.py` | executes the files in dependency order; `--local` mode for a laptop / CI Spark session |

All tables are `USING DELTA`, managed, created with `IF NOT EXISTS`, carry a
column comment recording the legacy T-SQL type, and set
`delta.enableChangeDataFeed = true` so the silver and gold slices can build
incremental loads off the layer below.

`--local` mode exists because open-source Delta has no catalogs and rejects
`PRIMARY KEY` / `FOREIGN KEY` clauses. It rewrites `banking.<schema>.<table>`
to `banking_<schema>.<table>`, skips `CREATE CATALOG`, and skips
`04_constraints.sql`; everything else — types, comments, table properties — is
executed verbatim, which is what `databricks/tests/ddl/test_ddl.py` asserts.
