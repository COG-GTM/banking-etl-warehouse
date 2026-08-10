# Talend Job Mapping Specification (reverse-engineered)

Authoritative source: the `.item` job XML inside `talend_jobs/*.zip`
(`IDX_INTERNSHIP/process/<Job>_0.1.item`) plus
`IDX_INTERNSHIP/metadata/connections/*.item`. Pretty-printed copies of those
files are checked in under `docs/talend_extracted/` (encrypted password blobs
replaced with `[REDACTED-ENCRYPTED-PASSWORD]`).

This document is intended to be sufficient to re-implement the four jobs on
Databricks (tickets 4-8) without access to Talend Studio.

Talend Studio version: TOS_DI 8.0.1. Project: `IDX_INTERNSHIP`. All four jobs
are version 0.1, `jobType="Standard"`, single subjob each, no error-handling
subjobs (no `tLogCatcher` / `tDie` / `tWarn` anywhere).

---

## 1. Source connection metadata

From `metadata/connections/Sample_DB_Connection_0.1.item` and
`metadata/connections/DWH_DB_Connection_0.1.item`.

| Property | `Sample_DB_Connection` | `DWH_DB_Connection` |
|---|---|---|
| Database type | Microsoft SQL Server (`id_MSSQL`, `SQL_SERVER`) | Microsoft SQL Server (`id_MSSQL`, `SQL_SERVER`) |
| Driver class | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| JDBC URL | `jdbc:sqlserver://localhost:1433;DatabaseName=sample;noDatetimeStringSync=true;trustServerCertificate=true;integratedSecurity=true` | `jdbc:sqlserver://localhost:1433;DatabaseName=DWH;noDatetimeStringSync=true;trustServerCertificate=true;integratedSecurity=true` |
| Server / port | `localhost` / `1433` | `localhost` / `1433` |
| Catalog (SID) | `sample` | `DWH` |
| UI schema | `dbo` | *(empty; components use `dbo` implicitly)* |
| Username | empty (Windows integrated security) | empty (Windows integrated security) |
| Password in connection `.item` | empty string | empty string |
| DB product version | 16.00.1000 (SQL Server 2022) | 16.00.1000 (SQL Server 2022) |

Credential handling — **no plaintext passwords exist anywhere in these files**:

* The connection `.item` files store `Username=""` / `Password=""`; auth is
  Windows **integrated security** (`integratedSecurity=true` in
  `AdditionalParams`).
* Every `tMSSqlInput` / `tMSSqlOutput` node nonetheless carries a `PASS`
  element parameter holding a Talend-encrypted blob of the form
  `enc:system.encryption.key.v1:<base64>` (value present but meaningless
  without the Studio key; not reproduced here). The `tFileInputExcel` node in
  `Load_FactTransaction` also carries an encrypted `PASSWORD` parameter (the
  workbook-open password slot) — the workbook in `data_sources/` is not
  encrypted, so this is an unused default.
* Job-level stats/log parameters (`PASS_IMPLICIT_CONTEXT`, `PASS`) likewise
  contain encrypted blobs but stats/log output is disabled
  (`ON_STATCATCHER_FLAG=false`, `ON_LOGCATCHER_FLAG=false`,
  `ON_DATABASE_FLAG=false`).
* `trustServerCertificate=true` means TLS validation was disabled.

### Source catalog `sample`, schema `dbo` (as recorded in the connection metadata)

| Table | Column | Talend type | SQL source type | Length | Precision | Nullable | Key |
|---|---|---|---|---|---|---|---|
| `account` | `account_id` | `id_Integer` | INT | 10 | 0 | no | PK |
| | `customer_id` | `id_Integer` | INT | 10 | 0 | yes | |
| | `account_type` | `id_String` | VARCHAR | 10 | 0 | yes | |
| | `balance` | `id_Integer` | INT | 10 | 0 | yes | |
| | `date_opened` | `id_Date` (pattern `dd-MM-yyyy`) | DATETIME2 | 19 | 0 | yes | |
| | `status` | `id_String` | VARCHAR | 10 | 0 | yes | |
| `branch` | `branch_id` | `id_Integer` | INT | 10 | 0 | no | PK |
| | `branch_name` | `id_String` | VARCHAR | 50 | 0 | yes | |
| | `branch_location` | `id_String` | VARCHAR | 50 | 0 | yes | |
| `city` | `city_id` | `id_Integer` | INT | 10 | 0 | no | PK |
| | `city_name` | `id_String` | VARCHAR | 50 | 0 | yes | |
| | `state_id` | `id_Integer` | INT | 10 | 0 | no | |
| `state` | `state_id` | `id_Integer` | INT | 10 | 0 | no | PK |
| | `state_name` | `id_String` | VARCHAR | 50 | 0 | yes | |
| `customer` | `customer_id` | `id_Integer` | INT | 10 | 0 | no | PK |
| | `customer_name` | `id_String` | VARCHAR | 50 | 0 | yes | |
| | `address` | `id_String` | VARCHAR | 2147483647 (`VARCHAR(MAX)`) | 0 | yes | |
| | `city_id` | `id_Integer` | INT | 10 | 0 | yes | |
| | `age` | `id_String` | VARCHAR | 3 | 0 | yes | |
| | `gender` | `id_String` | VARCHAR | 10 | 0 | yes | |
| | `email` | `id_String` | VARCHAR | 50 | 0 | yes | |
| `transaction_db` | `transaction_id` | `id_Integer` | INT | 10 | 0 | no | PK |
| | `account_id` | `id_Integer` | INT | 10 | 0 | yes | |
| | `transaction_date` | `id_Date` (pattern `dd-MM-yyyy`) | DATETIME2 | 19 | 0 | yes | |
| | `amount` | `id_Integer` | INT | 10 | 0 | yes | |
| | `transaction_type` | `id_String` | VARCHAR | 50 | 0 | yes | |
| | `branch_id` | `id_Integer` | INT | 10 | 0 | yes | |

Note the `age` column is **VARCHAR(3)** in the source, not numeric.

### Common component settings (all four jobs)

* All `tMSSqlInput` / `tMSSqlOutput` nodes: `DRIVER=MSSQL_PROP`,
  `HOST="localhost"`, `PORT="1433"`, `PROPERTIES="noDatetimeStringSync=true;trustServerCertificate=true;integratedSecurity=true"`,
  `ENCODING="ISO-8859-15"`, `USE_EXISTING_CONNECTION=false`
  (each component opens its own connection), `SPECIFY_DATASOURCE_ALIAS=false`.
* Inputs: `DB_SCHEMA="dbo"`, `DBNAME="sample"`, `TRIM_ALL_COLUMN=false`, no
  per-column trim.
* Outputs: `DB_SCHEMA=""`, `DBNAME="DWH"`, `DIE_ON_ERROR=false`,
  `COMMIT_EVERY=10000`, `USE_BATCH_SIZE=true`, `BATCH_SIZE=10000`,
  `IDENTITY_INSERT=false`, `USE_FIELD_OPTIONS=false`
  (so all columns are insertable, no update/delete keys defined),
  `IGNORE_DATE_OUTOF_RANGE=false`, `SUPPORT_NULL_WHERE=false`.
* Every output component exposes a `REJECT` flow (schema = output columns +
  `errorCode VARCHAR(255)`, `errorMessage VARCHAR(255)`), but **no job wires
  the REJECT connector to anything** — rejects are silently discarded and,
  because `DIE_ON_ERROR=false`, failed rows do not abort the job.
* Every `tMap` has `DIE_ON_ERROR=true`, `ENABLE_AUTO_CONVERT_TYPE=false`,
  `LKUP_PARALLELIZE=false`, `ROWS_BUFFER_SIZE=2000000`, and an empty `Var`
  table (no intermediate variables in any job).

---

## 2. `Load_DimBranch`

Context parameter: `namaTabel` = `"DimBranch"` (String, built-in).

### Component graph

| # | Component | Unique name | Label | Role |
|---|---|---|---|---|
| 1 | `tMSSqlInput` | `tDBInput_1` | `branch` | Reads `sample.dbo.branch` |
| 2 | `tMap` | `tMap_1` | | 1:1 rename to DWH column names |
| 3 | `tMSSqlOutput` | `tDBOutput_1` | | Writes `DWH..DimBranch` |

Flow: `tDBInput_1 --row1--> tMap_1 --to_DimBranch--> tDBOutput_1`.

### Source

* Repository schema `Sample_DB_Connection - branch`, `TABLE="branch"`.
* Query (verbatim):

```sql
SELECT dbo.branch.branch_id,
       dbo.branch.branch_name,
       dbo.branch.branch_location
FROM   dbo.branch
```

* No `WHERE` filter, no ordering, no row limit.
* Incoming schema `row1`: `branch_id` INT/`id_Integer`(10, not null, key),
  `branch_name` VARCHAR/`id_String`(50, nullable),
  `branch_location` VARCHAR/`id_String`(50, nullable).

### `tMap_1`

One input table (`row1`, `matchingMode=UNIQUE_MATCH`, `lookupMode=LOAD_ONCE`),
no lookups, no filter, no reject output. Single output table `to_DimBranch`:

| Target column | Type | Nullable | Expression |
|---|---|---|---|
| `BranchID` | `id_Integer` (INT, len 10) | no | `row1.branch_id` |
| `BranchName` | `id_String` (VARCHAR, len 50) | yes | `row1.branch_name` |
| `BranchLocation` | `id_String` (VARCHAR, len 50) | yes | `row1.branch_location` |

No functions applied.

### Target

* `tMSSqlOutput` → database `DWH`, table = `context.namaTabel` → **`DimBranch`**.
* `TABLE_ACTION=CREATE_IF_NOT_EXISTS`, `DATA_ACTION=INSERT` (plain append; no
  truncate, no upsert, no update key).
* `IDENTITY_FIELD` is set to `BranchID` in the XML but
  `SPECIFY_IDENTITY_FIELD=false`, so it is inert.

### Databricks translation notes

* Straight `SELECT ... AS` rename; target `dwh.silver.dim_branch`.
* `INSERT`-only against a `CREATE_IF_NOT_EXISTS` table means re-running the
  Talend job **duplicates rows** and violates the `DimBranch` PK on the second
  run. On Delta prefer an idempotent `MERGE` on `BranchID` (or
  `overwrite`) and record the behaviour change.
* Talend `VARCHAR(50)` lengths are not enforced by Delta `STRING`; the DDL in
  `sql_scripts/01_create_tables.sql` is wider (see §6) so no truncation risk.
* The unused `REJECT` flow has no equivalent; write-time failures in Spark are
  job-fatal rather than per-row skips.

---

## 3. `Load_DimAccount`

Context parameter: `namaTabelTujuan` = `"DimAccount"`.

### Component graph

| # | Component | Unique name | Label | Role |
|---|---|---|---|---|
| 1 | `tMSSqlInput` | `tDBInput_1` | `account` | Reads `sample.dbo.account` |
| 2 | `tMap` | `tMap_1` | | 1:1 rename |
| 3 | `tMSSqlOutput` | `tDBOutput_1` | | Writes `DWH..DimAccount` |

Flow: `tDBInput_1 --row1--> tMap_1 --to_DimAccount--> tDBOutput_1`.

### Source

* Repository schema `Sample_DB_Connection - account`, `TABLE="account"`.

```sql
SELECT dbo.account.account_id,
       dbo.account.customer_id,
       dbo.account.account_type,
       dbo.account.balance,
       dbo.account.date_opened,
       dbo.account.status
FROM   dbo.account
```

* No filter.
* `row1` schema:

| Column | Talend type | Source type | Length | Nullable | Pattern |
|---|---|---|---|---|---|
| `account_id` | `id_Integer` | INT | 10 | no (key) | |
| `customer_id` | `id_Integer` | INT | 10 | yes | |
| `account_type` | `id_String` | VARCHAR | 10 | yes | |
| `balance` | `id_Integer` | INT | 10 | yes | |
| `date_opened` | `id_Date` | DATETIME2 | 19 | yes | `dd-MM-yyyy` |
| `status` | `id_String` | VARCHAR | 10 | yes | |

### `tMap_1`

Single input `row1`, no lookups, no filter, no reject. Output `to_DimAccount`:

| Target column | Type (len, pattern) | Expression |
|---|---|---|
| `AccountID` | `id_Integer` (10), not null | `row1.account_id` |
| `CustomerID` | `id_Integer` (10) | `row1.customer_id` |
| `AccountType` | `id_String` (10) | `row1.account_type` |
| `Balance` | `id_Integer` (10) | `row1.balance` |
| `DateOpened` | `id_Date` (19, `dd-MM-yyyy`) | `row1.date_opened` |
| `Status` | `id_String` (10) | `row1.status` |

No functions applied.

### Target

* Database `DWH`, table = `context.namaTabelTujuan` → **`DimAccount`**.
* `TABLE_ACTION=CREATE_IF_NOT_EXISTS`, `DATA_ACTION=INSERT`.

### Databricks translation notes

* Target `dwh.silver.dim_account`. Same insert-only / re-run duplication
  caveat as `Load_DimBranch`; prefer `MERGE` on `AccountID`.
* **Type widening:** source `balance` is `INT`, but `DimAccount.Balance` is
  `MONEY` → `DECIMAL(19,4)`. Talend performs the implicit int→money widening in
  the JDBC driver; in Spark cast explicitly:
  `CAST(balance AS DECIMAL(19,4))`.
* **Date narrowing:** source `date_opened` is `DATETIME2` carried as Talend
  `id_Date` (java.util.Date, millisecond precision) with display pattern
  `dd-MM-yyyy`, while `DimAccount.DateOpened` is `DATE`. The time component is
  dropped by SQL Server on insert. In Spark: read as `TIMESTAMP`, then
  `CAST(... AS DATE)` for the silver table. Do **not** parse with the
  `dd-MM-yyyy` pattern — that pattern is only Talend's display/format hint for
  a JDBC-typed date column.
* `account_type` and `status` are `VARCHAR(10)` at source but `VARCHAR(50)` in
  the DWH DDL; no truncation.

---

## 4. `Load_DimCustomer`

Context parameter: `namaTabelCustomer` = `"DimCustomer"` (**declared but not
used** — the output component hard-codes the table name; see below).

### Component graph

| # | Component | Unique name | Label | Role |
|---|---|---|---|---|
| 1 | `tMSSqlInput` | `tDBInput_1` | `customer` | Main flow: `sample.dbo.customer` |
| 2 | `tMSSqlInput` | `tDBInput_2` | `city` | Lookup: `sample.dbo.city` |
| 3 | `tMSSqlInput` | `tDBInput_3` | `state` | Lookup: `sample.dbo.state` |
| 4 | `tMap` | `tMap_1` | | 2-step join + UPCASE cleansing |
| 5 | `tMSSqlOutput` | `tDBOutput_1` | | Writes `DWH.DimCustomer` |

Flows:

* `tDBInput_1 --row1--> tMap_1` (main, `lineStyle=0`)
* `tDBInput_2 --row2--> tMap_1` (lookup, `lineStyle=8`)
* `tDBInput_3 --row3--> tMap_1` (lookup, `lineStyle=8`)
* `tMap_1 --to_DimCustomer--> tDBOutput_1`

### Sources

```sql
-- tDBInput_1 (main), TABLE="customer"
SELECT dbo.customer.customer_id,
       dbo.customer.customer_name,
       dbo.customer.address,
       dbo.customer.city_id,
       dbo.customer.age,
       dbo.customer.gender,
       dbo.customer.email
FROM   dbo.customer

-- tDBInput_2 (lookup), TABLE="city"
SELECT dbo.city.city_id,
       dbo.city.city_name,
       dbo.city.state_id
FROM   dbo.city

-- tDBInput_3 (lookup), TABLE="state"
SELECT dbo.state.state_id,
       dbo.state.state_name
FROM   dbo.state
```

No filters on any of the three. Schemas match the source catalog table in §1
(main flow: `address` is `VARCHAR(MAX)` / length `2147483647`, `age` is
`VARCHAR(3)`).

### `tMap_1` join model

| Input table | Role | Join expression | Match mode | Lookup mode | Join type |
|---|---|---|---|---|---|
| `row1` (customer) | main | — | `UNIQUE_MATCH` | `LOAD_ONCE` | — |
| `row2` (city) | lookup 1 | `row2.city_id = row1.city_id` | `UNIQUE_MATCH` | `LOAD_ONCE` | **left outer** (no `innerJoin` flag set) |
| `row3` (state) | lookup 2 | `row3.state_id = row2.state_id` | `UNIQUE_MATCH` | `LOAD_ONCE` | **left outer** (no `innerJoin` flag set) |

Notes on semantics:

* The join is chained: `customer → city` on `city_id`, then `city → state` on
  `state_id`, i.e. `row3` keys off the *lookup* row `row2`, not off `row1`.
* `innerJoin` is absent on both lookup tables → Talend default **Left Outer
  Join**, so customers with an unmatched or NULL `city_id` are kept with
  `CityName`/`StateName` = NULL, and a customer whose city has an unmatched
  `state_id` keeps `StateName` = NULL.
* `UNIQUE_MATCH` = keep only one (the last loaded) matching lookup row; no row
  multiplication even if the lookup has duplicate keys.
* No `expressionFilter`, no "catch lookup inner join reject" output, no
  "die on lookup failure". There is exactly one output table.

### Output table `to_DimCustomer`

| Target column | Type | Length | Nullable | Expression |
|---|---|---|---|---|
| `CustomerID` | `id_Integer` (INT) | 10 | no | `row1.customer_id` |
| `CustomerName` | `id_String` (VARCHAR) | 50 | yes | `StringHandling.UPCASE(row1.customer_name)` |
| `Address` | `id_String` (VARCHAR) | 2147483647 | yes | `StringHandling.UPCASE(row1.address)` |
| `Age` | `id_String` (VARCHAR) | 3 | yes | `row1.age` |
| `Gender` | `id_String` (VARCHAR) | 10 | yes | `StringHandling.UPCASE(row1.gender)` |
| `Email` | `id_String` (VARCHAR) | 50 | yes | `row1.email` |
| `CityName` | `id_String` (VARCHAR) | 50 | yes | `row2.city_name` |
| `StateName` | `id_String` (VARCHAR) | 50 | yes | `row3.state_name` |

Column order in the output schema is as listed (note `CityName`/`StateName`
come **after** `Email`, unlike the DWH DDL order — see §6).

`StringHandling.UPCASE` is Talend's routine
`org.talend.routines.StringHandling.UPCASE(String)`, which returns
`s.toUpperCase()` and **returns `null` for a null input** (null-safe; it does
not throw). Email is deliberately *not* upper-cased; `Age` is passed through
as a string.

### Target

* Database `DWH`, `TABLE="DimCustomer"` (string literal in the component — the
  `namaTabelCustomer` context variable is unused).
* `TABLE_ACTION=CREATE_IF_NOT_EXISTS`, `DATA_ACTION=INSERT`.

### Databricks translation notes

* Target `dwh.silver.dim_customer`. Implement as a broadcast **LEFT JOIN**
  chain, not inner joins:
  `customer LEFT JOIN city USING (city_id) LEFT JOIN state USING (state_id)`
  where the `state` join key comes from `city.state_id`.
* `UNIQUE_MATCH` has no direct Spark equivalent: a duplicate `city_id` in
  `city` would fan out rows in Spark but not in Talend. If duplicate lookup
  keys are possible, dedupe the lookups first
  (e.g. `dropDuplicates(["city_id"])`) to preserve one-row-per-customer
  semantics. "Last one wins" ordering is not reproducible and not meaningful
  here since `city_id`/`state_id` are primary keys in the source.
* `StringHandling.UPCASE` → `upper(col)`; Spark's `upper` is also null-safe, so
  behaviour matches. Note it uses the default JVM locale in Talend vs UTF-8
  simple case mapping in Spark — irrelevant for ASCII data, worth flagging for
  Turkish-locale-style edge cases.
* `Age` is `VARCHAR(3)` at source and Talend keeps it a String, but
  `DimCustomer.Age` is `INT` in the DWH DDL — an **implicit string→int cast is
  performed by SQL Server on insert**. In Spark this must be explicit and
  fails differently: use `try_cast(age AS INT)` (returns NULL) rather than
  `cast`, and consider routing non-numeric ages to a quarantine table since
  Talend would have raised a per-row insert error that is currently swallowed
  by the unwired REJECT flow.
* `Address` is `VARCHAR(MAX)` at source but `VARCHAR(255)` in the DWH →
  **potential silent truncation/insert error today**; Delta `STRING` has no
  limit, so the Databricks table will retain longer values (behaviour change,
  in the safe direction).
* Insert-only → duplicates on re-run; prefer `MERGE` on `CustomerID`.

---

## 5. `Load_FactTransaction`

Context parameter: `namaTabelFakta` = `"FactTransaction"`.

### Component graph

| # | Component | Unique name | Label | Role |
|---|---|---|---|---|
| 1 | `tMSSqlInput` | `tDBInput_1` | `transaction_db` | Source 1: `sample.dbo.transaction_db` |
| 2 | `tFileInputExcel` | `tFileInputExcel_1` | | Source 2: `transaction_excel.xlsx` |
| 3 | `tFileInputDelimited` | `tFileInputDelimited_1` | | Source 3: `transaction_csv.csv` |
| 4 | `tUnite` | `tUnite_1` | | Unions the three streams |
| 5 | `tUniqRow` | `tUniqRow_1` | | Dedupe on `transaction_id` |
| 6 | `tMap` | `tMap_1` | | 1:1 rename |
| 7 | `tMSSqlOutput` | `tDBOutput_1` | | Writes `DWH..FactTransaction` |

Flows (merge order matters):

* `tDBInput_1 --row1--> tUnite_1` (`mergeOrder=1`)
* `tFileInputExcel_1 --row2--> tUnite_1` (`mergeOrder=2`)
* `tFileInputDelimited_1 --row3--> tUnite_1` (`mergeOrder=3`)
* `tUnite_1 --row4--> tUniqRow_1`
* `tUniqRow_1 --row5--> tMap_1` (connector **`UNIQUE`**)
* `tMap_1 --to_FactTransaction--> tDBOutput_1`

### Source 1 — `tMSSqlInput` (`transaction_db`)

```sql
SELECT dbo.transaction_db.transaction_id,
       dbo.transaction_db.account_id,
       dbo.transaction_db.transaction_date,
       dbo.transaction_db.amount,
       dbo.transaction_db.transaction_type,
       dbo.transaction_db.branch_id
FROM   dbo.transaction_db
```

No filter. Schema (this is also the schema `tUnite`/`tUniqRow` propagate):

| Column | Talend type | Source type | Length | Nullable | Pattern |
|---|---|---|---|---|---|
| `transaction_id` | `id_Integer` | INT | 10 | no (key) | |
| `account_id` | `id_Integer` | INT | 10 | yes | |
| `transaction_date` | `id_Date` | DATETIME2 | 19 | yes | `dd-MM-yyyy` |
| `amount` | `id_Integer` | INT | 10 | yes | |
| `transaction_type` | `id_String` | VARCHAR | 50 | yes | |
| `branch_id` | `id_Integer` | INT | 10 | yes | |

### Source 2 — `tFileInputExcel`

* `FILENAME = "C:/Data Rakamin/transaction_excel.xlsx"` (repo copy:
  `data_sources/transaction_excel.xlsx`), `VERSION_2007=true` (XLSX/OOXML).
* `ALL_SHEETS=false`; `SHEETLIST` = one entry `"Sheet1"` with
  `USE_REGEX=true` (so the sheet name is matched as a regex).
* `HEADER=1`, `FOOTER=0`, no `LIMIT`, `FIRST_COLUMN=1`, no `LAST_COLUMN`,
  `AFFECT_EACH_SHEET=false`, `STOPREAD_ON_EMPTYROW=false`,
  `READ_REAL_VALUE=false`, `GENERATION_MODE=USER_MODE`.
* `ENCODING="ISO-8859-15"`, `TRIMALL=false`, `ADVANCED_SEPARATOR=false`
  (thousands `,`, decimal `.` unused), `CONVERTDATETOSTRING=false`.
* `DIE_ON_ERROR=false`; a `REJECT` output exists but is **not connected**.
* An encrypted workbook `PASSWORD` parameter is present (unused default).
* Schema (built-in, no source types / lengths — all `length=-1`,
  `precision=-1`):

| Column | Talend type | Nullable | Pattern |
|---|---|---|---|
| `transaction_id` | `id_Integer` | no (key) | |
| `account_id` | `id_Integer` | yes | |
| `transaction_date` | `id_Date` | yes | `dd-MM-yyyy HH:mm:ss` |
| `amount` | `id_Integer` | yes | |
| `transaction_type` | `id_String` | yes | |
| `branch_id` | `id_Integer` | yes | |

### Source 3 — `tFileInputDelimited`

* `FILENAME = "C:/Data Rakamin/transaction_csv.csv"` (repo copy:
  `data_sources/transaction_csv.csv`).
* `CSV_OPTION=false` (simple delimited mode), `FIELDSEPARATOR=","`,
  `ROWSEPARATOR="\n"`, `HEADER=1`, `FOOTER=0`, no `LIMIT`,
  `REMOVE_EMPTY_ROW=true`, `UNCOMPRESS=false`, `RANDOM=false`,
  `TRIMALL=false`, `CHECK_FIELDS_NUM=false`, `CHECK_DATE=false`,
  `SPLITRECORD=false`, `ENCODING="ISO-8859-15"`, `DIE_ON_ERROR=false`.
  `ESCAPE_CHAR`/`TEXT_ENCLOSURE` are `"` but inactive because `CSV_OPTION` is
  off.
* Same six-column built-in schema as the Excel input, with
  `transaction_date` pattern `dd-MM-yyyy HH:mm:ss` (`originalLength=19`).
  The file's header row confirms the column order:
  `transaction_id,account_id,transaction_date,amount,transaction_type,branch_id`
  and values like `21-01-2024 14:00:00`.
* Unconnected `REJECT` output as above.

### `tUnite_1`

* Straight append of the three inputs in merge order **1) SQL Server
  `transaction_db`, 2) Excel, 3) CSV**. Output schema is the SQL Server
  schema (label `transaction_db`), so file rows are coerced into
  `INT`/`DATETIME2`/`VARCHAR(50)` typing.
* No dedupe, no sort — order within the union is the merge order, and this
  ordering is what determines which duplicate survives downstream.

### `tUniqRow_1`

* `UNIQUE_KEY`: **`transaction_id` only** (`KEY_ATTRIBUTE=true`,
  `CASE_SENSITIVE=false`). The other five columns are listed with
  `KEY_ATTRIBUTE=false` (not part of the key).
* `ONLY_ONCE_EACH_DUPLICATED_KEY=false`,
  `CHANGE_HASH_AND_EQUALS_FOR_BIGDECIMAL=false`, buffer size `M`.
* Two outputs are declared, `UNIQUE` and `DUPLICATE`; **only `UNIQUE` is
  wired** (to `tMap_1` as `row5`). Duplicates are dropped silently.
* Semantics: the **first** occurrence of each `transaction_id` in merge order
  wins → SQL Server beats Excel beats CSV.

### `tMap_1`

Single input `row5`, no lookups, no filter, no reject. Output
`to_FactTransaction`:

| Target column | Type (len, pattern) | Expression |
|---|---|---|
| `TransactionID` | `id_Integer` (10), not null | `row5.transaction_id` |
| `AccountID` | `id_Integer` (10) | `row5.account_id` |
| `TransactionDate` | `id_Date` (19, `dd-MM-yyyy`) | `row5.transaction_date` |
| `Amount` | `id_Integer` (10) | `row5.amount` |
| `TransactionType` | `id_String` (50) | `row5.transaction_type` |
| `BranchID` | `id_Integer` (10) | `row5.branch_id` |

No functions applied.

### Target

* Database `DWH`, table = `context.namaTabelFakta` → **`FactTransaction`**.
* `TABLE_ACTION=TRUNCATE` + `DATA_ACTION=INSERT` → **truncate-and-insert**
  (full reload), unlike the three dimension jobs.
* No update/delete keys; `IDENTITY_INSERT=false`.

### Databricks translation notes

* Target `dwh.gold.fact_transaction`; full reload maps naturally to
  `df.write.mode("overwrite")` / `INSERT OVERWRITE` (Talend `TRUNCATE` +
  `INSERT` is the only job whose re-run semantics are already idempotent).
  Note `TRUNCATE TABLE FactTransaction` would fail in SQL Server while the FKs
  from other tables exist; on Delta the FK constraints are informational only.
* **Union typing:** the three streams must be conformed *before* the union.
  Bronze tables `dwh.bronze.transaction_db`, `dwh.bronze.transaction_excel`,
  `dwh.bronze.transaction_csv` should be cast to the SQL Server schema
  (`INT`, `TIMESTAMP`, `STRING`) and unioned **by name** in the order
  db → excel → csv. `unionByName` (not positional `union`) avoids silent column
  shuffling.
* **Date parsing differs by source:** the DB column is a real `DATETIME2`; the
  CSV and Excel columns are parsed by Talend with pattern
  `dd-MM-yyyy HH:mm:ss` (day-first). In Spark, parse CSV with
  `to_timestamp(transaction_date, 'dd-MM-yyyy HH:mm:ss')` and set
  `spark.sql.legacy.timeParserPolicy` considerations aside by using the
  explicit pattern. Excel cells may arrive as native Excel serial dates via
  POI — read with a library that returns a timestamp (or read as string and
  apply the same pattern); do **not** assume ISO format.
  Talend's `dd-MM-yyyy` display pattern on the DB/target columns is a
  formatting hint only and must not be used for parsing.
* **Amount:** `INT` everywhere in Talend, but `FactTransaction.Amount` is
  `MONEY` → `DECIMAL(19,4)`; cast explicitly.
* **Dedup:** `tUniqRow` on `transaction_id` keeping the *first* row in merge
  order is not expressible with `dropDuplicates` (which is
  non-deterministic). Use
  `row_number() OVER (PARTITION BY transaction_id ORDER BY source_rank)`
  with `source_rank` = 1 (db) / 2 (excel) / 3 (csv), filtered to 1, and add a
  stable tiebreaker (e.g. `monotonically_increasing_id()` within source) so
  the result is reproducible. `CASE_SENSITIVE=false` is irrelevant for an
  integer key.
* Consider materialising the dropped duplicates (the unwired `DUPLICATE`
  output) to a quarantine table — Talend discards them with no audit trail.
* **Referential integrity:** the DDL declares FKs from `FactTransaction` to
  `DimAccount`/`DimBranch`, and the file sources can carry `account_id` /
  `branch_id` values absent from the dimensions. SQL Server would reject those
  rows one-by-one (silently, given `DIE_ON_ERROR=false` and the unwired
  REJECT); Delta does not enforce FKs, so the gold table may end up with more
  rows than the legacy one. Add an explicit anti-join / DQ check if legacy
  parity matters.
* Excel `ISO-8859-15` encoding and the regex sheet-name match (`"Sheet1"` with
  `USE_REGEX=true`) have no Spark analogue; read `Sheet1` explicitly.
  Encoding is irrelevant for XLSX (always UTF-8 internally) but does matter
  for the CSV: read it with `option("encoding", "ISO-8859-15")`.

---

## 6. Cross-check against `sql_scripts/01_create_tables.sql`

Legend: **OK** = name and intent match; ⚠ = discrepancy to handle.

### `DimBranch`

| DDL column | DDL type | Talend output | Talend type | Verdict |
|---|---|---|---|---|
| `BranchID` | `INT PRIMARY KEY` | `BranchID` | `id_Integer` (INT, len 10, not null) | OK |
| `BranchName` | `VARCHAR(100)` | `BranchName` | `id_String` (VARCHAR, len 50) | ⚠ width mismatch (DDL wider than source `branch_name VARCHAR(50)`) — harmless |
| `BranchLocation` | `VARCHAR(255)` | `BranchLocation` | `id_String` (VARCHAR, len 50) | ⚠ same, harmless |

### `DimAccount`

| DDL column | DDL type | Talend output | Talend type | Verdict |
|---|---|---|---|---|
| `AccountID` | `INT PRIMARY KEY` | `AccountID` | INT (10) | OK |
| `CustomerID` | `INT` | `CustomerID` | INT (10) | OK (no FK to `DimCustomer` in the DDL, even though the relationship exists) |
| `AccountType` | `VARCHAR(50)` | `AccountType` | VARCHAR (10) | ⚠ width mismatch, harmless |
| `Balance` | `MONEY` | `Balance` | `id_Integer` | ⚠ **type mismatch** — implicit INT→MONEY cast at insert; make explicit `DECIMAL(19,4)` |
| `DateOpened` | `DATE` | `DateOpened` | `id_Date` from `DATETIME2` | ⚠ **precision loss** — time component silently dropped |
| `Status` | `VARCHAR(50)` | `Status` | VARCHAR (10) | ⚠ width mismatch, harmless |

### `DimCustomer`

| DDL column | DDL type | Talend output | Talend type | Verdict |
|---|---|---|---|---|
| `CustomerID` | `INT PRIMARY KEY` | `CustomerID` | INT (10) | OK |
| `CustomerName` | `VARCHAR(100)` | `CustomerName` | VARCHAR (50), UPCASE | OK |
| `Address` | `VARCHAR(255)` | `Address` | VARCHAR (2147483647 / MAX), UPCASE | ⚠ **narrowing** — source `VARCHAR(MAX)` into `VARCHAR(255)`; addresses >255 chars fail/truncate |
| `CityName` | `VARCHAR(100)` | `CityName` | VARCHAR (50) | ⚠ width mismatch, harmless. Also **column order differs**: DDL is `Address, CityName, StateName, Age, Gender, Email`; the Talend output schema is `Address, Age, Gender, Email, CityName, StateName`. Talend's JDBC insert names columns explicitly so this is safe today, but any positional `INSERT` (or Spark `insertInto` without `byName`) would corrupt the data |
| `StateName` | `VARCHAR(100)` | `StateName` | VARCHAR (50) | ⚠ as above |
| `Age` | `INT` | `Age` | `id_String` (VARCHAR 3) | ⚠ **type mismatch** — implicit string→int cast at insert; non-numeric ages become per-row insert errors that are currently swallowed |
| `Gender` | `VARCHAR(10)` | `Gender` | VARCHAR (10), UPCASE | OK |
| `Email` | `VARCHAR(100)` | `Email` | VARCHAR (50) | OK (not upper-cased) |

### `FactTransaction`

| DDL column | DDL type | Talend output | Talend type | Verdict |
|---|---|---|---|---|
| `TransactionID` | `INT PRIMARY KEY` | `TransactionID` | INT (10) | OK |
| `AccountID` | `INT` (FK → `DimAccount`) | `AccountID` | INT (10) | ⚠ FK not enforced upstream; file sources may reference unknown accounts |
| `TransactionDate` | `DATETIME` | `TransactionDate` | `id_Date` from `DATETIME2` | OK (`DATETIME2`→`DATETIME` narrows fractional-second precision; irrelevant for this data) |
| `Amount` | `MONEY` | `Amount` | `id_Integer` | ⚠ **type mismatch** — implicit INT→MONEY cast |
| `TransactionType` | `VARCHAR(50)` | `TransactionType` | VARCHAR (50) | OK (values seen: `Deposit`, `Withdrawal`, `Transfer`, `Payment`; not normalised/upper-cased, unlike `DimCustomer`) |
| `BranchID` | `INT` (FK → `DimBranch`) | `BranchID` | INT (10) | ⚠ FK not enforced upstream |

### Other discrepancies worth flagging

1. **No `DimCustomer` FK.** `FactTransaction` has FKs to `DimAccount` and
   `DimBranch` only; `DimAccount.CustomerID` has no FK to
   `DimCustomer.CustomerID` even though the join exists logically.
2. **Load order is load-bearing.** Because of the FKs, the README's order
   (`DimBranch` → `DimAccount` → `DimCustomer` → `FactTransaction`) must be
   preserved; `DimAccount` in fact only needs `DimCustomer` logically, not by
   constraint. On Databricks (informational constraints) order still matters
   for correctness of the gold fact build.
3. **Dimension jobs are not idempotent** (`CREATE_IF_NOT_EXISTS` + `INSERT`);
   only the fact job truncates. A second run of any dimension job violates its
   PK. Any Databricks re-implementation should standardise on
   `MERGE`/overwrite and note the deviation.
4. **`namaTabelCustomer` is dead configuration** — `Load_DimCustomer`
   hard-codes `"DimCustomer"` while the other three jobs read their table name
   from context.
5. **All target-name context defaults match the DDL** (`DimBranch`,
   `DimAccount`, `DimCustomer`, `FactTransaction`), so the parameterisation is
   cosmetic.
6. **No stored-procedure logic lives in the Talend jobs** — `sp_DailyTransaction`
   and `sp_BalancePerCustomer` are pure T-SQL in
   `sql_scripts/02_create_procedures.sql` and are out of scope here.

---

## 7. Summary table for the Databricks build

| Talend job | Sources | Key transform | Target (legacy) | Write mode | Target (Databricks) |
|---|---|---|---|---|---|
| `Load_DimBranch` | `sample.dbo.branch` | rename only | `DWH.DimBranch` | insert (create-if-not-exists) | `dwh.silver.dim_branch` |
| `Load_DimAccount` | `sample.dbo.account` | rename only | `DWH.DimAccount` | insert (create-if-not-exists) | `dwh.silver.dim_account` |
| `Load_DimCustomer` | `sample.dbo.customer` + left-join `city` + left-join `state` | `UPCASE` on name/address/gender | `DWH.DimCustomer` | insert (create-if-not-exists) | `dwh.silver.dim_customer` |
| `Load_FactTransaction` | `sample.dbo.transaction_db` + `transaction_excel.xlsx` (Sheet1) + `transaction_csv.csv` | union (db, excel, csv) → dedupe on `transaction_id` (first wins) → rename | `DWH.FactTransaction` | truncate + insert | `dwh.gold.fact_transaction` |

Bronze landing tables implied by the above:
`dwh.bronze.branch`, `dwh.bronze.account`, `dwh.bronze.customer`,
`dwh.bronze.city`, `dwh.bronze.state`, `dwh.bronze.transaction_db`,
`dwh.bronze.transaction_excel`, `dwh.bronze.transaction_csv` — the eight
sources referenced by the README.
