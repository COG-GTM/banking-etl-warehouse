# T-SQL → Spark SQL / PySpark Equivalence Notes

Scoped to the constructs actually used in `sql_scripts/01_create_tables.sql`,
`sql_scripts/02_create_procedures.sql`, and the implicit conversions performed
by the Talend jobs. Every row cites where in this repo the construct appears.

## 1. Data types

| T-SQL (DDL) | Used by | Spark SQL type | PySpark `DataType` | Notes |
|---|---|---|---|---|
| `INT` | all keys, `Age` | `INT` | `IntegerType()` | 32-bit in both. |
| `MONEY` | `DimAccount.Balance`, `FactTransaction.Amount` | `DECIMAL(19,4)` | `DecimalType(19, 4)` | `MONEY` is a fixed-point 8-byte type with 4 decimal places, range ±922,337,203,685,477.5807 — `DECIMAL(19,4)` covers it exactly. Do **not** use `DOUBLE` (binary rounding breaks `SUM` parity). Talend feeds these columns as Java `Integer`; Spark must `cast("decimal(19,4)")` before writing. |
| `DATETIME` | `FactTransaction.TransactionDate` | `TIMESTAMP` | `TimestampType()` | `DATETIME` has 3.33 ms precision; `TIMESTAMP` is microsecond. Truncate to milliseconds (`date_trunc('millisecond', ..)`) on the SQL Server side when hashing for parity. Spark `TIMESTAMP` is session-timezone-aware; set `spark.sql.session.timeZone` explicitly (the source data has no offsets) — use `TIMESTAMP_NTZ` on DBR ≥ 13.3 if strict wall-clock semantics are preferred. |
| `DATE` | `DimAccount.DateOpened`, proc params | `DATE` | `DateType()` | 1:1. |
| `VARCHAR(n)` | all text columns | `STRING` (or `VARCHAR(n)` on Delta/UC, length is enforced on write) | `StringType()` | Spark strings are unbounded; use `VARCHAR(n)` in the gold DDL only if the write-time length check is wanted. Collation differs — see §4. |

Talend intermediate types (from the `.item` schemas) and their Spark reading types:

| Talend `id_*` | Spark | Note |
|---|---|---|
| `id_Integer` | `INT` | |
| `id_String` | `STRING` | `Age` arrives as `id_String(3)` → `cast("int")` |
| `id_Date` with pattern `dd-MM-yyyy HH:mm:ss` | `TIMESTAMP` via `to_timestamp(col, 'dd-MM-yyyy HH:mm:ss')` | Spark's default parser expects ISO `yyyy-MM-dd`; the CSV is day-first and **will silently null out** without the explicit pattern (or throw under `spark.sql.legacy.timeParserPolicy=EXCEPTION`, which is the default). |
| `id_Date` with pattern `dd-MM-yyyy` | `DATE` via `to_date(col, 'dd-MM-yyyy')` | only relevant if `date_opened` lands as string |

## 2. Functions and expressions

| T-SQL | Where used | Spark SQL | PySpark | Notes |
|---|---|---|---|---|
| `CAST(TransactionDate AS DATE)` | `sp_DailyTransaction` (SELECT, WHERE, GROUP BY) | `to_date(TransactionDate)` or `CAST(TransactionDate AS DATE)` | `F.to_date("TransactionDate")` | Both forms work in Spark; `to_date` is idiomatic. Spark can also `GROUP BY` the alias (`GROUP BY Date`), unlike T-SQL. |
| `x BETWEEN @a AND @b` | `sp_DailyTransaction` | `x BETWEEN :a AND :b` | `F.col("Date").between(a, b)` | Inclusive on both ends in both engines. |
| `COUNT(TransactionID)` | `sp_DailyTransaction` | `count(TransactionID)` | `F.count("TransactionID")` | Non-null count, identical. |
| `SUM(Amount)` on `MONEY` | both procs | `sum(Amount)` on `DECIMAL(19,4)` | `F.sum("Amount")` | Spark widens the result to `DECIMAL(29,4)` (precision +10); cast back to `DECIMAL(19,4)` for schema parity. |
| `CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END` | `sp_BalancePerCustomer` CTE | identical | `F.when(F.col("TransactionType") == "Deposit", F.col("Amount")).otherwise(-F.col("Amount"))` | String equality is **case-sensitive** in Spark — `'deposit'` would be treated as a debit. The mapping spec shows `TransactionType` is not upper-cased by Talend, so either normalise in silver (`initcap`) or compare with `lower(TransactionType) = 'deposit'`. |
| `ISNULL(ts.TotalTransactionAmount, 0)` | `sp_BalancePerCustomer` | `coalesce(ts.TotalTransactionAmount, 0)` | `F.coalesce(F.col("TotalTransactionAmount"), F.lit(0))` | `ISNULL` is a T-SQL-only two-arg function; `coalesce` is the ANSI equivalent. Spark also has `nvl(a, b)` and `ifnull(a, b)` aliases. Keep the literal typed: `F.lit(0).cast("decimal(19,4)")` to avoid an implicit decimal→double widening. |
| `a.Balance + ISNULL(..., 0)` | `sp_BalancePerCustomer` | `a.Balance + coalesce(..., 0)` | column arithmetic | `DECIMAL(19,4) + DECIMAL(29,4)` → `DECIMAL(30,4)`; cast result to `DECIMAL(19,4)`. |
| `c.CustomerName LIKE '%' + @customer_name + '%'` | `sp_BalancePerCustomer` | `upper(c.CustomerName) LIKE concat('%', upper(:name), '%')` or `c.CustomerName ILIKE concat('%', :name, '%')` | `F.upper(F.col("CustomerName")).contains(name.upper())` | `+` string concat → `concat`/`||`. `contains` is the idiomatic PySpark form for `%x%`. Case-insensitivity must be made explicit (§4). `ILIKE` is available in Spark ≥ 3.3. |
| `a.Status = 'active'` | `sp_BalancePerCustomer` | `lower(a.Status) = 'active'` | `F.lower(F.col("Status")) == "active"` | Same collation caveat. |
| `WITH cte AS (...)` | `sp_BalancePerCustomer` | identical | build a DataFrame and join | Spark SQL supports CTEs; in PySpark express as an intermediate DataFrame. |
| `JOIN ... ON` / `LEFT JOIN ... ON` | `sp_BalancePerCustomer`, tMap lookups | identical | `df.join(other, on=..., how="inner"/"left")` | tMap lookups with *Inner join* unchecked = `LEFT JOIN`; `UNIQUE_MATCH` ≈ dedup the lookup side first (`dropDuplicates(["city_id"])`) to avoid fan-out. |
| `ORDER BY [Date]` | `sp_DailyTransaction` | `ORDER BY Date` | `.orderBy("Date")` | Square-bracket identifiers → backticks `` `Date` `` if quoting is needed (`Date` is not reserved in Spark). |
| `SET NOCOUNT ON` | both procs | – | – | No equivalent needed. |
| `StringHandling.UPCASE(x)` (Talend) | `Load_DimCustomer` | `upper(x)` | `F.upper("x")` | Both return `NULL` for `NULL`. |
| `tUnite` | `Load_FactTransaction` | `UNION ALL` | `df1.unionByName(df2)` | Use `unionByName` (not positional `union`) for safety. |
| `tUniqRow` on `transaction_id` | `Load_FactTransaction` | `ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY <priority>) = 1` | `F.row_number().over(Window.partitionBy("transaction_id").orderBy("_src_priority"))` | `dropDuplicates(["transaction_id"])` is equivalent but non-deterministic about which row survives; use the window form with an explicit `ORDER BY`. |
| `TRUNCATE TABLE` + `INSERT` (tMSSqlOutput) | `Load_FactTransaction` | `INSERT OVERWRITE` / `CREATE OR REPLACE TABLE ... AS` | `df.write.mode("overwrite").saveAsTable(...)` | Delta overwrite is transactional (no window with an empty table, unlike TRUNCATE+INSERT). |

## 3. Stored procedures and parameters

Databricks SQL has no `CREATE PROCEDURE` with `EXEC`-style semantics in the DBR
versions targeted here. Options, in order of preference:

1. **Materialised gold table + predicate at query time** (`gold.daily_transaction`,
   `gold.balance_per_customer`) — simplest, works with BI tools, and the result is
   reproducible. Parameters become `WHERE` clauses:
   ```sql
   SELECT * FROM gold.daily_transaction WHERE Date BETWEEN :start_date AND :end_date ORDER BY Date;
   ```
2. **SQL table-valued function** (`CREATE FUNCTION ... RETURNS TABLE`) when callers
   want the procedure signature:
   ```sql
   SELECT * FROM gold.fn_balance_per_customer('john');
   ```
3. **Parameter markers** in Databricks SQL / notebooks (`:start_date`, `spark.sql(sql, args={...})`
   on Spark ≥ 3.4) for ad-hoc use.

Parameter type mapping: `@start_date DATE` → `DATE` literal / `datetime.date`;
`@customer_name VARCHAR(100)` → `STRING` / `str`.

## 4. Collation and case sensitivity

SQL Server default collation (`SQL_Latin1_General_CP1_CI_AS`) is
**case-insensitive**; Spark string comparison is **case-sensitive** (unless a
UTF8_LCASE collation is applied on DBR ≥ 16 / Spark 4). Every equality or `LIKE`
on text in this repo therefore needs explicit normalisation:

| Legacy predicate | Spark-safe form |
|---|---|
| `TransactionType = 'Deposit'` | `lower(TransactionType) = 'deposit'` (or normalise in silver) |
| `Status = 'active'` | `lower(Status) = 'active'` |
| `CustomerName LIKE '%x%'` | `upper(CustomerName) LIKE concat('%', upper('x'), '%')` — `CustomerName` is already upper-cased by Talend, so upper-casing the parameter is sufficient |

Trailing spaces: SQL Server `=` ignores trailing spaces on `VARCHAR`; Spark does
not. The Talend inputs have `TRIM = false`, so any trailing spaces in `sample`
will carry through — add `rtrim` in silver if parity tests show mismatches.

## 5. Constraints and integrity

| T-SQL | Where | Delta Lake / Unity Catalog | Enforcement |
|---|---|---|---|
| `AccountID INT PRIMARY KEY` (and other PKs) | `01_create_tables.sql` | `ALTER TABLE t ALTER COLUMN AccountID SET NOT NULL` + `ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (AccountID)` | `NOT NULL` is enforced on write. UC `PRIMARY KEY` is **informational only** (not enforced). Uniqueness must be asserted in the pipeline: `assert df.count() == df.select(pk).distinct().count()`, or a DLT expectation `EXPECT (row_number() OVER (...) = 1) ON VIOLATION FAIL UPDATE`. |
| `CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (AccountID) REFERENCES DimAccount(AccountID)` | `01_create_tables.sql` | `ALTER TABLE gold.FactTransaction ADD CONSTRAINT fk_fact_account FOREIGN KEY (AccountID) REFERENCES gold.DimAccount(AccountID)` | Informational only. Enforce with an anti-join before write (`fact.join(dim, "AccountID", "left_anti").count() == 0`) or route orphans to a rejects table; DLT: `EXPECT AccountID IN (SELECT AccountID FROM LIVE.DimAccount)` is not supported directly — join to the dim and `EXPECT dim_key IS NOT NULL`. |
| `CONSTRAINT FK_FactTransaction_DimBranch ...` | same | same pattern on `BranchID` | |
| Domain check (none in DDL, implied by proc) | `TransactionType` | `ALTER TABLE gold.FactTransaction ADD CONSTRAINT chk_tx_type CHECK (TransactionType IN ('Deposit','Withdrawal','Transfer','Payment'))` | Delta `CHECK` constraints **are enforced** on write. |
| `MONEY` non-negativity (none in DDL) | `Amount` | `CHECK (Amount >= 0)` | Enforced; optional. |
| `IDENTITY` (tMSSqlOutput `IDENTITY_FIELD`) | all outputs | `GENERATED ALWAYS AS IDENTITY` | Not needed — the jobs pass the natural key through; the identity setting had no effect. |

## 6. DDL skeleton (Spark SQL / Delta)

```sql
CREATE TABLE IF NOT EXISTS gold.DimAccount (
  AccountID    INT           NOT NULL,
  CustomerID   INT,
  AccountType  STRING,
  Balance      DECIMAL(19,4),
  DateOpened   DATE,
  Status       STRING,
  CONSTRAINT pk_dimaccount PRIMARY KEY (AccountID)
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.DimBranch (
  BranchID       INT NOT NULL,
  BranchName     STRING,
  BranchLocation STRING,
  CONSTRAINT pk_dimbranch PRIMARY KEY (BranchID)
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.DimCustomer (
  CustomerID   INT NOT NULL,
  CustomerName STRING,
  Address      STRING,
  CityName     STRING,
  StateName    STRING,
  Age          INT,
  Gender       STRING,
  Email        STRING,
  CONSTRAINT pk_dimcustomer PRIMARY KEY (CustomerID)
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.FactTransaction (
  TransactionID   INT           NOT NULL,
  AccountID       INT,
  TransactionDate TIMESTAMP,
  Amount          DECIMAL(19,4),
  TransactionType STRING,
  BranchID        INT,
  CONSTRAINT pk_facttransaction PRIMARY KEY (TransactionID),
  CONSTRAINT fk_fact_account FOREIGN KEY (AccountID) REFERENCES gold.DimAccount(AccountID),
  CONSTRAINT fk_fact_branch  FOREIGN KEY (BranchID)  REFERENCES gold.DimBranch(BranchID)
) USING DELTA;

ALTER TABLE gold.FactTransaction ADD CONSTRAINT chk_tx_type
  CHECK (TransactionType IN ('Deposit','Withdrawal','Transfer','Payment'));
```

Local-test fallback: the `PRIMARY KEY` / `FOREIGN KEY` clauses require Unity
Catalog; when running on plain open-source Spark (parquet or `delta-spark`),
create the tables without those clauses and rely on the pipeline-side assertions
listed in §5.

## 7. PySpark equivalents of the two procedures

```python
from pyspark.sql import functions as F

def daily_transaction(fact, start_date, end_date):
    return (fact.withColumn("Date", F.to_date("TransactionDate"))
                .filter(F.col("Date").between(start_date, end_date))
                .groupBy("Date")
                .agg(F.count("TransactionID").alias("TotalTransactions"),
                     F.sum("Amount").cast("decimal(19,4)").alias("TotalAmount"))
                .orderBy("Date"))

def balance_per_customer(dim_customer, dim_account, fact, customer_name):
    summary = (fact.groupBy("AccountID")
                   .agg(F.sum(F.when(F.lower("TransactionType") == "deposit", F.col("Amount"))
                               .otherwise(-F.col("Amount"))).alias("TotalTransactionAmount")))
    return (dim_customer.alias("c")
            .join(dim_account.alias("a"), F.col("c.CustomerID") == F.col("a.CustomerID"))
            .join(summary.alias("ts"), F.col("a.AccountID") == F.col("ts.AccountID"), "left")
            .filter(F.upper("c.CustomerName").contains(customer_name.upper())
                    & (F.lower("a.Status") == "active"))
            .select(F.col("c.CustomerName"),
                    F.col("a.AccountType"),
                    F.col("a.Balance").alias("InitialBalance"),
                    (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"),
                                                     F.lit(0).cast("decimal(19,4)")))
                        .cast("decimal(19,4)").alias("CurrentBalance")))
```
