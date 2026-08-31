# Legacy → dbt mapping

Exhaustive mapping of every legacy artifact in this repository (4 Talend jobs,
4 T-SQL tables, 2 stored procedures, every column, every key) to its dbt +
Databricks SQL counterpart.

Target names below are the agreed program-wide names. Several of the dbt files
are delivered by sibling tickets, so a name may be documented here before the
file exists on `main`.

## 1. Artifact overview

| Legacy artifact | Kind | dbt counterpart | Materialisation |
| --- | --- | --- | --- |
| `Load_DimBranch` (Talend) | job | `stg_sample__branch` → `dim_branch` | view → table |
| `Load_DimAccount` (Talend) | job | `stg_sample__account` → `dim_account` | view → table |
| `Load_DimCustomer` (Talend) | job | `stg_sample__customer`, `stg_sample__city`, `stg_sample__state` → `int_customer_enriched` → `dim_customer` | view → view → table |
| `Load_FactTransaction` (Talend) | job | `stg_sample__transaction`, `stg_seed__transaction_csv`, `stg_seed__transaction_excel` → `int_transactions_unioned` → `fct_transaction` | view → view → table |
| `DimBranch` (T-SQL table) | table | `dim_branch` | Delta table |
| `DimAccount` (T-SQL table) | table | `dim_account` | Delta table |
| `DimCustomer` (T-SQL table) | table | `dim_customer` | Delta table |
| `FactTransaction` (T-SQL table) | table | `fct_transaction` | Delta table |
| `sp_DailyTransaction` | stored procedure | `rpt_daily_transaction` | Delta table |
| `sp_BalancePerCustomer` | stored procedure | `rpt_balance_per_customer` | Delta table |
| SQL Agent / manual job order | orchestration | `dbt build` DAG order (inferred from `ref()`) | — |
| `PRIMARY KEY` constraints | DDL | `unique` + `not_null` tests in `schema.yml` | — |
| `FOREIGN KEY` constraints | DDL | `relationships` tests in `schema.yml` | — |

## 2. Sources and landing

| Legacy source | Type | dbt declaration |
| --- | --- | --- |
| `sample.dbo.customer` | SQL Server table | `source('sample', 'customer')` — Unity Catalog external table `banking.raw.customer` |
| `sample.dbo.city` | SQL Server table | `source('sample', 'city')` |
| `sample.dbo.state` | SQL Server table | `source('sample', 'state')` |
| `sample.dbo.account` | SQL Server table | `source('sample', 'account')` |
| `sample.dbo.branch` | SQL Server table | `source('sample', 'branch')` |
| `sample.dbo.transaction_db` | SQL Server table | `source('sample', 'transaction_db')` |
| `data_sources/transaction_csv.csv` | CSV file | dbt seed `transaction_csv` (`banking.raw`) |
| `data_sources/transaction_excel.xlsx` | Excel file | dbt seed `transaction_excel` (`banking.raw`, converted to CSV on landing) |

Sources are declared in `dbt/models/staging/_sources.yml` (catalog `banking`,
schema `raw`). No ingestion tool is part of this program: the SQL Server tables
are exposed to Unity Catalog as external tables and the two files land as seeds.

## 3. Type mapping

| SQL Server | Databricks / dbt |
| --- | --- |
| `INT` | `INT` |
| `VARCHAR(n)` | `STRING` |
| `VARCHAR(MAX)` | `STRING` |
| `MONEY` | `DECIMAL(18,2)` |
| `DATE` | `DATE` |
| `DATETIME` / `DATETIME2` | `TIMESTAMP` |
| `BIT` | `BOOLEAN` |

`MONEY` is a 4-decimal fixed-point type in SQL Server, but every amount in this
warehouse is a whole rupiah value, so `DECIMAL(18,2)` is lossless here and keeps
sums exact (never use `DOUBLE` for money on Databricks).

## 4. Table and column mapping

### 4.1 `DimBranch` → `dim_branch`

| Legacy column | Legacy type | dbt column | dbt type | Test |
| --- | --- | --- | --- | --- |
| `BranchID` | `INT PRIMARY KEY` | `branch_id` | `INT` | `unique`, `not_null` |
| `BranchName` | `VARCHAR(100)` | `branch_name` | `STRING` | — |
| `BranchLocation` | `VARCHAR(255)` | `branch_location` | `STRING` | — |

Source columns `branch.branch_id/branch_name/branch_location` pass straight
through `stg_sample__branch` (rename is a no-op; the staging model only casts).

### 4.2 `DimAccount` → `dim_account`

| Legacy column | Legacy type | dbt column | dbt type | Test |
| --- | --- | --- | --- | --- |
| `AccountID` | `INT PRIMARY KEY` | `account_id` | `INT` | `unique`, `not_null` |
| `CustomerID` | `INT` | `customer_id` | `INT` | `relationships` → `dim_customer.customer_id` |
| `AccountType` | `VARCHAR(50)` | `account_type` | `STRING` | `accepted_values: ['saving', 'checking']` |
| `Balance` | `MONEY` | `balance` | `DECIMAL(18,2)` | `not_null` |
| `DateOpened` | `DATE` | `date_opened` | `DATE` | — |
| `Status` | `VARCHAR(50)` | `status` | `STRING` | `accepted_values: ['active', 'terminated']` |

The source column `account.balance` is `INT`; the cast to `DECIMAL(18,2)`
happens in `stg_sample__account`, matching the legacy `MONEY` column.
`account.date_opened` is `DATETIME2` in the source and `DATE` in the warehouse —
the truncation the Talend job performed is done by the staging cast.

### 4.3 `DimCustomer` → `dim_customer`

Legacy `Load_DimCustomer` joins three source tables in a `tMap` and uppercases
the text fields. In dbt that is `int_customer_enriched`.

| Legacy column | Legacy type | Source expression | dbt column | dbt type | Test |
| --- | --- | --- | --- | --- | --- |
| `CustomerID` | `INT PRIMARY KEY` | `customer.customer_id` | `customer_id` | `INT` | `unique`, `not_null` |
| `CustomerName` | `VARCHAR(100)` | `upper(customer.customer_name)` | `customer_name` | `STRING` | `not_null` |
| `Address` | `VARCHAR(255)` | `upper(customer.address)` | `address` | `STRING` | — |
| `CityName` | `VARCHAR(100)` | `upper(city.city_name)` via `customer.city_id = city.city_id` | `city_name` | `STRING` | — |
| `StateName` | `VARCHAR(100)` | `upper(state.state_name)` via `city.state_id = state.state_id` | `state_name` | `STRING` | — |
| `Age` | `INT` | `cast(customer.age as int)` (`VARCHAR(3)` in the source) | `age` | `INT` | — |
| `Gender` | `VARCHAR(10)` | `customer.gender` (not uppercased) | `gender` | `STRING` | `accepted_values: ['male', 'female']` |
| `Email` | `VARCHAR(100)` | `customer.email` (not uppercased) | `email` | `STRING` | — |

Cleansing rule carried over verbatim: name, address, city and state are
uppercased; gender and email keep their source casing. Both joins are `LEFT`
joins so a customer with an unknown city is never dropped.

### 4.4 `FactTransaction` → `fct_transaction`

Legacy `Load_FactTransaction` unites three transaction streams (`tUnite`),
deduplicates on `transaction_id` (`tUniqRow`) and loads the fact table.

| Legacy column | Legacy type | dbt column | dbt type | Test |
| --- | --- | --- | --- | --- |
| `TransactionID` | `INT PRIMARY KEY` | `transaction_id` | `INT` | `unique`, `not_null` |
| `AccountID` | `INT FK → DimAccount` | `account_id` | `INT` | `relationships` → `dim_account.account_id` |
| `TransactionDate` | `DATETIME` | `transaction_date` | `TIMESTAMP` | `not_null` |
| `Amount` | `MONEY` | `amount` | `DECIMAL(18,2)` | `not_null` |
| `TransactionType` | `VARCHAR(50)` | `transaction_type` | `STRING` | `accepted_values: ['Deposit', 'Withdrawal', 'Transfer', 'Payment']` |
| `BranchID` | `INT FK → DimBranch` | `branch_id` | `INT` | `relationships` → `dim_branch.branch_id` |
| — (new) | — | `record_source` | `STRING` | `accepted_values: ['sql_server', 'excel', 'csv']` |

`record_source` is the one added column: it is produced by
`int_transactions_unioned` and makes the dedup outcome auditable, which the
Talend job could not show.

Date parsing: the CSV stream uses the mask `dd-MM-yyyy HH:mm:ss` (Talend
`tFileInputDelimited` date pattern). On Databricks that is
`to_timestamp(transaction_date, 'dd-MM-yyyy HH:mm:ss')` in
`stg_seed__transaction_csv`. The Excel and SQL Server streams already carry
timestamps.

### 4.5 Talend component → dbt construct

| Talend component | Job | dbt construct |
| --- | --- | --- |
| `tMSSqlInput` / `tFileInputDelimited` / `tFileInputExcel` | all | `source()` / seed + a `stg_*` model |
| `tMap` (join `customer`+`city`+`state`) | `Load_DimCustomer` | `LEFT JOIN`s in `int_customer_enriched` |
| `tMap` (uppercase expressions) | `Load_DimCustomer` | `upper()` in `int_customer_enriched` |
| `tUnite` | `Load_FactTransaction` | `UNION ALL` in `int_transactions_unioned` |
| `tUniqRow` on `transaction_id` | `Load_FactTransaction` | `row_number() over (partition by transaction_id order by source_priority) = 1` |
| `tMSSqlOutput` (truncate + insert) | all | model materialisation (`table`) |
| Job run order (branch → account → customer → fact) | runbook | DAG order inferred from `ref()` |

Dedup precedence is the `tUnite` input order: `sql_server` (1), `excel` (2),
`csv` (3). Transactions `6` and `7` exist in the SQL Server stream (2022 dates)
and again in the Excel stream (2024 dates); both the legacy warehouse and
`int_transactions_unioned` keep the SQL Server rows. The parity harness pins
this behaviour.

## 5. Stored procedure mapping

### 5.1 `sp_DailyTransaction(@start_date, @end_date)` → `rpt_daily_transaction`

| Legacy output column | dbt column | Expression |
| --- | --- | --- |
| `Date` | `transaction_day` | `cast(transaction_date as date)` |
| `TotalTransactions` | `total_transactions` | `count(transaction_id)` |
| `TotalAmount` | `total_amount` | `sum(amount)` |

| Legacy parameter | dbt var | Default |
| --- | --- | --- |
| `@start_date DATE` | `var('start_date')` | `2024-01-01` |
| `@end_date DATE` | `var('end_date')` | `2024-12-31` |

`SET NOCOUNT ON` and the trailing `ORDER BY` have no dbt equivalent (a table
materialisation has no inherent order); ordering is the BI layer's job. The
`BETWEEN` predicate is kept on `cast(transaction_date as date)` so the inclusive
end-date semantics of the procedure are preserved exactly.

### 5.2 `sp_BalancePerCustomer(@customer_name)` → `rpt_balance_per_customer`

| Legacy output column | dbt column | Expression |
| --- | --- | --- |
| `CustomerName` | `customer_name` | `dim_customer.customer_name` |
| `AccountType` | `account_type` | `dim_account.account_type` |
| `InitialBalance` | `initial_balance` | `dim_account.balance` |
| `CurrentBalance` | `current_balance` | `balance + coalesce(total_transaction_amount, 0)` |

| Legacy construct | dbt equivalent |
| --- | --- |
| `WITH TransactionSummary AS (...)` | identical CTE over `ref('fct_transaction')` |
| `CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END` | unchanged |
| `ISNULL(ts.TotalTransactionAmount, 0)` | `coalesce(..., 0)` |
| `JOIN DimCustomer / DimAccount` | `ref('dim_customer')` / `ref('dim_account')` |
| `a.Status = 'active'` | unchanged |
| `c.CustomerName LIKE '%' + @customer_name + '%'` | `upper(customer_name) like '%' \|\| upper('{{ var("customer_name") }}') \|\| '%'` |
| `@customer_name VARCHAR(100)` | `var('customer_name')`, default `''` (matches every customer) |

**Collation.** SQL Server's default `SQL_Latin1_General_CP1_CI_AS` collation
makes `LIKE` case-insensitive, so `'shelly juwita'` matches the uppercased
`SHELLY JUWITA` stored by `Load_DimCustomer`. Databricks string comparison is
case-sensitive, so both sides are wrapped in `upper()`. Without this the model
would silently return no rows for lower-case input — the
`balance_per_customer__lowercase_input` parity scenario guards it.

**Unfiltered use.** With the default empty `customer_name` the predicate is
`like '%%'`, so the model materialises every active account and the BI layer can
filter. Running with `--vars '{customer_name: Shelly}'` reproduces the
procedure's parameterised behaviour.

## 6. Constraint mapping

Databricks does not enforce primary or foreign keys (they can be declared
`RELY`, but they are informational), so every legacy constraint becomes a dbt
test in the model's `schema.yml`.

| Legacy constraint | dbt tests |
| --- | --- |
| `DimAccount.AccountID PRIMARY KEY` | `unique`, `not_null` on `dim_account.account_id` |
| `DimBranch.BranchID PRIMARY KEY` | `unique`, `not_null` on `dim_branch.branch_id` |
| `DimCustomer.CustomerID PRIMARY KEY` | `unique`, `not_null` on `dim_customer.customer_id` |
| `FactTransaction.TransactionID PRIMARY KEY` | `unique`, `not_null` on `fct_transaction.transaction_id` |
| `FK_FactTransaction_DimAccount` | `relationships` on `fct_transaction.account_id` → `dim_account.account_id` |
| `FK_FactTransaction_DimBranch` | `relationships` on `fct_transaction.branch_id` → `dim_branch.branch_id` |
| implicit `account.customer_id` → `customer` | `relationships` on `dim_account.customer_id` → `dim_customer.customer_id` |

**Known failure.** Three CSV transactions (`transaction_id` 23, 24, 25)
reference `account_id` 22 and 23, which do not exist in the source `account`
table. SQL Server's foreign key would have rejected those rows on load;
Databricks accepts them, so `relationships_fct_transaction_account_id__dim_account`
fails with 3 rows until the source data is fixed. See the cutover runbook for
the accepted handling (`severity: warn` plus a tracked data-quality ticket).

## 7. Naming conventions

* Models: `stg_<source>__<entity>`, `int_<description>`, `dim_<entity>`,
  `fct_<event>`, `rpt_<report>`.
* Columns: legacy `PascalCase` → `snake_case`; `ID` suffix → `_id`.
* Schemas: `banking.raw` (sources/seeds), `..._staging`, `..._intermediate`,
  `..._marts` via the `+schema` config in `dbt/dbt_project.yml`.
* Reports keep the business name of the procedure they replace, minus the `sp_`
  prefix and in `snake_case` (`sp_DailyTransaction` → `rpt_daily_transaction`).
