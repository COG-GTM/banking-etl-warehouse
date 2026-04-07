# Migration Mapping: Legacy SQL Server + Talend → Databricks

This document provides a comprehensive mapping between the legacy ETL system
(SQL Server + Talend Open Studio) and the new Databricks-native pipeline.

---

## 1. Table Mapping

### Source Tables (SQL Server `sample` database → Bronze Layer)

| Legacy SQL Server Table | Databricks Bronze Table | Notes |
|------------------------|------------------------|-------|
| `sample.dbo.customer` | `banking_dwh.bronze.src_customer` | Exported as CSV, ingested as Delta |
| `sample.dbo.city` | `banking_dwh.bronze.src_city` | Exported as CSV, ingested as Delta |
| `sample.dbo.state` | `banking_dwh.bronze.src_state` | Exported as CSV, ingested as Delta |
| `sample.dbo.account` | `banking_dwh.bronze.src_account` | Exported as CSV, ingested as Delta |
| `sample.dbo.branch` | `banking_dwh.bronze.src_branch` | Exported as CSV, ingested as Delta |
| `sample.dbo.transaction` | `banking_dwh.bronze.src_transaction` | Exported as CSV, ingested as Delta |
| `transaction_csv.csv` (flat file) | `banking_dwh.bronze.transactions_csv` | Direct CSV ingestion |
| `transaction_excel.xlsx` (Excel) | `banking_dwh.bronze.transactions_excel` | Excel ingestion via spark-excel or CSV fallback |

### DWH Tables (SQL Server `DWH` database → Silver Layer)

| Legacy DWH Table | Databricks Silver Table | Notebook |
|-----------------|------------------------|----------|
| `DWH.dbo.DimCustomer` | `banking_dwh.silver.dim_customer` | `silver/build_dim_customer.py` |
| `DWH.dbo.DimAccount` | `banking_dwh.silver.dim_account` | `silver/build_dim_account.py` |
| `DWH.dbo.DimBranch` | `banking_dwh.silver.dim_branch` | `silver/build_dim_branch.py` |
| `DWH.dbo.FactTransaction` | `banking_dwh.silver.fact_transaction` | `silver/build_fact_transaction.py` |

### Analytical Objects (Stored Procedures → Gold Layer)

| Legacy Stored Procedure | Databricks Gold Table | Notebook |
|------------------------|----------------------|----------|
| `sp_DailyTransaction` | `banking_dwh.gold.daily_transaction_summary` | `gold/daily_transaction_summary.py` |
| `sp_BalancePerCustomer` | `banking_dwh.gold.balance_per_customer` | `gold/balance_per_customer.py` |

---

## 2. Job Mapping: Talend → Databricks Workflow

| Talend Job | Databricks Workflow Task | Notebook | Dependencies |
|-----------|------------------------|----------|--------------|
| — (manual SQL Server restore) | `setup_source_data` | `setup/upload_source_data.py` | None |
| — (Talend DB connections) | `bronze_ingest_sql_tables` | `bronze/ingest_sql_server_tables.py` | `setup_source_data` |
| — (Talend file input) | `bronze_ingest_csv` | `bronze/ingest_transactions_csv.py` | `setup_source_data` |
| — (Talend Excel input) | `bronze_ingest_excel` | `bronze/ingest_transactions_excel.py` | `setup_source_data` |
| `Load_DimBranch` | `silver_dim_branch` | `silver/build_dim_branch.py` | `bronze_ingest_sql_tables` |
| `Load_DimAccount` | `silver_dim_account` | `silver/build_dim_account.py` | `bronze_ingest_sql_tables` |
| `Load_DimCustomer` | `silver_dim_customer` | `silver/build_dim_customer.py` | `bronze_ingest_sql_tables` |
| `Load_FactTransaction` | `silver_fact_transaction` | `silver/build_fact_transaction.py` | All Bronze tasks |
| `sp_DailyTransaction` (T-SQL) | `gold_daily_transaction_summary` | `gold/daily_transaction_summary.py` | `silver_fact_transaction` |
| `sp_BalancePerCustomer` (T-SQL) | `gold_balance_per_customer` | `gold/balance_per_customer.py` | All Silver dim + fact tasks |

### Execution Order

**Legacy (sequential):**
```
1. Restore sample.bak → SQL Server
2. Run 01_create_tables.sql → Create DWH schema
3. Load_DimBranch (Talend)
4. Load_DimAccount (Talend)
5. Load_DimCustomer (Talend)
6. Load_FactTransaction (Talend)
7. Run 02_create_procedures.sql → Create stored procedures
8. EXEC sp_DailyTransaction / sp_BalancePerCustomer (ad-hoc)
```

**Databricks (parallel where possible):**
```
1. setup_source_data
2. bronze_ingest_sql_tables ─┐
   bronze_ingest_csv ────────┤  (parallel)
   bronze_ingest_excel ──────┘
3. silver_dim_branch ────────┐
   silver_dim_account ───────┤  (parallel, after bronze_ingest_sql_tables)
   silver_dim_customer ──────┘
   silver_fact_transaction ──── (after ALL bronze tasks)
4. gold_daily_transaction_summary ──── (after silver_fact_transaction)
   gold_balance_per_customer ───────── (after silver dims + fact)
```

---

## 3. Transformation Mapping: Talend Component → PySpark/SQL

### Load_DimCustomer

| Talend Component | Purpose | Databricks Equivalent | Code Location |
|-----------------|---------|----------------------|---------------|
| `tDBInput` (customer) | Read customer table from SQL Server | `spark.table("bronze.src_customer")` | `build_dim_customer.py` |
| `tDBInput` (city) | Read city lookup table | `spark.table("bronze.src_city")` | `build_dim_customer.py` |
| `tDBInput` (state) | Read state lookup table | `spark.table("bronze.src_state")` | `build_dim_customer.py` |
| `tMap` (LEFT OUTER JOIN) | Join customer + city + state on city_id and state_id | `.join(df_city, "city_id", "left").join(df_state, "state_id", "left")` | `build_dim_customer.py` |
| `tMap` (UPPERCASE) | Normalize names to uppercase | `upper(col("customer_name"))` | `build_dim_customer.py` |
| `tDBOutput` | Write to DWH.DimCustomer | `.write.format("delta").saveAsTable(...)` | `build_dim_customer.py` |

### Load_DimAccount

| Talend Component | Purpose | Databricks Equivalent | Code Location |
|-----------------|---------|----------------------|---------------|
| `tDBInput` | Read account table | `spark.table("bronze.src_account")` | `build_dim_account.py` |
| `tMap` | Column mapping / passthrough | `.select(col("account_id").alias("AccountID"), ...)` | `build_dim_account.py` |
| `tDBOutput` | Write to DWH.DimAccount | `.write.format("delta").saveAsTable(...)` | `build_dim_account.py` |

### Load_DimBranch

| Talend Component | Purpose | Databricks Equivalent | Code Location |
|-----------------|---------|----------------------|---------------|
| `tDBInput` | Read branch table | `spark.table("bronze.src_branch")` | `build_dim_branch.py` |
| `tMap` | Column mapping / passthrough | `.select(col("branch_id").alias("BranchID"), ...)` | `build_dim_branch.py` |
| `tDBOutput` | Write to DWH.DimBranch | `.write.format("delta").saveAsTable(...)` | `build_dim_branch.py` |

### Load_FactTransaction

| Talend Component | Purpose | Databricks Equivalent | Code Location |
|-----------------|---------|----------------------|---------------|
| `tDBInput` | Read SQL Server transactions | `spark.table("bronze.src_transaction")` | `build_fact_transaction.py` |
| `tFileInputDelimited` | Read CSV transactions | `spark.table("bronze.transactions_csv")` | `build_fact_transaction.py` |
| `tFileInputExcel` | Read Excel transactions | `spark.table("bronze.transactions_excel")` | `build_fact_transaction.py` |
| `tUnite` | Merge all 3 transaction streams | `df_sql.unionByName(df_csv).unionByName(df_excel)` | `build_fact_transaction.py` |
| `tUniqRow` | Deduplicate by transaction_id | `Window + row_number().over(...)` | `build_fact_transaction.py` |
| `tDBOutput` | Write to DWH.FactTransaction | `.write.format("delta").saveAsTable(...)` | `build_fact_transaction.py` |

### Stored Procedures

| Legacy T-SQL | Purpose | Databricks Equivalent | Code Location |
|-------------|---------|----------------------|---------------|
| `CAST(TransactionDate AS DATE)` | Extract date part | `to_date(col("TransactionDate"))` | `daily_transaction_summary.py` |
| `COUNT(TransactionID)` | Count transactions | `count("TransactionID")` | `daily_transaction_summary.py` |
| `SUM(Amount)` | Sum amounts | `spark_sum("Amount")` | `daily_transaction_summary.py` |
| `CASE WHEN TransactionType='Deposit' THEN Amount ELSE -Amount END` | Signed amount logic | `when(col("TransactionType")=="Deposit", col("Amount")).otherwise(-col("Amount"))` | `balance_per_customer.py` |
| `ISNULL(ts.TotalTransactionAmount, 0)` | Null coalesce | `coalesce(col("TotalTransactionAmount"), lit(0))` | `balance_per_customer.py` |
| `a.Status = 'active'` | Active account filter | `.filter(col("a.Status") == "active")` | `balance_per_customer.py` |
| `c.CustomerName LIKE '%' + @customer_name + '%'` | Name search (parameterized) | `WHERE CustomerName LIKE '%JOHN%'` (ad-hoc query) | `balance_per_customer.py` |

---

## 4. Schema Mapping: Legacy Column Types → Delta Column Types

### DimCustomer

| Column | Legacy SQL Server Type | Delta / Spark Type | Notes |
|--------|----------------------|-------------------|-------|
| CustomerID | `INT PRIMARY KEY` | `IntegerType` | PK preserved |
| CustomerName | `VARCHAR(100)` | `StringType` | UPPER() applied during Silver transform |
| Address | `VARCHAR(255)` | `StringType` | |
| CityName | `VARCHAR(100)` | `StringType` | UPPER() applied; joined from city table |
| StateName | `VARCHAR(100)` | `StringType` | UPPER() applied; joined from state table |
| Age | `INT` | `IntegerType` | |
| Gender | `VARCHAR(10)` | `StringType` | |
| Email | `VARCHAR(100)` | `StringType` | |

### DimAccount

| Column | Legacy SQL Server Type | Delta / Spark Type | Notes |
|--------|----------------------|-------------------|-------|
| AccountID | `INT PRIMARY KEY` | `IntegerType` | PK preserved |
| CustomerID | `INT` | `IntegerType` | FK to DimCustomer |
| AccountType | `VARCHAR(50)` | `StringType` | |
| Balance | `MONEY` | `DoubleType` | SQL Server MONEY → Double |
| DateOpened | `DATE` | `DateType` | |
| Status | `VARCHAR(50)` | `StringType` | |

### DimBranch

| Column | Legacy SQL Server Type | Delta / Spark Type | Notes |
|--------|----------------------|-------------------|-------|
| BranchID | `INT PRIMARY KEY` | `IntegerType` | PK preserved |
| BranchName | `VARCHAR(100)` | `StringType` | |
| BranchLocation | `VARCHAR(255)` | `StringType` | |

### FactTransaction

| Column | Legacy SQL Server Type | Delta / Spark Type | Notes |
|--------|----------------------|-------------------|-------|
| TransactionID | `INT PRIMARY KEY` | `IntegerType` | PK preserved; deduplicated |
| AccountID | `INT` (FK) | `IntegerType` | FK to DimAccount |
| TransactionDate | `DATETIME` | `TimestampType` | Parsed from multiple source formats |
| Amount | `MONEY` | `DoubleType` | SQL Server MONEY → Double |
| TransactionType | `VARCHAR(50)` | `StringType` | Values: Deposit, Withdrawal, Transfer, Payment |
| BranchID | `INT` (FK) | `IntegerType` | FK to DimBranch |

### Gold: Daily Transaction Summary

| Column | Legacy SP Output | Delta / Spark Type | Notes |
|--------|-----------------|-------------------|-------|
| Date | `DATE` (CAST) | `DateType` | Truncated from TransactionDate |
| TotalTransactions | `INT` (COUNT) | `LongType` | Count of transactions per day |
| TotalAmount | `MONEY` (SUM) | `DoubleType` | Sum of amounts per day |

### Gold: Balance Per Customer

| Column | Legacy SP Output | Delta / Spark Type | Notes |
|--------|-----------------|-------------------|-------|
| CustomerID | `INT` | `IntegerType` | From DimCustomer |
| CustomerName | `VARCHAR(100)` | `StringType` | Uppercase normalized |
| AccountID | `INT` | `IntegerType` | From DimAccount |
| AccountType | `VARCHAR(50)` | `StringType` | |
| InitialBalance | `MONEY` | `DoubleType` | Original balance from DimAccount |
| CurrentBalance | `MONEY` (computed) | `DoubleType` | InitialBalance + net transactions |

---

## 5. Infrastructure Mapping

| Legacy Component | Databricks Equivalent |
|-----------------|----------------------|
| SQL Server instance | Unity Catalog + Delta Lake |
| SQL Server `sample` database | `banking_dwh` catalog, `bronze` schema |
| SQL Server `DWH` database | `banking_dwh` catalog, `silver`/`gold` schemas |
| Talend Open Studio | Databricks Notebooks (PySpark) |
| Talend Job Scheduler | Databricks Workflows (`workflow.json`) |
| SSMS (ad-hoc queries) | Databricks SQL Editor / SQL Warehouse |
| T-SQL Stored Procedures | Gold layer Delta tables + SQL queries |
| `sample.bak` backup file | CSV exports in Unity Catalog Volume |
| File system (CSV/Excel) | Unity Catalog Volume (`/Volumes/banking_dwh/raw_data/source_files/`) |

---

## 6. Key Behavioral Differences

| Aspect | Legacy System | Databricks Pipeline |
|--------|--------------|-------------------|
| **Execution Model** | Sequential (Talend jobs run one at a time) | Parallel where possible (workflow DAG) |
| **Data Format** | SQL Server tables (row-oriented) | Delta Lake tables (columnar, versioned) |
| **Stored Procedures** | Parameterized, on-demand execution | Pre-materialized Gold tables; filter at query time |
| **Deduplication** | Talend `tUniqRow` (in-memory) | PySpark Window + `row_number()` (distributed) |
| **Schema Enforcement** | SQL Server DDL constraints + FKs | Delta schema enforcement + explicit schema definitions |
| **Data Lineage** | Manual tracking | `_ingestion_timestamp`, `_source_file`, `_source_system` metadata columns |
| **Refresh Strategy** | Full overwrite per Talend run | Full overwrite (extensible to incremental merge) |
| **Time Travel** | Not available | Delta Lake time travel (version history) |
