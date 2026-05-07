# SSIS/ETL Package Inventory & Migration Assessment for Azure Data Factory

> **Project:** Banking ETL/Data Warehouse Migration to Azure  
> **Source Platform:** Talend Open Studio for Data Integration 8.0.1  
> **Target Platform:** Azure Data Factory (ADF) + Azure SQL  
> **Assessment Date:** 2026-05-07  
> **Document Version:** 1.0

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Complete ETL Catalog](#2-complete-etl-catalog)
3. [Migration Classification](#3-migration-classification)
4. [Stored Procedure Assessment](#4-stored-procedure-assessment)
5. [Data Source Assessment](#5-data-source-assessment)
6. [Risk & Complexity Matrix](#6-risk--complexity-matrix)
7. [Recommended Migration Sequence](#7-recommended-migration-sequence)
8. [Microsoft Data Migration Assistant Results (Simulated)](#8-microsoft-data-migration-assistant-results-simulated)
9. [Appendix: Talend-to-ADF Component Mapping](#9-appendix-talend-to-adf-component-mapping)

---

## 1. Executive Summary

This document provides a comprehensive inventory and migration assessment for the Banking ETL/Data Warehouse solution currently running on **Talend Open Studio** with a **Microsoft SQL Server** backend. The system implements a Star Schema data warehouse (`DWH`) that consolidates banking data from three source types (SQL Server, Excel, CSV) into four core tables: `DimCustomer`, `DimAccount`, `DimBranch`, and `FactTransaction`.

### Current State Overview

| Attribute | Value |
|---|---|
| ETL Engine | Talend Open Studio for Data Integration 8.0.1 |
| Database Engine | Microsoft SQL Server (on-premises) |
| Number of ETL Jobs | 4 |
| Number of Stored Procedures | 2 |
| Data Sources | 8 distinct sources across 3 connection types |
| DWH Schema | Star Schema (3 Dimensions + 1 Fact) |
| Talend Project Name | `IDX_INTERNSHIP` |

### Migration Recommendation Summary

All four Talend jobs are recommended for **Native ADF Rewrite** using ADF Mapping Data Flows. There are no existing SSIS packages, so Azure-SSIS Integration Runtime is not required. The stored procedures are fully compatible with Azure SQL Database. The estimated total migration effort is **74-110 hours**.

---

## 2. Complete ETL Catalog

### 2.1 Load_DimBranch

| Attribute | Detail |
|---|---|
| **Job Name** | `Load_DimBranch` |
| **Version** | 0.1 |
| **Purpose** | Loads branch master data from the `sample` operational database into the `DimBranch` dimension table in the DWH |
| **Schedule/Trigger** | Manual execution; run first in the load sequence |
| **Dependencies** | DWH database and `DimBranch` table must exist |
| **Complexity Rating** | **Simple** |
| **Source** | SQL Server `sample.dbo.branch` (localhost:1433) |
| **Target** | SQL Server `DWH.dbo.DimBranch` (localhost:1433) |
| **Connection Type** | MSSQL (JDBC, Windows Integrated Authentication) |
| **Row Count (est.)** | Low volume (reference/master data) |

**Talend Components Used:**

| Component | Instance | Purpose |
|---|---|---|
| `tMSSqlInput` | `tDBInput_1` | Reads from `sample.dbo.branch` via SQL query: `SELECT branch_id, branch_name, branch_location FROM dbo.branch` |
| `tMap` | `tMap_1` | Simple 1:1 column mapping: `branch_id` -> `BranchID`, `branch_name` -> `BranchName`, `branch_location` -> `BranchLocation` |
| `tMSSqlOutput` | `tDBOutput_1` | Writes to `DWH.dbo.DimBranch` with `CREATE_IF_NOT_EXISTS` table action, `INSERT` data action, batch size 10,000, commit every 10,000 |

**Data Flow:** `tMSSqlInput` -> (row1) -> `tMap` -> (to_DimBranch) -> `tMSSqlOutput`

**Schema (3 columns):**

| Source Column | Target Column | Data Type | Key |
|---|---|---|---|
| `branch_id` | `BranchID` | INT | PK |
| `branch_name` | `BranchName` | VARCHAR(50) | - |
| `branch_location` | `BranchLocation` | VARCHAR(50) | - |

---

### 2.2 Load_DimAccount

| Attribute | Detail |
|---|---|
| **Job Name** | `Load_DimAccount` |
| **Version** | 0.1 |
| **Purpose** | Loads account master data from the `sample` operational database into the `DimAccount` dimension table in the DWH |
| **Schedule/Trigger** | Manual execution; run second in the load sequence |
| **Dependencies** | DWH database and `DimAccount` table must exist |
| **Complexity Rating** | **Simple** |
| **Source** | SQL Server `sample.dbo.account` (localhost:1433) |
| **Target** | SQL Server `DWH.dbo.DimAccount` (localhost:1433) |
| **Connection Type** | MSSQL (JDBC, Windows Integrated Authentication) |
| **Row Count (est.)** | Low-to-medium volume (account reference data) |

**Talend Components Used:**

| Component | Instance | Purpose |
|---|---|---|
| `tMSSqlInput` | `tDBInput_1` | Reads from `sample.dbo.account` via SQL query: `SELECT account_id, customer_id, account_type, balance, date_opened, status FROM dbo.account` |
| `tMap` | `tMap_1` | Simple 1:1 column mapping with PascalCase rename (e.g., `account_id` -> `AccountID`, `date_opened` -> `DateOpened`) |
| `tMSSqlOutput` | `tDBOutput_1` | Writes to `DWH.dbo.DimAccount` with `CREATE_IF_NOT_EXISTS` table action, `INSERT` data action, batch size 10,000 |

**Data Flow:** `tMSSqlInput` -> (row1) -> `tMap` -> (to_DimAccount) -> `tMSSqlOutput`

**Schema (6 columns):**

| Source Column | Target Column | Data Type | Key |
|---|---|---|---|
| `account_id` | `AccountID` | INT | PK |
| `customer_id` | `CustomerID` | INT | - |
| `account_type` | `AccountType` | VARCHAR(10) | - |
| `balance` | `Balance` | MONEY | - |
| `date_opened` | `DateOpened` | DATETIME2 | - |
| `status` | `Status` | VARCHAR(10) | - |

---

### 2.3 Load_DimCustomer

| Attribute | Detail |
|---|---|
| **Job Name** | `Load_DimCustomer` |
| **Version** | 0.1 |
| **Purpose** | Builds a denormalized customer dimension by joining `customer`, `city`, and `state` tables from the operational database, applying data cleansing (UPCASE), and loading into `DimCustomer` |
| **Schedule/Trigger** | Manual execution; run third in the load sequence |
| **Dependencies** | DWH database and `DimCustomer` table must exist; source tables `customer`, `city`, `state` must be populated |
| **Complexity Rating** | **Complex** |
| **Sources** | SQL Server `sample.dbo.customer`, `sample.dbo.city`, `sample.dbo.state` (localhost:1433) |
| **Target** | SQL Server `DWH.dbo.DimCustomer` (localhost:1433) |
| **Connection Type** | MSSQL (JDBC, Windows Integrated Authentication) |
| **Row Count (est.)** | Medium volume (customer master data) |

**Talend Components Used:**

| Component | Instance | Purpose |
|---|---|---|
| `tMSSqlInput` | `tDBInput_1` | Reads from `sample.dbo.customer`: `SELECT customer_id, customer_name, address, city_id, age, gender, email` |
| `tMSSqlInput` | `tDBInput_2` | Reads from `sample.dbo.city` (lookup): `SELECT city_id, city_name, state_id` |
| `tMSSqlInput` | `tDBInput_3` | Reads from `sample.dbo.state` (lookup): `SELECT state_id, state_name` |
| `tMap` | `tMap_1` | **Multi-table JOIN** with 3 input streams. Lookup joins: `row1.city_id = row2.city_id` (city lookup), `row2.state_id = row3.state_id` (state lookup). **Data cleansing**: `StringHandling.UPCASE()` applied to `customer_name`, `address`, and `gender` fields. Matching mode: `UNIQUE_MATCH`, Lookup mode: `LOAD_ONCE`. |
| `tMSSqlOutput` | `tDBOutput_1` | Writes to `DWH.dbo.DimCustomer` with `CREATE_IF_NOT_EXISTS` table action, `INSERT` data action, batch size 10,000 |

**Data Flow:**
```
tMSSqlInput (customer) --row1--> \
tMSSqlInput (city)     --row2-->  tMap (JOIN + UPCASE) --to_DimCustomer--> tMSSqlOutput
tMSSqlInput (state)    --row3--> /
```

**tMap Transformation Details:**

| Output Column | Expression | Notes |
|---|---|---|
| `CustomerID` | `row1.customer_id` | Direct mapping |
| `CustomerName` | `StringHandling.UPCASE(row1.customer_name)` | Uppercase cleansing |
| `Address` | `StringHandling.UPCASE(row1.address)` | Uppercase cleansing |
| `Age` | `row1.age` | Direct mapping |
| `Gender` | `StringHandling.UPCASE(row1.gender)` | Uppercase cleansing |
| `Email` | `row1.email` | Direct mapping (no UPCASE) |
| `CityName` | `row2.city_name` | From city lookup |
| `StateName` | `row3.state_name` | From state lookup |

**Schema (8 columns):**

| Target Column | Data Type | Source | Key |
|---|---|---|---|
| `CustomerID` | INT | `customer.customer_id` | PK |
| `CustomerName` | VARCHAR(50) | `customer.customer_name` (UPCASE) | - |
| `Address` | VARCHAR(MAX) | `customer.address` (UPCASE) | - |
| `CityName` | VARCHAR(50) | `city.city_name` via JOIN | - |
| `StateName` | VARCHAR(50) | `state.state_name` via JOIN | - |
| `Age` | VARCHAR(3) | `customer.age` | - |
| `Gender` | VARCHAR(10) | `customer.gender` (UPCASE) | - |
| `Email` | VARCHAR(50) | `customer.email` | - |

---

### 2.4 Load_FactTransaction

| Attribute | Detail |
|---|---|
| **Job Name** | `Load_FactTransaction` |
| **Version** | 0.1 |
| **Purpose** | Main integration pipeline that unifies transaction data from 3 heterogeneous sources (SQL Server, Excel, CSV), deduplicates records by `transaction_id`, maps columns to DWH format, and loads into `FactTransaction` |
| **Schedule/Trigger** | Manual execution; run last in the load sequence (depends on all dimension tables) |
| **Dependencies** | DWH database with `FactTransaction` table; `DimAccount` and `DimBranch` must be loaded (FK constraints); source files must be accessible |
| **Complexity Rating** | **Complex** |
| **Sources** | 1. SQL Server `sample.dbo.transaction_db` (localhost:1433) 2. Excel file `transaction_excel.xlsx` (Sheet1) 3. CSV file `transaction_csv.csv` (comma-delimited) |
| **Target** | SQL Server `DWH.dbo.FactTransaction` (localhost:1433) |
| **Connection Types** | MSSQL (JDBC), Excel (OOXML/.xlsx), Delimited File (CSV) |
| **Row Count (est.)** | High volume (transactional fact data) |

**Talend Components Used:**

| Component | Instance | Purpose |
|---|---|---|
| `tMSSqlInput` | `tDBInput_1` | Reads from `sample.dbo.transaction_db`: `SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id` |
| `tFileInputExcel` | `tFileInputExcel_1` | Reads from `transaction_excel.xlsx`, Sheet1, OOXML format (VERSION_2007=true), header row=1, encoding ISO-8859-15 |
| `tFileInputDelimited` | `tFileInputDelimited_1` | Reads from `transaction_csv.csv`, comma delimiter, newline row separator, header row=1, `REMOVE_EMPTY_ROW=true`, encoding ISO-8859-15 |
| `tUnite` | `tUnite_1` | Merges/unions the 3 input streams (SQL, Excel, CSV) into a single unified stream with consistent schema |
| `tUniqRow` | `tUniqRow_1` | Deduplicates the unified stream based on `transaction_id` as the unique key (case-insensitive). Outputs: `UNIQUE` (kept records) and `DUPLICATE` (rejected records) |
| `tMap` | `tMap_1` | Column mapping from source schema (lowercase) to DWH schema (PascalCase): `transaction_id` -> `TransactionID`, etc. |
| `tMSSqlOutput` | `tDBOutput_1` | Writes to `DWH.dbo.FactTransaction` with **`TRUNCATE`** table action (full reload), `INSERT` data action, batch size 10,000 |

**Data Flow:**
```
tMSSqlInput (SQL Server)       --row1--> \
tFileInputExcel (Excel .xlsx)  --row3-->  tUnite --row4--> tUniqRow --row5--> tMap --to_FactTransaction--> tMSSqlOutput
tFileInputDelimited (CSV)      --row2--> /
```

**Schema (6 columns):**

| Source Column | Target Column | Data Type | Key |
|---|---|---|---|
| `transaction_id` | `TransactionID` | INT | PK |
| `account_id` | `AccountID` | INT | FK -> DimAccount |
| `transaction_date` | `TransactionDate` | DATETIME2 | - |
| `amount` | `Amount` | MONEY | - |
| `transaction_type` | `TransactionType` | VARCHAR(50) | - |
| `branch_id` | `BranchID` | INT | FK -> DimBranch |

**Key Design Decisions:**
- **Full reload strategy**: The `TRUNCATE` table action means the fact table is fully reloaded each run (not incremental)
- **Deduplication on `transaction_id`**: Ensures no duplicate transactions are loaded when the same transaction appears in multiple sources
- **Date pattern**: `dd-MM-yyyy` or `dd-MM-yyyy HH:mm:ss` depending on source

---

### 2.5 Shared Infrastructure

**Database Connections (from Talend Metadata):**

| Connection Name | Database | Host | Port | Driver | Auth |
|---|---|---|---|---|---|
| `Sample_DB_Connection` | `sample` | localhost | 1433 | MSSQL_PROP (JDBC) | Windows Integrated (`integratedSecurity=true`) |
| `DWH_DB_Connection` | `DWH` | localhost | 1433 | MSSQL_PROP (JDBC) | Windows Integrated (`integratedSecurity=true`) |

**Connection Properties:** `noDatetimeStringSync=true; trustServerCertificate=true; integratedSecurity=true`

---

## 3. Migration Classification

### 3.1 Classification Summary

| Workload | Classification | Rationale |
|---|---|---|
| `Load_DimBranch` | **Native ADF Rewrite** | Simple SQL-to-SQL pipeline; straightforward to implement as ADF Copy Activity with column mapping |
| `Load_DimAccount` | **Native ADF Rewrite** | Simple SQL-to-SQL pipeline; straightforward to implement as ADF Copy Activity with column mapping |
| `Load_DimCustomer` | **Native ADF Rewrite** | Multi-table JOIN and UPCASE transformations require ADF Mapping Data Flow with Lookup and Derived Column transformations |
| `Load_FactTransaction` | **Native ADF Rewrite** | Multi-source union, deduplication, and mapping require ADF Mapping Data Flow with Union, Aggregate/Window, and Select transformations |
| `sp_DailyTransaction` | **Migrate as-is** | T-SQL fully compatible with Azure SQL Database; deploy stored procedure directly |
| `sp_BalancePerCustomer` | **Migrate as-is** | T-SQL (CTE, CASE, ISNULL) fully compatible with Azure SQL Database; deploy directly |
| Star Schema DDL | **Migrate as-is** | `CREATE TABLE`, `CREATE DATABASE`, FK constraints fully compatible with Azure SQL Database |

### 3.2 Why Not Lift-and-Shift (Azure-SSIS IR)?

The lift-and-shift approach using Azure-SSIS Integration Runtime is **not applicable** for this workload because:

1. **No existing SSIS packages**: The current ETL engine is Talend Open Studio, not SQL Server Integration Services. There are no `.dtsx` packages to migrate.
2. **No SSIS-dependent features**: The pipelines use Talend-native components (`tMap`, `tUnite`, `tUniqRow`) that have no SSIS equivalent packages.
3. **Cost consideration**: Azure-SSIS IR requires a dedicated cluster of Azure VMs running continuously, which is significantly more expensive than serverless ADF pipelines for this workload size.

### 3.3 Deprecation Candidates

No workloads are recommended for deprecation. All four ETL jobs and both stored procedures are actively used and serve current business reporting needs.

### 3.4 Detailed ADF Implementation Mapping

| Talend Job | ADF Pipeline Type | ADF Activities |
|---|---|---|
| `Load_DimBranch` | **Copy Activity** | Source: Azure SQL (query), Sink: Azure SQL (table), Column Mapping |
| `Load_DimAccount` | **Copy Activity** | Source: Azure SQL (query), Sink: Azure SQL (table), Column Mapping |
| `Load_DimCustomer` | **Mapping Data Flow** | 3x Source (SQL), 2x Lookup (city, state), Derived Column (UPPER), Select, Sink (SQL) |
| `Load_FactTransaction` | **Mapping Data Flow** | 3x Source (SQL, Excel, CSV), Union, Aggregate/Deduplicate, Select, Sink (SQL) |

---

## 4. Stored Procedure Assessment

### 4.1 sp_DailyTransaction

```sql
CREATE PROCEDURE sp_DailyTransaction
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        CAST(TransactionDate AS DATE) AS [Date],
        COUNT(TransactionID) AS TotalTransactions,
        SUM(Amount) AS TotalAmount
    FROM FactTransaction
    WHERE CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
    GROUP BY CAST(TransactionDate AS DATE)
    ORDER BY [Date];
END;
```

**Compatibility Assessment:**

| Aspect | Azure SQL Database | Azure SQL Managed Instance |
|---|---|---|
| `CREATE PROCEDURE` | Fully supported | Fully supported |
| `SET NOCOUNT ON` | Fully supported | Fully supported |
| `CAST(... AS DATE)` | Fully supported | Fully supported |
| `BETWEEN` clause | Fully supported | Fully supported |
| `COUNT`, `SUM` aggregations | Fully supported | Fully supported |
| `GROUP BY`, `ORDER BY` | Fully supported | Fully supported |
| Parameter types (`DATE`) | Fully supported | Fully supported |

**Verdict:** No modifications required. Fully compatible with both Azure SQL Database and Azure SQL Managed Instance.

**Recommended Target:** Azure SQL Database (lower cost, fully managed).

### 4.2 sp_BalancePerCustomer

```sql
CREATE PROCEDURE sp_BalancePerCustomer
    @customer_name VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    WITH TransactionSummary AS (
        SELECT AccountID,
            SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
                AS TotalTransactionAmount
        FROM FactTransaction
        GROUP BY AccountID
    )
    SELECT c.CustomerName, a.AccountType, a.Balance AS InitialBalance,
        a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
    FROM DimCustomer c
    JOIN DimAccount a ON c.CustomerID = a.CustomerID
    LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
    WHERE c.CustomerName LIKE '%' + @customer_name + '%'
        AND a.Status = 'active';
END;
```

**Compatibility Assessment:**

| Aspect | Azure SQL Database | Azure SQL Managed Instance |
|---|---|---|
| Common Table Expression (CTE) | Fully supported | Fully supported |
| `CASE WHEN ... THEN ... ELSE` | Fully supported | Fully supported |
| `ISNULL()` function | Fully supported | Fully supported |
| `LIKE` with wildcard concatenation | Fully supported | Fully supported |
| `JOIN`, `LEFT JOIN` | Fully supported | Fully supported |
| `MONEY` data type (`Balance`) | Fully supported | Fully supported |
| String concatenation (`'%' + @var + '%'`) | Fully supported | Fully supported |

**Verdict:** No modifications required. Fully compatible with both Azure SQL Database and Azure SQL Managed Instance.

**Recommended Target:** Azure SQL Database (lower cost, fully managed).

### 4.3 DDL Scripts Assessment

The `01_create_tables.sql` script uses:

| T-SQL Feature | Azure SQL DB Compatibility |
|---|---|
| `IF NOT EXISTS (SELECT * FROM sys.databases ...)` | **Requires modification** -- `CREATE DATABASE` cannot be run from within a user database in Azure SQL DB. Database must be created via Azure Portal/CLI/ARM. |
| `CREATE DATABASE DWH` | **Not supported in-session** -- Must be provisioned through Azure management plane |
| `USE DWH; GO` | **Not supported** -- Azure SQL Database does not support `USE` statement; each database has its own connection |
| `CREATE TABLE` with `PRIMARY KEY` | Fully supported |
| `FOREIGN KEY CONSTRAINTS` | Fully supported |
| `MONEY` data type | Fully supported |
| `PRINT` statements | Fully supported |

**Required Modifications for Azure SQL Database:**
1. Remove `CREATE DATABASE` and `USE` statements -- database will be pre-provisioned
2. Run the `CREATE TABLE` statements directly against the target Azure SQL Database connection
3. No changes needed to table definitions, data types, or constraints

---

## 5. Data Source Assessment

### 5.1 SQL Server Database (.bak file)

| Attribute | Detail |
|---|---|
| **File** | `data_sources/sample.bak` |
| **Purpose** | SQL Server backup of the `sample` operational database containing source tables: `branch`, `account`, `customer`, `city`, `state`, `transaction_db` |
| **ADF Connector** | **Azure SQL Database** connector (if migrated to Azure SQL) or **SQL Server** connector (if using Self-Hosted Integration Runtime for on-premises) |
| **Migration Path for Data** | Restore `.bak` to Azure SQL Managed Instance (native backup restore) or use Azure Database Migration Service (DMS) for Azure SQL Database |
| **Network Considerations** | If source remains on-premises: requires Self-Hosted Integration Runtime (SHIR) installed on a machine with network access to the SQL Server instance. If migrated to Azure: use Azure-native connectivity. |
| **Authentication** | Current: Windows Integrated Authentication (`integratedSecurity=true`). Azure migration: switch to SQL Authentication or Azure AD Authentication. SHIR supports Windows Auth for on-prem. |
| **Configuration Notes** | Connection properties `trustServerCertificate=true` should be reviewed for production -- use proper TLS certificates in Azure. |

### 5.2 Excel File (.xlsx)

| Attribute | Detail |
|---|---|
| **File** | `data_sources/transaction_excel.xlsx` |
| **Purpose** | Contains transaction data (Sheet1) as one of three transaction sources |
| **Current Access** | Local filesystem path: `C:/Data Rakamin/transaction_excel.xlsx` |
| **ADF Connector** | **Excel** dataset type with **Azure Blob Storage** or **Azure Data Lake Storage Gen2** linked service |
| **Migration Path** | Upload `.xlsx` file to Azure Blob Storage or ADLS Gen2 container. Configure ADF Excel dataset pointing to the blob path. |
| **Configuration** | Sheet: `Sheet1`, Header row: 1, OOXML format (2007+) |
| **Network Considerations** | File must be accessible from ADF. Upload to Azure Storage eliminates network dependency. Alternatively, use SHIR if files remain on-premises. |
| **Authentication** | Azure Storage: Account Key, SAS Token, Managed Identity, or Service Principal |
| **Schema** | 6 columns: `transaction_id` (INT), `account_id` (INT), `transaction_date` (DATE), `amount` (INT), `transaction_type` (STRING), `branch_id` (INT) |

### 5.3 CSV File (.csv)

| Attribute | Detail |
|---|---|
| **File** | `data_sources/transaction_csv.csv` |
| **Purpose** | Contains transaction data as one of three transaction sources |
| **Current Access** | Local filesystem path: `C:/Data Rakamin/transaction_csv.csv` |
| **ADF Connector** | **DelimitedText** dataset type with **Azure Blob Storage** or **Azure Data Lake Storage Gen2** linked service |
| **Migration Path** | Upload `.csv` file to Azure Blob Storage or ADLS Gen2 container. Configure ADF DelimitedText dataset. |
| **Configuration** | Delimiter: comma (`,`), Row separator: `\n`, Header: first row, Encoding: ISO-8859-15 (consider converting to UTF-8 for Azure) |
| **Network Considerations** | Same as Excel -- upload to Azure Storage or use SHIR for on-premises access |
| **Authentication** | Azure Storage: Account Key, SAS Token, Managed Identity, or Service Principal |
| **Schema** | 6 columns: `transaction_id` (INT), `account_id` (INT), `transaction_date` (DATE), `amount` (INT), `transaction_type` (STRING), `branch_id` (INT) |

### 5.4 Data Source Migration Summary

| Source Type | Current Location | Azure Target | ADF Linked Service | IR Required |
|---|---|---|---|---|
| SQL Server (.bak) | On-premises localhost:1433 | Azure SQL Database | Azure SQL Database | Azure IR (after migration) or SHIR (hybrid) |
| Excel (.xlsx) | Local filesystem | Azure Blob Storage / ADLS Gen2 | Azure Blob Storage | Azure IR |
| CSV (.csv) | Local filesystem | Azure Blob Storage / ADLS Gen2 | Azure Blob Storage | Azure IR |

---

## 6. Risk & Complexity Matrix

### 6.1 ETL Workloads

| Workload | Technical Complexity (1-5) | Business Criticality (1-5) | Migration Risk | Estimated Effort (hrs) | Notes |
|---|---|---|---|---|---|
| `Load_DimBranch` | 1 | 2 | **Low** | 4-6 | Simple copy activity; minimal transformation |
| `Load_DimAccount` | 1 | 3 | **Low** | 4-6 | Simple copy activity; date format validation recommended |
| `Load_DimCustomer` | 3 | 4 | **Medium** | 12-16 | Multi-table lookup joins and UPCASE transformation in Mapping Data Flow |
| `Load_FactTransaction` | 4 | 5 | **Medium-High** | 16-24 | 3-source union, deduplication, multi-format ingestion (SQL/Excel/CSV), full-reload strategy |
| `sp_DailyTransaction` | 1 | 3 | **Low** | 2-3 | Direct deployment to Azure SQL; no code changes |
| `sp_BalancePerCustomer` | 2 | 4 | **Low** | 2-3 | Direct deployment to Azure SQL; CTE logic unchanged |
| DDL (Star Schema) | 1 | 5 | **Low** | 2-4 | Remove `CREATE DATABASE`/`USE`; deploy tables directly |
| Data Migration (source DB) | 2 | 5 | **Medium** | 8-12 | Restore .bak to Azure SQL or use DMS |
| File Migration (Excel/CSV) | 1 | 3 | **Low** | 2-4 | Upload to Azure Blob/ADLS Gen2 |

### 6.2 Risk Summary

| Risk Category | Description | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| **Authentication change** | Moving from Windows Integrated Auth to SQL/Azure AD Auth | High | Medium | Pre-configure Azure AD/SQL Auth; test connectivity before cutover |
| **Data format differences** | Date parsing differences between Talend and ADF for Excel/CSV sources | Medium | Medium | Validate date formats in test runs; configure explicit format patterns in ADF |
| **Encoding mismatch** | Source files use ISO-8859-15; Azure defaults to UTF-8 | Low | Low | Convert files to UTF-8 during upload or configure encoding in ADF dataset |
| **Network latency** | If hybrid setup with SHIR, network latency impacts ETL performance | Medium | Low | Migrate all sources to Azure to eliminate hybrid connectivity |
| **Deduplication logic** | `tUniqRow` behavior (first-match wins) must be replicated exactly in ADF | Medium | High | Implement using ADF Aggregate transformation with `first()` function; validate row counts |
| **Full reload impact** | `FactTransaction` uses TRUNCATE+INSERT; during migration, parallel systems may conflict | Low | High | Implement proper cutover plan with maintenance window |
| **MONEY data type precision** | `MONEY` type in SQL Server maps differently than `INT` used in Talend | Low | Medium | Verify data type mapping; ADF handles this natively with Azure SQL connector |

### 6.3 Total Estimated Effort

| Phase | Effort (hrs) |
|---|---|
| Infrastructure Setup (Azure SQL DB, Storage, ADF) | 8-12 |
| Simple Pipelines (DimBranch, DimAccount) | 8-12 |
| Complex Pipelines (DimCustomer, FactTransaction) | 28-40 |
| Stored Procedure Deployment | 4-6 |
| Data Migration (DB + Files) | 10-16 |
| Testing & Validation | 16-24 |
| **Total** | **74-110** |

---

## 7. Recommended Migration Sequence

### Phase 1: Foundation (Week 1)

| Priority | Task | Rationale |
|---|---|---|
| 1.1 | Provision Azure SQL Database | All workloads depend on the target database |
| 1.2 | Provision Azure Data Factory instance | Required for all pipeline migrations |
| 1.3 | Provision Azure Blob Storage / ADLS Gen2 | Required for file-based sources (Excel, CSV) |
| 1.4 | Deploy DDL scripts to Azure SQL DB | Creates the DWH Star Schema (remove `CREATE DATABASE`/`USE` statements) |
| 1.5 | Deploy stored procedures to Azure SQL DB | `sp_DailyTransaction` and `sp_BalancePerCustomer` -- zero code changes needed |

### Phase 2: Data Migration (Week 1-2)

| Priority | Task | Rationale |
|---|---|---|
| 2.1 | Migrate `sample` database to Azure SQL DB | Use Azure Database Migration Service (DMS) or bacpac export/import |
| 2.2 | Upload `transaction_excel.xlsx` to Azure Blob Storage | Required for `Load_FactTransaction` pipeline |
| 2.3 | Upload `transaction_csv.csv` to Azure Blob Storage | Required for `Load_FactTransaction` pipeline |
| 2.4 | Configure ADF Linked Services | Azure SQL (source DB), Azure SQL (DWH), Azure Blob Storage |

### Phase 3: Simple Pipelines (Week 2)

| Priority | Task | Rationale |
|---|---|---|
| 3.1 | Implement `Load_DimBranch` as ADF Copy Activity | Simplest pipeline; validates end-to-end connectivity |
| 3.2 | Implement `Load_DimAccount` as ADF Copy Activity | Second simplest; validates date type handling |
| 3.3 | Validate dimension data against source | Row count comparison and data sampling |

### Phase 4: Complex Pipelines (Week 2-3)

| Priority | Task | Rationale |
|---|---|---|
| 4.1 | Implement `Load_DimCustomer` as ADF Mapping Data Flow | Multi-table join and UPCASE transformation |
| 4.2 | Validate `DimCustomer` output against Talend baseline | Verify JOIN results and UPCASE transformations |
| 4.3 | Implement `Load_FactTransaction` as ADF Mapping Data Flow | Most complex -- 3-source union + dedup |
| 4.4 | Validate `FactTransaction` output against Talend baseline | Critical: verify deduplication and row counts |

### Phase 5: Orchestration & Testing (Week 3-4)

| Priority | Task | Rationale |
|---|---|---|
| 5.1 | Create master ADF pipeline with sequential execution | Enforce execution order: DimBranch -> DimAccount -> DimCustomer -> FactTransaction |
| 5.2 | Configure ADF triggers (schedule or event-based) | Replace manual Talend execution with automated scheduling |
| 5.3 | End-to-end integration testing | Full pipeline run with production-like data |
| 5.4 | Validate stored procedure results on Azure SQL | Run `sp_DailyTransaction` and `sp_BalancePerCustomer` against migrated data |
| 5.5 | Performance benchmarking | Compare ADF execution times with Talend baseline |
| 5.6 | Configure monitoring and alerting | ADF Monitor, Azure Monitor alerts for pipeline failures |

### Phase 6: Cutover (Week 4)

| Priority | Task | Rationale |
|---|---|---|
| 6.1 | Final data sync from on-premises to Azure | Ensure data is current |
| 6.2 | Production cutover to ADF pipelines | Switch from Talend to ADF |
| 6.3 | Decommission Talend jobs | Archive Talend project; retain for rollback period |
| 6.4 | Post-migration validation | 1-week parallel run comparison |

---

## 8. Microsoft Data Migration Assistant Results (Simulated)

> **Note:** The following represents simulated DMA assessment results based on analysis of the actual T-SQL scripts and database schema in this repository. In a real migration, the Data Migration Assistant tool would be run against the live SQL Server instance.

### 8.1 DMA Assessment Configuration

| Parameter | Value |
|---|---|
| Assessment Type | Database Migration |
| Source | SQL Server (on-premises) |
| Target | Azure SQL Database v12 |
| Databases Assessed | `sample`, `DWH` |

### 8.2 Compatibility Issues Found

#### Database: `DWH`

| # | Severity | Category | Issue | Affected Object | Recommendation |
|---|---|---|---|---|---|
| 1 | **Warning** | Cross-database references | `USE DWH` statement in DDL script | `01_create_tables.sql` | Remove `USE` statement; connect directly to target database |
| 2 | **Warning** | Unsupported features | `CREATE DATABASE` in user context | `01_create_tables.sql` | Provision database via Azure Portal, CLI, or ARM template |
| 3 | **Info** | Authentication | Windows Integrated Authentication configured | All DB connections | Switch to SQL Authentication or Azure AD Authentication |
| 4 | **Info** | Performance | No indexes defined beyond primary keys | `FactTransaction` | Consider adding indexes on `AccountID`, `BranchID`, `TransactionDate` for query performance |
| 5 | **Info** | Data types | `MONEY` type used for `Balance` and `Amount` | `DimAccount`, `FactTransaction` | `MONEY` is supported in Azure SQL DB; however, consider `DECIMAL(19,4)` for cross-platform compatibility |

#### Database: `sample`

| # | Severity | Category | Issue | Affected Object | Recommendation |
|---|---|---|---|---|---|
| 1 | **Info** | Backup/Restore | `.bak` file cannot be directly restored to Azure SQL Database | `sample.bak` | Use Azure SQL Managed Instance for native restore, or use DMS/bacpac for Azure SQL Database |
| 2 | **Info** | Authentication | Windows Integrated Authentication | All connections | Configure SQL Auth or Azure AD for Azure target |

### 8.3 Feature Parity Assessment

| Feature | Used in Workload | Azure SQL DB Support | Notes |
|---|---|---|---|
| Stored Procedures | `sp_DailyTransaction`, `sp_BalancePerCustomer` | Fully supported | No changes needed |
| Common Table Expressions (CTE) | `sp_BalancePerCustomer` | Fully supported | - |
| `CASE WHEN` expressions | `sp_BalancePerCustomer` | Fully supported | - |
| `ISNULL()` function | `sp_BalancePerCustomer` | Fully supported | - |
| `MONEY` data type | `DimAccount`, `FactTransaction` | Fully supported | - |
| Primary Key constraints | All tables | Fully supported | - |
| Foreign Key constraints | `FactTransaction` | Fully supported | - |
| `CAST(... AS DATE)` | `sp_DailyTransaction` | Fully supported | - |
| `LIKE` with wildcards | `sp_BalancePerCustomer` | Fully supported | - |
| `CREATE DATABASE` | DDL script | **Not supported in-session** | Use Azure management plane |
| `USE [database]` | DDL script | **Not supported** | Connect directly to target DB |
| Cross-database queries | Not used | N/A | No impact |
| CLR functions | Not used | N/A | No impact |
| Linked servers | Not used | N/A | No impact |
| SQL Agent jobs | Not used | N/A | No impact (ADF handles scheduling) |

### 8.4 DMA Migration Readiness Score

| Database | Readiness | Blockers | Warnings | Info |
|---|---|---|---|---|
| `DWH` | **Ready with minor changes** | 0 | 2 | 3 |
| `sample` | **Ready** | 0 | 0 | 2 |
| **Overall** | **Ready for migration** | **0 blockers** | **2 warnings** | **5 info** |

### 8.5 DMA Recommendations

1. **Pre-provision Azure SQL Database** via Azure Portal or ARM template instead of using `CREATE DATABASE` in scripts
2. **Update connection strings** to use SQL Authentication or Azure AD instead of Windows Integrated Authentication
3. **Add performance indexes** on `FactTransaction` for columns used in stored procedure queries (`AccountID`, `TransactionDate`)
4. **Consider service tier**: Based on the workload profile (batch ETL with periodic stored procedure queries), **General Purpose** tier with 2-4 vCores should be sufficient
5. **Use Azure Database Migration Service** for the initial data migration from `.bak` file to Azure SQL Database

---

## 9. Appendix: Talend-to-ADF Component Mapping

This reference table maps each Talend component used in the current solution to its Azure Data Factory equivalent.

| Talend Component | Purpose in Current Solution | ADF Equivalent | ADF Activity Type | Notes |
|---|---|---|---|---|
| `tMSSqlInput` | Read data from SQL Server tables | **Source** transformation | Copy Activity / Data Flow Source | Use Azure SQL Database connector |
| `tMSSqlOutput` | Write data to SQL Server tables | **Sink** transformation | Copy Activity / Data Flow Sink | Configure write behavior (insert, truncate+insert) |
| `tMap` | Column mapping, transformations, lookups | **Derived Column** + **Select** + **Lookup** | Mapping Data Flow | For simple mapping: Copy Activity column mapping. For complex: Data Flow |
| `tUnite` | Union/merge multiple data streams | **Union** transformation | Mapping Data Flow | Combines streams with matching schemas |
| `tUniqRow` | Deduplicate rows by key column | **Aggregate** transformation | Mapping Data Flow | Use `first()` function grouped by key column |
| `tFileInputExcel` | Read Excel (.xlsx) files | **Source** with Excel dataset | Copy Activity / Data Flow Source | Use Azure Blob Storage + Excel dataset format |
| `tFileInputDelimited` | Read CSV/delimited files | **Source** with DelimitedText dataset | Copy Activity / Data Flow Source | Use Azure Blob Storage + DelimitedText dataset |
| `StringHandling.UPCASE()` | Convert strings to uppercase | **Derived Column** with `upper()` | Mapping Data Flow | ADF expression: `upper(columnName)` |
| Context Variables | Parameterize table names | **Pipeline Parameters** | Pipeline | Define parameters at pipeline level, reference in activities |
| Repository Connections | Reusable connection metadata | **Linked Services** | ADF Resource | Create reusable linked services for each data store |

---

*End of Assessment Document*
