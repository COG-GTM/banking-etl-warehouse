# SSIS Package Inventory & ADF Migration Assessment

## Executive Summary

This document provides a comprehensive inventory of all ETL components in the `banking-etl-warehouse` repository and assesses their readiness for migration to **Azure Data Factory (ADF)**.

### Key Findings

- **No SSIS packages (`.dtsx`, `.dtsConfig`, `.params`, `.ispac`) were found** in this repository.
- The ETL layer is built entirely with **Talend Open Studio for Data Integration (v8.0.1)**, not SSIS.
- The repository contains **4 Talend ETL jobs**, **2 SQL scripts** (DDL + Stored Procedures), **3 raw data sources**, and **2 database connection definitions**.
- All components are candidates for migration to ADF pipelines, Data Flows, and Azure SQL Database.
- Overall migration complexity is **Low-to-Medium** — the jobs are straightforward extract-transform-load patterns with no advanced orchestration, error handling frameworks, or scheduling logic.

### Scope

| Category | Count | Details |
|---|---|---|
| SSIS Packages | **0** | None found in repo |
| Talend ETL Jobs | **4** | Dimension and fact loaders |
| SQL DDL Scripts | **1** | Star schema creation (4 tables) |
| Stored Procedures | **2** | Analytical reporting queries |
| Data Sources | **3** | SQL Server BAK, CSV, Excel (XLSX) |
| DB Connections | **2** | Source (`sample`) and Target (`DWH`) |

---

## 1. Data Source Inventory

### 1.1 SQL Server Backup — `sample.bak`

| Attribute | Value |
|---|---|
| **File Path** | `data_sources/sample.bak` |
| **Size** | 7.8 MB |
| **Format** | SQL Server native backup (NT Backup archive) |
| **DB Engine** | Microsoft SQL Server 16.00.1000 |
| **Database Name** | `sample` |
| **Schema** | `dbo` |

**Source Tables (5 tables):**

| Table | Primary Key | Columns | Description |
|---|---|---|---|
| `account` | `account_id` (INT) | account_id, customer_id, account_type, balance, date_opened, status | Bank account master data |
| `branch` | `branch_id` (INT) | branch_id, branch_name, branch_location | Branch reference data |
| `city` | `city_id` (INT) | city_id, city_name, state_id | City lookup table |
| `state` | `state_id` (INT) | state_id, state_name | State lookup table |
| `customer` | `customer_id` (INT) | customer_id, customer_name, address, city_id, age, gender, email | Customer profiles |
| `transaction_db` | `transaction_id` (INT) | transaction_id, account_id, transaction_date, amount, transaction_type, branch_id | SQL-sourced transactions |

### 1.2 CSV File — `transaction_csv.csv`

| Attribute | Value |
|---|---|
| **File Path** | `data_sources/transaction_csv.csv` |
| **Size** | 603 bytes |
| **Records** | 11 data rows |
| **Columns** | transaction_id, account_id, transaction_date, amount, transaction_type, branch_id |
| **Date Format** | `dd-MM-yyyy HH:mm:ss` |
| **Delimiter** | Comma |
| **Purpose** | Supplemental transaction data (IDs 14–25) |

### 1.3 Excel File — `transaction_excel.xlsx`

| Attribute | Value |
|---|---|
| **File Path** | `data_sources/transaction_excel.xlsx` |
| **Size** | 9.1 KB |
| **Format** | Microsoft Excel 2007+ (OOXML) |
| **Columns** | transaction_id, account_id, transaction_date, amount, transaction_type, branch_id |
| **Purpose** | Supplemental transaction data from Excel-based reporting |

---

## 2. Database Connection Inventory

### 2.1 Source Connection — `Sample_DB_Connection`

| Attribute | Value |
|---|---|
| **File Path** | `talend_jobs/*.zip` → `IDX_INTERNSHIP/metadata/connections/Sample_DB_Connection_0.1.item` |
| **Database Type** | Microsoft SQL Server |
| **Driver** | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| **JDBC URL** | `jdbc:sqlserver://localhost:1433;DatabaseName=sample` |
| **Authentication** | Windows Integrated Security (`integratedSecurity=true`) |
| **Schema** | `dbo` |
| **Additional Params** | `noDatetimeStringSync=true;trustServerCertificate=true` |

### 2.2 Target Connection — `DWH_DB_Connection`

| Attribute | Value |
|---|---|
| **File Path** | `talend_jobs/*.zip` → `IDX_INTERNSHIP/metadata/connections/DWH_DB_Connection_0.1.item` |
| **Database Type** | Microsoft SQL Server |
| **Driver** | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| **JDBC URL** | `jdbc:sqlserver://localhost:1433;DatabaseName=DWH` |
| **Authentication** | Windows Integrated Security (`integratedSecurity=true`) |
| **Schemas** | dbo, db_owner, db_datareader, db_datawriter, etc. |

---

## 3. Talend ETL Job Inventory

### 3.1 Load_DimBranch

| Attribute | Value |
|---|---|
| **File Path** | `talend_jobs/Load_DimBranch.zip` |
| **Talend Project** | `IDX_INTERNSHIP` |
| **Job Type** | Standard |
| **Version** | 0.1 |

**Purpose:** Loads branch reference data from the `sample.branch` table into `DWH.DimBranch`.

**Data Flow:**
```
tMSSqlInput (branch) → tMap_1 → tMSSqlOutput (DimBranch)
```

**Components:**
| Component | Unique Name | Details |
|---|---|---|
| `tMSSqlInput` | tDBInput_1 | Reads from `dbo.branch` — `SELECT branch_id, branch_name, branch_location FROM dbo.branch` |
| `tMap` | tMap_1 | Direct 1:1 field mapping (no transformations) |
| `tMSSqlOutput` | tDBOutput_1 | Writes to `DimBranch` (table action: `CREATE_IF_NOT_EXISTS`) |

**Transformations:** None — direct passthrough mapping.

**Source:** `sample.dbo.branch` (SQL Server)
**Target:** `DWH.DimBranch`

**Schedule:** Not documented (manual execution).
**Dependencies:** None — independent dimension load.

---

### 3.2 Load_DimAccount

| Attribute | Value |
|---|---|
| **File Path** | `talend_jobs/Load_DimAccount.zip` |
| **Talend Project** | `IDX_INTERNSHIP` |
| **Job Type** | Standard |
| **Version** | 0.1 |

**Purpose:** Loads account master data from `sample.account` into `DWH.DimAccount`.

**Data Flow:**
```
tMSSqlInput (account) → tMap_1 → tMSSqlOutput (DimAccount)
```

**Components:**
| Component | Unique Name | Details |
|---|---|---|
| `tMSSqlInput` | tDBInput_1 | Reads from `dbo.account` — `SELECT account_id, customer_id, account_type, balance, date_opened, status FROM dbo.account` |
| `tMap` | tMap_1 | Direct 1:1 field mapping (no transformations) |
| `tMSSqlOutput` | tDBOutput_1 | Writes to `DimAccount` (table action: `CREATE_IF_NOT_EXISTS`) |

**Transformations:** None — direct passthrough mapping.

**Source:** `sample.dbo.account` (SQL Server)
**Target:** `DWH.DimAccount`

**Schedule:** Not documented (manual execution).
**Dependencies:** None — independent dimension load.

---

### 3.3 Load_DimCustomer

| Attribute | Value |
|---|---|
| **File Path** | `talend_jobs/Load_DimCustomer.zip` |
| **Talend Project** | `IDX_INTERNSHIP` |
| **Job Type** | Standard |
| **Version** | 0.1 |

**Purpose:** Loads customer dimension data by joining `customer`, `city`, and `state` tables from the source database, then writing the denormalized result to `DWH.DimCustomer`.

**Data Flow:**
```
tMSSqlInput (customer) ─┐
tMSSqlInput (city) ─────┤→ tMap_1 → tMSSqlOutput (DimCustomer)
tMSSqlInput (state) ────┘
```

**Components:**
| Component | Unique Name | Details |
|---|---|---|
| `tMSSqlInput` | tDBInput_1 | Reads from `dbo.customer` — `SELECT customer_id, customer_name, address, city_id, age, gender, email FROM dbo.customer` |
| `tMSSqlInput` | tDBInput_2 | Reads from `dbo.city` — `SELECT city_id, city_name, state_id FROM dbo.city` |
| `tMSSqlInput` | tDBInput_3 | Reads from `dbo.state` — `SELECT state_id, state_name FROM dbo.state` |
| `tMap` | tMap_1 | Multi-table JOIN: customer ↔ city (on city_id) ↔ state (on state_id). Applies `UPPER()` transformation on text fields. |
| `tMSSqlOutput` | tDBOutput_1 | Writes to `DimCustomer` (table action: `CREATE_IF_NOT_EXISTS`) |

**Transformations:**
- **JOIN:** customer.city_id → city.city_id; city.state_id → state.state_id
- **Data Cleansing:** Text fields converted to uppercase via `StringHandling.UPCASE()`
- **Denormalization:** Flattens customer + city + state into a single dimension row

**Source:** `sample.dbo.customer`, `sample.dbo.city`, `sample.dbo.state` (SQL Server)
**Target:** `DWH.DimCustomer`

**Schedule:** Not documented (manual execution).
**Dependencies:**
- Upstream: Requires source tables `customer`, `city`, `state` populated in `sample` DB.

---

### 3.4 Load_FactTransaction

| Attribute | Value |
|---|---|
| **File Path** | `talend_jobs/Load_FactTransaction.zip` |
| **Talend Project** | `IDX_INTERNSHIP` |
| **Job Type** | Standard |
| **Version** | 0.1 |

**Purpose:** The main integration pipeline that unifies transaction data from all three source systems (SQL Server, Excel, CSV), deduplicates records, and loads the result into `DWH.FactTransaction`.

**Data Flow:**
```
tMSSqlInput (transaction_db) ────┐
tFileInputExcel (Excel) ────────┤→ tUnite_1 → tUniqRow_1 → tMap_1 → tMSSqlOutput (FactTransaction)
tFileInputDelimited (CSV) ──────┘
```

**Components:**
| Component | Unique Name | Details |
|---|---|---|
| `tMSSqlInput` | tDBInput_1 | Reads from `dbo.transaction_db` — `SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id FROM dbo.transaction_db` |
| `tFileInputExcel` | tFileInputExcel_1 | Reads transaction data from `transaction_excel.xlsx` |
| `tFileInputDelimited` | tFileInputDelimited_1 | Reads transaction data from `transaction_csv.csv` (comma-delimited) |
| `tUnite` | tUnite_1 | Merges all three input streams into a single unified stream |
| `tUniqRow` | tUniqRow_1 | Deduplicates records based on `transaction_id` (ensures atomicity) |
| `tMap` | tMap_1 | Field mapping and formatting for target schema |
| `tMSSqlOutput` | tDBOutput_1 | Writes to `FactTransaction` (table action: `TRUNCATE` — full reload pattern) |

**Transformations:**
- **Stream Unification:** Merges SQL, Excel, and CSV sources into a single stream via `tUnite`
- **Deduplication:** Removes duplicate `transaction_id` values via `tUniqRow`
- **Field Mapping:** Maps source fields to target DWH schema via `tMap`
- **Full Reload:** Target table is truncated before each load

**Sources:**
- `sample.dbo.transaction_db` (SQL Server)
- `data_sources/transaction_excel.xlsx` (Excel)
- `data_sources/transaction_csv.csv` (CSV)

**Target:** `DWH.FactTransaction`

**Schedule:** Not documented (manual execution).
**Dependencies:**
- Upstream: All three source files must be available.
- Downstream: Must run AFTER dimension loads (`Load_DimBranch`, `Load_DimAccount`) due to FK constraints.

---

## 4. SQL Script Inventory

### 4.1 DDL Script — `01_create_tables.sql`

| Attribute | Value |
|---|---|
| **File Path** | `sql_scripts/01_create_tables.sql` |
| **Purpose** | Creates the DWH database and Star Schema tables |
| **Target Database** | `DWH` |

**Tables Created:**

| Table | Type | Columns | Key Constraints |
|---|---|---|---|
| `DimAccount` | Dimension | AccountID (PK), CustomerID, AccountType, Balance, DateOpened, Status | Primary Key |
| `DimBranch` | Dimension | BranchID (PK), BranchName, BranchLocation | Primary Key |
| `DimCustomer` | Dimension | CustomerID (PK), CustomerName, Address, CityName, StateName, Age, Gender, Email | Primary Key |
| `FactTransaction` | Fact | TransactionID (PK), AccountID (FK), TransactionDate, Amount, TransactionType, BranchID (FK) | PK + 2 Foreign Keys |

**Foreign Key Relationships:**
- `FactTransaction.AccountID` → `DimAccount.AccountID`
- `FactTransaction.BranchID` → `DimBranch.BranchID`

**Notes:**
- Uses `IF NOT EXISTS` guard for database creation
- Uses `MONEY` data type for monetary fields
- No indexes beyond primary keys defined

---

### 4.2 Stored Procedures — `02_create_procedures.sql`

| Attribute | Value |
|---|---|
| **File Path** | `sql_scripts/02_create_procedures.sql` |
| **Purpose** | Creates analytical stored procedures against the DWH |
| **Target Database** | `DWH` |

#### 4.2.1 `sp_DailyTransaction`

| Attribute | Value |
|---|---|
| **Parameters** | `@start_date DATE`, `@end_date DATE` |
| **Purpose** | Generates daily summary of transaction volume and total amount within a date range |
| **Logic** | `GROUP BY CAST(TransactionDate AS DATE)`, aggregates `COUNT(TransactionID)` and `SUM(Amount)` |
| **Output** | Date, TotalTransactions, TotalAmount |

#### 4.2.2 `sp_BalancePerCustomer`

| Attribute | Value |
|---|---|
| **Parameters** | `@customer_name VARCHAR(100)` |
| **Purpose** | Calculates current balance for each active account of a given customer |
| **Logic** | Uses CTE (`TransactionSummary`) to compute net balance change per account. Applies `CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END` logic. Joins DimCustomer → DimAccount → FactTransaction. Filters on `Status = 'active'` and `CustomerName LIKE '%' + @customer_name + '%'`. |
| **Output** | CustomerName, AccountType, InitialBalance, CurrentBalance |

---

## 5. Documentation

### 5.1 PDF Documentation

| Attribute | Value |
|---|---|
| **File Path** | `DEProjectIDXPartners-DataWarehouseETLSolution (1).pdf` |
| **Purpose** | Architectural design document for Staging and DWH layers |
| **Contents** | System architecture, design decisions, layer descriptions |

---

## 6. ADF Migration Classification

### Classification Legend

| Classification | Definition |
|---|---|
| **Lift-and-Shift** | Can run as-is on Azure-SSIS Integration Runtime |
| **Native ADF Rewrite** | Should be rebuilt as ADF pipelines / Data Flows |
| **Deprecate** | No longer needed or superseded by native ADF capabilities |

### Classification Results

| # | Component | Type | Classification | Rationale |
|---|---|---|---|---|
| 1 | `Load_DimBranch` | Talend Job | **Native ADF Rewrite** | Simple SQL→SQL copy; ideal for ADF Copy Activity with no transformation |
| 2 | `Load_DimAccount` | Talend Job | **Native ADF Rewrite** | Simple SQL→SQL copy; ideal for ADF Copy Activity with no transformation |
| 3 | `Load_DimCustomer` | Talend Job | **Native ADF Rewrite** | Multi-table JOIN + UPPER() transform; use ADF Mapping Data Flow or pre-stage with SQL view |
| 4 | `Load_FactTransaction` | Talend Job | **Native ADF Rewrite** | Multi-source union + dedup; use ADF Data Flow with Union + Aggregate/Distinct transformations |
| 5 | `01_create_tables.sql` | DDL Script | **Native ADF Rewrite** | Migrate DDL to Azure SQL Database; deploy via SSDT/dacpac or ADF Stored Procedure activity |
| 6 | `02_create_procedures.sql` | Stored Procedures | **Native ADF Rewrite** | Port to Azure SQL Database; callable via ADF Stored Procedure Activity |
| 7 | `Sample_DB_Connection` | DB Connection | **Deprecate** | Replace with ADF Linked Service (Azure SQL Database or SQL Managed Instance) |
| 8 | `DWH_DB_Connection` | DB Connection | **Deprecate** | Replace with ADF Linked Service (Azure SQL Database) |
| 9 | `sample.bak` | Data Source | **Deprecate** | Restore once to Azure SQL DB; not needed as ongoing artifact |
| 10 | `transaction_csv.csv` | Data Source | **Native ADF Rewrite** | Migrate to Azure Blob Storage / Data Lake; ingest via ADF DelimitedText dataset |
| 11 | `transaction_excel.xlsx` | Data Source | **Native ADF Rewrite** | Migrate to Azure Blob Storage / Data Lake; ingest via ADF Excel dataset |

**Summary:**
- **Lift-and-Shift candidates: 0** — No SSIS packages exist; Talend jobs cannot run on Azure-SSIS IR.
- **Native ADF Rewrite candidates: 8** — All active ETL logic and data sources.
- **Deprecate: 3** — Connection definitions and the BAK file.

---

## 7. Risk & Complexity Assessment

### Risk Matrix

| # | Component | Complexity | Risk | Effort | Key Concerns |
|---|---|---|---|---|---|
| 1 | `Load_DimBranch` | **Low** | **Low** | **S** | Trivial 1:1 copy. No transformation logic. |
| 2 | `Load_DimAccount` | **Low** | **Low** | **S** | Trivial 1:1 copy. 6 columns, direct mapping. |
| 3 | `Load_DimCustomer` | **Medium** | **Low** | **M** | Multi-table JOIN (3 tables) and UPPER() transformation. Requires ADF Data Flow or SQL view. |
| 4 | `Load_FactTransaction` | **Medium** | **Medium** | **M** | Multi-source ingestion (SQL + CSV + Excel), union, and deduplication. CSV/Excel files need Azure Blob staging. Date format parsing (`dd-MM-yyyy HH:mm:ss`) may need explicit handling. |
| 5 | `01_create_tables.sql` | **Low** | **Low** | **S** | Standard T-SQL DDL. Compatible with Azure SQL Database with minimal changes (remove `USE`/`GO` if using dacpac). |
| 6 | `sp_DailyTransaction` | **Low** | **Low** | **S** | Simple aggregation query. Fully compatible with Azure SQL Database. |
| 7 | `sp_BalancePerCustomer` | **Low** | **Low** | **S** | CTE-based balance calculation. Fully compatible with Azure SQL Database. `MONEY` type supported in Azure SQL. |
| 8 | CSV Data Source | **Low** | **Low** | **S** | Upload to Azure Blob/ADLS. Create ADF DelimitedText dataset. |
| 9 | Excel Data Source | **Low** | **Medium** | **S** | Upload to Azure Blob/ADLS. ADF Excel connector has some limitations (e.g., sheet selection, header detection). Test carefully. |
| 10 | DB Connections | **Low** | **Medium** | **S** | Windows Integrated Security (`integratedSecurity=true`) must change to SQL Auth or Azure AD Auth in cloud. Connection strings need updating for Azure SQL endpoints. |
| 11 | PDF Documentation | **Low** | **Low** | **S** | No migration needed; update to reflect new ADF architecture. |

### Overall Risk Summary

| Risk Level | Count | Components |
|---|---|---|
| **Low** | 9 | DimBranch, DimAccount, DimCustomer, DDL, sp_DailyTransaction, sp_BalancePerCustomer, CSV, PDF, Documentation |
| **Medium** | 2 | FactTransaction (multi-source complexity), Excel (connector limitations), DB Connections (auth change) |
| **High** | 0 | None |

### Blockers & Concerns

1. **Authentication Model Change:** Source connections use Windows Integrated Security. Azure SQL requires SQL Authentication, Azure AD Authentication, or Managed Identity. Credential management must be planned.
2. **File Source Location:** CSV and Excel files are currently local filesystem references. These must be migrated to Azure Blob Storage or Azure Data Lake Storage Gen2.
3. **No Scheduling Metadata:** No scheduling information exists in the repository. ADF Triggers (schedule, tumbling window, or event-based) will need to be defined from scratch.
4. **No Error Handling:** Talend jobs have no explicit error handling, retry logic, or alerting. ADF pipelines should implement proper error handling, retry policies, and Azure Monitor alerts.
5. **No Incremental Load Pattern:** `Load_FactTransaction` uses a TRUNCATE-and-reload pattern. Consider implementing incremental loading (watermark-based) in ADF for better performance at scale.
6. **Date Format Parsing:** CSV uses `dd-MM-yyyy HH:mm:ss` format which differs from ISO standard. ADF's date parsing must be explicitly configured.

---

## 8. Assumptions & Limitations

### Assumptions
1. The target Azure environment will use **Azure SQL Database** (or Azure SQL Managed Instance) as the DWH platform.
2. File-based sources (CSV, Excel) will be migrated to **Azure Blob Storage** or **Azure Data Lake Storage Gen2**.
3. ADF Managed Identity or Azure Key Vault will be used for secure credential management.
4. The existing Star Schema design will be preserved in the target environment.
5. The `sample.bak` SQL Server backup contains the full source database schema and data as described in the Talend metadata.

### Limitations
1. **No SSIS packages exist** — the assessment focuses entirely on Talend jobs, SQL scripts, and data sources.
2. **Talend job screenshots** were not analyzed for visual flow details beyond what the XML metadata provides.
3. **The PDF documentation** was not parsed (binary format) — its contents are inferred from the README description.
4. **Data volumes** are small (sample data); performance characteristics at production scale cannot be assessed from this repository alone.
5. **No runtime logs or execution history** are available to assess job reliability or performance baselines.
6. **The Excel file contents** could not be fully inspected (binary OOXML); column structure is inferred from the Talend job metadata.
