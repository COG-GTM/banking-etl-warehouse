# Setup Guide: Banking ETL Data Warehouse

This guide walks you through setting up and running the complete banking data warehouse pipeline from a fresh clone. Two paths are provided:

- **Path A (Quick Start):** SQL scripts only — no Talend needed. Ideal for verifying the warehouse schema and stored procedures.
- **Path B (Full Pipeline):** Restore source DB, run Talend ETL jobs, then verify. This is the production-like flow.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Structure](#2-repository-structure)
3. [Path A: Quick Start (No Talend)](#3-path-a-quick-start-no-talend)
4. [Path B: Full Pipeline (With Talend)](#4-path-b-full-pipeline-with-talend)
5. [Source File Placement](#5-source-file-placement)
6. [Talend Job Execution Order](#6-talend-job-execution-order)
7. [Verification](#7-verification)
8. [Stored Procedure Usage](#8-stored-procedure-usage)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Microsoft SQL Server** | 2019+ (Express is fine) | Source DB + DWH hosting |
| **SQL Server Management Studio (SSMS)** or **sqlcmd** | Latest | Running SQL scripts |
| **Talend Open Studio for Data Integration** | 8.0+ | ETL jobs *(Path B only)* |
| **Git** | Any | Clone this repo |

### Optional (for automation scripts)

| Tool | Purpose |
|------|---------|
| **sqlcmd CLI** | Used by `scripts/init_databases.sh` and `.ps1` |
| **Bash** (Linux/macOS/WSL) | Shell automation script |
| **PowerShell** (Windows) | Windows automation script |

### Install sqlcmd (if not already available)

- **Windows:** Included with SQL Server or SSMS. Also available via `winget install Microsoft.SqlServer.SqlCmd`.
- **Linux:** [Microsoft docs](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools)
- **macOS:** `brew install microsoft/mssql-release/mssql-tools18`

---

## 2. Repository Structure

```
banking-etl-warehouse/
├── SETUP.md                          # This file
├── README.md                         # Project overview and architecture
├── data_sources/
│   ├── sample.bak                    # SQL Server backup of source database
│   ├── transaction_csv.csv           # CSV transaction source (IDs 14-25)
│   └── transaction_excel.xlsx        # Excel transaction source (IDs 6-7, 11-15)
├── sql_scripts/
│   ├── 01_create_tables.sql          # Create DWH database + star schema
│   ├── 02_create_procedures.sql      # Deploy stored procedures
│   ├── 03_create_source_database.sql # Create source DB schema (alternative to .bak)
│   ├── 04_seed_source_data.sql       # Seed source DB with sample data
│   ├── 05_seed_dwh_data.sql          # Seed DWH directly (bypasses Talend)
│   └── 06_verify_warehouse.sql       # Verification checks (11 tests)
├── scripts/
│   ├── init_databases.sh             # Bash automation script
│   └── init_databases.ps1            # PowerShell automation script
└── talend_jobs/
    ├── Load_DimBranch.zip            # Talend job: load branch dimension
    ├── Load_DimAccount.zip           # Talend job: load account dimension
    ├── Load_DimCustomer.zip          # Talend job: load customer dimension
    └── Load_FactTransaction.zip      # Talend job: load fact table
```

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────────┐     │
│  │  SQL Server   │  │  transaction_    │  │  transaction_     │     │
│  │  (sample DB)  │  │  csv.csv         │  │  excel.xlsx       │     │
│  │  6 tables     │  │  IDs 14-25       │  │  IDs 6-7, 11-15  │     │
│  └──────┬───────┘  └────────┬─────────┘  └─────────┬─────────┘     │
│         │                   │                       │               │
└─────────┼───────────────────┼───────────────────────┼───────────────┘
          │                   │                       │
          ▼                   ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TALEND ETL PIPELINE                               │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐      │
│  │ Load_DimBranch │  │ Load_DimAccount│  │ Load_DimCustomer │      │
│  │ (parallel OK)  │  │ (parallel OK)  │  │ (parallel OK)    │      │
│  └────────┬───────┘  └────────┬───────┘  └────────┬─────────┘      │
│           │                   │                    │                │
│           └───────────────────┼────────────────────┘                │
│                               │ (must complete first)              │
│                    ┌──────────▼──────────┐                          │
│                    │Load_FactTransaction │                          │
│                    │ tUnite + tUniqRow   │                          │
│                    └──────────┬──────────┘                          │
└───────────────────────────────┼─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE (DWH)                              │
│              ┌─────────────────────────┐                            │
│              │    FactTransaction      │                            │
│              │  (25 deduplicated rows) │                            │
│              └───┬─────────────────┬───┘                            │
│                  │                 │                                 │
│          ┌───────▼──────┐  ┌──────▼───────┐  ┌──────────────┐      │
│          │  DimAccount  │  │  DimBranch   │  │ DimCustomer  │      │
│          │  (25 rows)   │  │  (5 rows)    │  │ (25 rows)    │      │
│          └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                                     │
│  Stored Procedures:                                                 │
│    sp_DailyTransaction    - Daily volume & amount summary           │
│    sp_BalancePerCustomer  - Current balance per customer account    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Path A: Quick Start (No Talend)

This path uses SQL scripts to set up everything, including seeding the DWH directly. No Talend installation required.

### Option 1: Automated (Recommended)

**Linux / macOS / WSL:**
```bash
./scripts/init_databases.sh \
    --password "YourSqlServerPassword" \
    --seed-dwh \
    --verify
```

**Windows PowerShell:**
```powershell
.\scripts\init_databases.ps1 `
    -Password "YourSqlServerPassword" `
    -SeedDwh `
    -Verify
```

**Windows Authentication:**
```bash
# Bash
./scripts/init_databases.sh --windows-auth --seed-dwh --verify

# PowerShell
.\scripts\init_databases.ps1 -WindowsAuth -SeedDwh -Verify
```

### Option 2: Manual (Step-by-Step in SSMS)

1. **Create the DWH database and tables:**
   - Open `sql_scripts/01_create_tables.sql` in SSMS
   - Execute (F5). This creates the `DWH` database with `DimAccount`, `DimBranch`, `DimCustomer`, and `FactTransaction`.

2. **Deploy stored procedures:**
   - Open `sql_scripts/02_create_procedures.sql` in SSMS
   - Execute. This creates `sp_DailyTransaction` and `sp_BalancePerCustomer`.

3. **Seed the DWH with sample data:**
   - Open `sql_scripts/05_seed_dwh_data.sql` in SSMS
   - Execute. This populates all four tables with representative data.

4. **Verify the setup:**
   - Open `sql_scripts/06_verify_warehouse.sql` in SSMS
   - Execute. All 11 checks should show `PASS`.

5. **Test stored procedures:**
   ```sql
   EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-22';
   EXEC sp_BalancePerCustomer @customer_name = 'ANDI';
   ```

---

## 4. Path B: Full Pipeline (With Talend)

This path mirrors the production ETL flow using Talend Open Studio.

### Step 1: Set Up the Source Database

**Option A — Restore from backup (recommended):**

1. Open SSMS and connect to your SQL Server instance.
2. Right-click **Databases** → **Restore Database...**
3. Select **Device** → **...** → **Add** → browse to `data_sources/sample.bak`
4. Click **OK** to restore. This creates the `sample` database with all 6 source tables.

**Option B — Create from scripts (if .bak restore fails):**

```sql
-- Run in SSMS in this order:
-- 1. Create source database schema:
--    Open and execute: sql_scripts/03_create_source_database.sql
-- 2. Seed source data:
--    Open and execute: sql_scripts/04_seed_source_data.sql
```

Or via automation:
```bash
./scripts/init_databases.sh --password "YourPassword"
```

### Step 2: Create the DWH Database

Execute `sql_scripts/01_create_tables.sql` in SSMS against your server. This creates the `DWH` database with the star schema.

### Step 3: Deploy Stored Procedures

Execute `sql_scripts/02_create_procedures.sql` in SSMS against the `DWH` database.

### Step 4: Configure Talend

1. Open **Talend Open Studio for Data Integration**.
2. **Import the project:**
   - File → Import → Existing Talend Projects
   - Extract any of the `.zip` files from `talend_jobs/` — they all contain the same `IDX_INTERNSHIP` project
   - Select the extracted `IDX_INTERNSHIP` folder
3. **Configure database connections** in the Metadata section:
   - **Sample_DB_Connection:** Points to the `sample` database
     - Server: `localhost`
     - Port: `1433`
     - Database: `sample`
     - Authentication: Windows Auth or SQL login
   - **DWH_DB_Connection:** Points to the `DWH` database
     - Server: `localhost`
     - Port: `1433`
     - Database: `DWH`
     - Authentication: Windows Auth or SQL login

### Step 5: Place Source Files

Talend's `Load_FactTransaction` job reads from CSV and Excel files. Ensure these files are accessible to Talend:

| File | Default Location | Description |
|------|-----------------|-------------|
| `transaction_csv.csv` | `data_sources/transaction_csv.csv` | 12 CSV transactions (IDs 14-25) |
| `transaction_excel.xlsx` | `data_sources/transaction_excel.xlsx` | 7 Excel transactions (IDs 6-7, 11-15) |

> **Note:** You may need to update the file paths in the Talend job's `tFileInputDelimited` and `tFileInputExcel` components to match your local file system.

### Step 6: Run Talend Jobs

See [Section 6: Talend Job Execution Order](#6-talend-job-execution-order) below.

### Step 7: Verify

Run `sql_scripts/06_verify_warehouse.sql` in SSMS. All 11 checks should show `PASS`.

---

## 5. Source File Placement

| Source | File | Location | Format | Records |
|--------|------|----------|--------|---------|
| SQL Server | `sample.bak` | `data_sources/sample.bak` | SQL Server backup | 6 tables |
| CSV | `transaction_csv.csv` | `data_sources/transaction_csv.csv` | Comma-separated, UTF-8 | 12 rows (IDs 14-25) |
| Excel | `transaction_excel.xlsx` | `data_sources/transaction_excel.xlsx` | OOXML (.xlsx) | 7 rows (IDs 6-7, 11-15) |

### Source Database Tables (inside `sample.bak` or created by `03_create_source_database.sql`)

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `state` | `state_id` | US state reference (5 rows) |
| `city` | `city_id` | City reference, FK → state (10 rows) |
| `branch` | `branch_id` | Bank branch locations (5 rows) |
| `customer` | `customer_id` | Customer master data, FK → city (25 rows) |
| `account` | `account_id` | Customer accounts, FK → customer (25 rows) |
| `transaction_db` | `transaction_id` | SQL Server transactions, FK → account, branch (13 rows) |

### CSV File Schema (`transaction_csv.csv`)

```
transaction_id,account_id,transaction_date,amount,transaction_type,branch_id
```

Date format: `DD-MM-YYYY HH:MM:SS`

### Excel File Schema (`transaction_excel.xlsx`)

Sheet: `Sheet1` — same columns as CSV. Date format: native Excel datetime.

---

## 6. Talend Job Execution Order

The four Talend ETL jobs must be run in a specific order due to foreign key constraints in the DWH.

```
Phase 1 (can run in parallel):
  ├── Load_DimBranch     → Populates DimBranch from sample.dbo.branch
  ├── Load_DimAccount    → Populates DimAccount from sample.dbo.account
  └── Load_DimCustomer   → Populates DimCustomer from sample.dbo.customer
                           (joins with city + state; converts names to UPPERCASE)

Phase 2 (must run AFTER Phase 1):
  └── Load_FactTransaction → Populates FactTransaction
                              Sources: transaction_db + CSV + Excel
                              Uses tUnite (merge streams) + tUniqRow (deduplicate)
                              Requires DimAccount and DimBranch to exist (FK constraints)
```

### Step-by-step in Talend:

1. Open the `IDX_INTERNSHIP` project in Talend Studio.
2. In the **Repository** panel, expand **Job Designs**.
3. **Double-click** `Load_DimBranch` → click **Run** (F6). Wait for completion.
4. **Double-click** `Load_DimAccount` → click **Run** (F6). Wait for completion.
5. **Double-click** `Load_DimCustomer` → click **Run** (F6). Wait for completion.
6. **Double-click** `Load_FactTransaction` → click **Run** (F6). Wait for completion.

> **Tip:** Jobs 1-3 can run simultaneously if you open them in separate tabs. Job 4 must wait for all three to finish.

### What Each Job Does

| Job | Source(s) | Target | Key Transformations |
|-----|-----------|--------|-------------------|
| `Load_DimBranch` | `sample.dbo.branch` | `DWH.dbo.DimBranch` | Direct mapping |
| `Load_DimAccount` | `sample.dbo.account` | `DWH.dbo.DimAccount` | Direct mapping |
| `Load_DimCustomer` | `sample.dbo.customer` + `city` + `state` | `DWH.dbo.DimCustomer` | Multi-table JOIN via tMap; UPPERCASE customer names |
| `Load_FactTransaction` | `transaction_db` + CSV + Excel | `DWH.dbo.FactTransaction` | tUnite (merge 3 streams) → tUniqRow (deduplicate by transaction_id) |

---

## 7. Verification

After completing either Path A or Path B, run the verification script to confirm everything is correct.

### Run Verification

**In SSMS:** Open and execute `sql_scripts/06_verify_warehouse.sql`

**Via automation:**
```bash
# Just the verification step
./scripts/init_databases.sh --password "YourPassword" --verify --skip-source
```

### Verification Checks (11 total)

| # | Check | Expected |
|---|-------|----------|
| 1 | DimBranch row count | >= 5 |
| 2 | DimAccount row count | >= 25 |
| 3 | DimCustomer row count | >= 25 |
| 4 | DimCustomer names are UPPERCASE | 0 lowercase names |
| 5 | FactTransaction row count | >= 25 |
| 6 | No duplicate TransactionIDs | 0 duplicates |
| 7 | FK integrity: AccountID → DimAccount | 0 orphans |
| 8 | FK integrity: BranchID → DimBranch | 0 orphans |
| 9 | sp_DailyTransaction returns results | > 0 rows |
| 10 | sp_BalancePerCustomer returns results | > 0 rows |
| 11 | Transaction type coverage | >= 4 types |

All checks should show `PASS`. Any `FAIL` indicates a problem with the data load.

---

## 8. Stored Procedure Usage

### sp_DailyTransaction

Generates a daily summary of transaction volume and total amount.

```sql
-- Parameters: @start_date DATE, @end_date DATE
EXEC sp_DailyTransaction
    @start_date = '2024-01-18',
    @end_date = '2024-01-22';
```

**Expected output with seed data:**

| Date | TotalTransactions | TotalAmount |
|------|------------------|-------------|
| 2024-01-18 | 5 | 2,050,000 |
| 2024-01-19 | 4 | 1,280,000 |
| 2024-01-20 | 4 | 2,600,000 |
| 2024-01-21 | 2 | 2,000,000 |
| 2024-01-22 | 10 | 5,180,000 |

### sp_BalancePerCustomer

Calculates the current balance for each active account of a customer.

```sql
-- Parameters: @customer_name VARCHAR(100) — supports partial match
EXEC sp_BalancePerCustomer @customer_name = 'ANDI';
```

**Expected output with seed data:**

| CustomerName | AccountType | InitialBalance | CurrentBalance |
|-------------|-------------|----------------|----------------|
| ANDI PRATAMA | savings | 5,000,000 | 5,500,000 |

> The current balance = initial balance + deposits - withdrawals/transfers/payments.

---

## 9. Troubleshooting

### sample.bak restore fails

- **Version mismatch:** The `.bak` was created on SQL Server 2022. If you're running an older version, use the script-based approach instead:
  ```sql
  -- Execute in order:
  -- sql_scripts/03_create_source_database.sql
  -- sql_scripts/04_seed_source_data.sql
  ```
- **File path issues:** SQL Server needs read access to the `.bak` file location. Copy it to a path like `C:\temp\sample.bak` or `/var/opt/mssql/backup/`.

### sqlcmd connection fails

- Verify SQL Server is running: `sqlcmd -S localhost -Q "SELECT 1"`
- Check the port: Default is 1433. Docker-based SQL Server may use a different port.
- For named instances: Use `--server "localhost\INSTANCENAME"` (omit the `--port`).
- Enable TCP/IP in SQL Server Configuration Manager if connection is refused.

### Talend can't find CSV/Excel files

- Update the file paths in the Talend job components to match your local file system.
- The files are in `data_sources/` relative to the repo root.
- Use absolute paths in Talend (e.g., `C:\repos\banking-etl-warehouse\data_sources\transaction_csv.csv`).

### MERGE statements fail (older SQL Server)

The seed scripts use `MERGE` statements which require SQL Server 2008+. If you're on an older version, replace each `MERGE` with individual `INSERT` statements and add `IF NOT EXISTS` checks.

### Stored procedure already exists

If you get "There is already an object named 'sp_DailyTransaction'", the procedures were already created. Either:
- Drop them first: `DROP PROCEDURE sp_DailyTransaction; DROP PROCEDURE sp_BalancePerCustomer;`
- Or change `CREATE PROCEDURE` to `ALTER PROCEDURE` in the script.
