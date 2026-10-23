# End-to-End Data Warehouse and ETL Pipeline for Banking Analytics

## Project Overview

This project is a comprehensive, end-to-end simulation of a real-world data engineering task developed during my project-based internship with **ID/X Partners** and **Rakamin Academy**. The primary objective was to address a common business challenge for a banking client: inefficient and delayed reporting due to operational data being scattered across multiple, disparate systems.

This repository contains the complete solution, which transforms raw data from various sources into a centralized, analytics-ready Data Warehouse, complete with automated ETL pipelines and pre-built analytical queries.

---

## 🏛️ Solution Architecture

The solution follows a classic ETL (Extract, Transform, Load) architecture, designed to create a robust and scalable single source of truth.

**The data flows through four key stages:**
1.  **Data Sources:** Raw data is ingested from three different types of systems:
    - Relational Database (SQL Server)
    - Excel Files (`.xlsx`)
    - CSV Files (`.csv`)
2.  **ETL Processing (Talend):** Talend Open Studio is used as the core ETL engine to perform:
    - **Extraction:** Pulling data from all 8 distinct sources.
    - **Transformation:** Cleansing data, joining multiple tables, unifying different data streams, and deduplicating records.
    - **Loading:** Loading the clean, transformed data into the target Data Warehouse.
3.  **Data Warehouse (SQL Server):** A centralized DWH built on Microsoft SQL Server using a Star Schema data model. It consists of:
    - **3 Dimension Tables:** `DimCustomer`, `DimAccount`, `DimBranch`
    - **1 Fact Table:** `FactTransaction`
4.  **Data Access & Analytics:** Pre-built Stored Procedures provide quick, aggregated insights for business users and analysts, enabling faster decision-making.

---

## 🛠️ Tech Stack

*   **Database:** Microsoft SQL Server
*   **ETL Tool:** Talend Open Studio for Data Integration
*   **Data Modeling:** Star Schema
*   **Language:** T-SQL (for Stored Procedures)
*   **Version Control:** Git & GitHub

---

## ✨ Key Features & Implementation Details

### 1. Data Warehouse Design (Star Schema)
The DWH was designed from scratch with a focus on analytical performance and clarity.

- **`DimCustomer`**: A consolidated view of customer information, enriched with city and state data from separate tables.
- **`DimAccount` & `DimBranch`**: Dimension tables providing descriptive context for accounts and bank branches.
- **`FactTransaction`**: The core table containing all unique transaction records from the three source systems. Primary and Foreign keys were implemented to ensure data integrity.

### 2. Modular ETL Pipelines in Talend
A total of four distinct Talend jobs were created for modularity and maintainability:

- **`Load_DimBranch` & `Load_DimAccount`**: Simple pipelines to load master data.
- **`Load_DimCustomer`**: A more complex pipeline featuring multi-table **JOINs** within `tMap` to combine `customer`, `city`, and `state` data. It also includes data cleansing steps like converting text fields to uppercase.
- **`Load_FactTransaction`**: The main integration pipeline that unifies data from all three transaction sources (`tUnite`), removes duplicates based on `transaction_id` (`tUniqRow`), and formats the final output for loading.

### 3. Automated Business Reports (Stored Procedures)
To provide immediate value to the "client," two parameterized Stored Procedures were developed:

- **`sp_DailyTransaction`**: Generates a daily summary of transaction volume and total amount for a given date range.
- **`sp_BalancePerCustomer`**: A sophisticated procedure that calculates the current balance of each active account for a specific customer, applying business logic (`CASE WHEN`) to handle deposits and withdrawals.

---

## 🚀 How to Run This Project

The warehouse now runs on Databricks: Delta Lake tables, Unity Catalog, Spark SQL, and one
Databricks Workflow (`banking_dwh_pipeline`). The original SQL Server + Talend steps are kept
under [Legacy (SQL Server + Talend)](#legacy-sql-server--talend).

### 1. Prerequisites
- Databricks workspace with Unity Catalog and serverless jobs compute.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) v0.2xx+ authenticated
  (`databricks auth login` or `DATABRICKS_HOST`/`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET`).
- A secret scope with the SQL Server JDBC credentials (scope and key names: `databricks/config/environments.yml` → `jdbc`).
- For local tests: Python 3.10+, JDK 17 or 21.

### 2. Targets and namespaces
| Target | Catalog | Schemas | Bundle root |
|---|---|---|---|
| `dev` (default) | `migration_demo` | `bdwh_bronze`, `bdwh_silver`, `bdwh_gold`, `bdwh_ops` | `/Workspace/Shared/banking_dwh_migration/bundle` |
| `prod` | `banking_dwh` | `bronze`, `silver`, `gold`, `ops` | `/Workspace/Shared/banking_dwh/bundle` |

Names are resolved by `EnvConfig` (`databricks/src/banking_dwh/common/config.py`) from
`databricks/config/environments.yml`. Don't hard-code them.

### 3. Deploy the bundle
```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy   -t dev          # or -t prod
```
`databricks.yml` includes `resources/jobs/*.yml`, which deploys the job `banking_dwh_pipeline_<target>`.
Before deploying to prod, set `failure_email` (the default is a placeholder): `--var failure_email=team@bank.com`.

### 4. Run the pipeline
```bash
databricks bundle run -t dev banking_dwh_pipeline
# override job parameters for a one-off run, e.g. a scratch namespace:
databricks bundle run -t dev banking_dwh_pipeline --params schema_prefix=bdwh_t10_
```
Tasks run on serverless compute (environment client 3, plus `pyyaml`), in this order:

| # | Task | Notebook | Replaces |
|---|---|---|---|
| 1 | `setup_unity_catalog` | `notebooks/00_setup/01_setup_unity_catalog` | Creating the `DWH` database |
| 2 | `create_star_schema` | `notebooks/00_setup/02_create_star_schema` | `sql_scripts/01_create_tables.sql` |
| 3 | `bronze_ingest_sqlserver`, `bronze_ingest_csv_autoloader`, `bronze_ingest_excel` (parallel) | `notebooks/bronze/01..03_*` | Talend `tMSSqlInput`, `tFileInputDelimited`, `tFileInputExcel` |
| 4 | `load_dim_branch` → `load_dim_account` → `load_dim_customer` → `load_fact_transaction` | `notebooks/silver/01..04_*` | `Load_DimBranch`, `Load_DimAccount`, `Load_DimCustomer`, `Load_FactTransaction` |
| 5 | `dq_star_schema` | `notebooks/00_setup/03_dq_star_schema` | The SQL Server PK/FK constraints |
| 6 | `gold_objects` | `notebooks/gold/` (Ticket 9) | `sp_DailyTransaction`, `sp_BalancePerCustomer` |
| 7 | `reconcile` | `notebooks/orchestration/01_reconcile` | Manual comparison against the legacy DWH |

Retries: Bronze tasks retry twice (also on timeout), setup, Silver and Gold tasks retry once, and the two DQ gates never
retry. Every task has a timeout, and the run has a 3 h cap plus a 90 min duration warning. Failure and duration alerts go to
`failure_email`. A daily 02:00 UTC schedule is deployed **paused**.

### 5. Job parameters
| Parameter | Default | Meaning |
|---|---|---|
| `target` | bundle target (`dev`/`prod`) | Which `environments.yml` target to resolve names from. |
| `catalog` | blank (from config) | Catalog override. |
| `schema_prefix` | blank (from config) | Schema prefix override (`-` = no prefix). |

All three are job-level parameters, and every task receives them explicitly as notebook parameters. This matters because
the notebooks' `target` widget defaults to `dev`. Widget values that end up in SQL are validated as plain identifiers.

### 6. Data quality and reconciliation
- **`dq_star_schema`** runs PK not-null/unique checks and the two FK checks (`FK_FactTransaction_DimAccount`,
  `FK_FactTransaction_DimBranch`). It appends to `<ops>.dq_results` and fails the run on any violation.
- **`reconcile`** compares every Silver table and Gold object with the legacy SQL Server output captured in
  `databricks/fixtures/parity/`. It checks row count, distinct keys, SUM of every money column (`amount`, `balance`,
  `total_amount`, `current_balance`, ...), and a full-row diff. Comparisons are exact. One row per metric goes to
  `<ops>.reconciliation_results` and one row per object to `<ops>.dq_results`. The task fails on any mismatch.
```sql
SELECT check_name, expected, actual, passed
FROM migration_demo.bdwh_ops.reconciliation_results
WHERE run_id = '<job run id>' AND NOT passed;
```
- To regenerate the fixtures (Docker SQL Server 2022 + the Talend-equivalent load):
  `python databricks/fixtures/parity/build_parity.py` (see the docstring of that script).
- Rehearse the gate without touching shared tables. Run `notebooks/orchestration/90_seed_parity_scratch` and then
  `01_reconcile` with `schema_prefix=bdwh_t<N>_` (the seeder refuses any other namespace). Pass `break=fact_row_missing`
  to see the gate fail.

### 7. Local tests
```bash
cd databricks
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 pytest
```

### Legacy (SQL Server + Talend)

The original run book. Kept for reference and for regenerating the parity fixtures (`databricks/fixtures/parity/build_parity.py` automates steps 2-5 in Docker).


1.  **Prerequisites:**
    - Microsoft SQL Server and SQL Server Management Studio (SSMS) installed.
    - Talend Open Studio for Data Integration installed.

2.  **Setup the Source Database:**
    - In SSMS, restore the source database using the provided `sample.bak` file. This will create the `sample` database with all necessary source tables.

3.  **Build the Data Warehouse:**
    - In the `sql_scripts` folder of this repository, you will find `create_tables.sql`.
    - Open this script in SSMS and execute it to create the `DWH` database and all dimension and fact tables.

4.  **Configure and Run the Talend Jobs:**
    - Open Talend Studio and import the project/jobs from this repository.
    - Set up the database connections in the Metadata section for both the `sample` and `DWH` databases.
    - Run the Talend jobs in the following order to ensure data dependencies are met:
        1. `Load_DimBranch`
        2. `Load_DimAccount`
        3. `Load_DimCustomer`
        4. `Load_FactTransaction`

5.  **Deploy and Test the Stored Procedures:**
    - In the `sql_scripts` folder, open `create_procedures.sql`.
    - Execute this script in SSMS against the `DWH` database.
    - You can now test the procedures with sample commands, e.g., `EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-20';`.

---

## 🌟 Project Outcomes

This project successfully demonstrates a complete data engineering lifecycle. The final solution transforms a chaotic, multi-source data environment into a clean, reliable, and high-performance Data Warehouse, ready to power business intelligence and analytics.
