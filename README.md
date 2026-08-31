# End-to-End Data Warehouse and ETL Pipeline for Banking Analytics

## Project Overview

This project is a comprehensive, end-to-end simulation of a real-world data engineering task developed during my project-based internship with **ID/X Partners** and **Rakamin Academy**. The primary objective was to address a common business challenge for a banking client: inefficient and delayed reporting due to operational data being scattered across multiple, disparate systems.

This repository contains the complete solution, which transforms raw data from various sources into a centralized, analytics-ready Data Warehouse, complete with automated transformation pipelines and pre-built analytical queries. The current implementation is the dbt project targeting Databricks SQL; the original SQL Server and Talend implementation remains available for reference.

---

## 🏛️ Solution Architecture

The current solution follows a dbt medallion architecture on Databricks SQL, designed to create a robust and scalable single source of truth. The legacy SQL Server and Talend architecture below is preserved as a reference for the original implementation.

### Legacy architecture (reference)

**The legacy data flows through four key stages:**
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

*   **Platform:** Databricks SQL (Unity Catalog, Delta Lake)
*   **Transformation:** dbt (>= 1.8, dbt-databricks) with dbt_utils + dbt_expectations
*   **Architecture:** Medallion architecture (bronze → silver → gold) with a star schema in gold
*   **Development:** Test-driven development (schema tests + dbt unit tests authored before models)
*   **CI/CD:** GitHub Actions CI (`dbt build` + `dbt docs generate`)
*   **Version Control:** Git & GitHub

The legacy Talend/SQL Server implementation is preserved under `sql_scripts/`, `talend_jobs/`, and `data_sources/` for reference.

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

### Legacy (SQL Server + Talend)

To replicate the legacy solution, follow these steps:

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

### Modern (dbt + Databricks)

Install dbt, resolve the project packages, configure the Databricks connection, and run the medallion build:

```bash
pip install "dbt-core>=1.8" "dbt-databricks>=1.8"
dbt deps
export DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<personal-access-token>"
export DATABRICKS_CATALOG="banking_dev"
export DATABRICKS_SCHEMA="dbt"
DBT_PROFILES_DIR=. dbt build
```

---

## 🌟 Project Outcomes

This project successfully demonstrates a complete data engineering lifecycle. The final solution transforms a chaotic, multi-source data environment into a clean, reliable, and high-performance Data Warehouse, ready to power business intelligence and analytics.

## dbt + Databricks Medallion Rebuild

This repository is being rebuilt as a dbt project targeting Databricks SQL with a medallion architecture:

- **Bronze**: Typed passthrough views of raw sources.
- **Silver**: Conformed and deduplicated tables.
- **Gold**: Star-schema dimensions and facts, plus report models.

The rebuild follows a test-driven workflow. Schema and unit tests are authored before their models, and `dbt build` runs models and tests together.

### Run order

`dbt build` resolves the DAG automatically. The modern flow is:

```text
seeds → bronze views → silver → gold dimensions → fact → report models
```

The legacy sequence `Load_DimBranch` → `Load_DimAccount` → `Load_DimCustomer` → `Load_FactTransaction` now corresponds to `slv_branch`/`slv_account`/`slv_customer` → `slv_transaction` → the gold models.

Layer-scoped builds are also available:

```bash
dbt build --select bronze
dbt build --select silver
dbt build --select gold
```

Report models support optional dbt variables:

- `daily_transaction_start_date`
- `daily_transaction_end_date`
- `balance_customer_name`

For example:

```bash
dbt build --select rpt_daily_transaction --vars '{daily_transaction_start_date: 2024-01-18, daily_transaction_end_date: 2024-01-20}'
```

GitHub Actions requires the following repository secrets to enable warehouse-backed builds and documentation generation: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN`. Optional repository variables `DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA` override their defaults.

### Running the dbt project

Install the project dependencies, resolve packages, and run the build:

```bash
pip install 'dbt-core>=1.8' 'dbt-databricks>=1.8'
dbt deps
DBT_PROFILES_DIR=. dbt build
```
