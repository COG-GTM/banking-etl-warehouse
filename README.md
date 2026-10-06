# End-to-End Data Warehouse and ETL Pipeline for Banking Analytics

## Project Overview

This project is a comprehensive, end-to-end simulation of a real-world data engineering task developed during my project-based internship with **ID/X Partners** and **Rakamin Academy**. The primary objective was to address a common business challenge for a banking client: inefficient and delayed reporting due to operational data being scattered across multiple, disparate systems.

This repository contains the complete solution, which transforms raw data from various sources into a centralized, analytics-ready Data Warehouse, complete with automated ETL pipelines and pre-built analytical queries.

---

## Databricks / PySpark

The Talend jobs and T-SQL procedures have been ported to PySpark jobs that write Delta tables in a `dwh` schema (Unity Catalog or `hive_metastore`). The SQL Server / Talend setup further down is kept as historical reference.

| Talend / SQL Server | Databricks |
|---|---|
| `Load_DimBranch` -> `DimBranch` | `databricks/jobs/load_dim_branch.py` -> `dwh.dim_branch` |
| `Load_DimAccount` -> `DimAccount` | `databricks/jobs/load_dim_account.py` -> `dwh.dim_account` |
| `Load_DimCustomer` -> `DimCustomer` | `databricks/jobs/load_dim_customer.py` -> `dwh.dim_customer` |
| `Load_FactTransaction` -> `FactTransaction` | `databricks/jobs/load_fact_transaction.py` -> `dwh.fact_transaction` |
| `sp_DailyTransaction` | `analytics.reporting.daily_transaction(start_date, end_date)` |
| `sp_BalancePerCustomer` | `analytics.reporting.balance_per_customer(customer_name)` |

### Repo layout

```
databricks/
├── common/
│   ├── config.py        # job arguments (catalog, schema, landing path, secret scope) + secret lookup
│   ├── io.py            # JDBC / CSV / Excel readers, full-refresh Delta writer
│   └── runner.py        # spark_python_task entry-point boilerplate
├── jobs/
│   ├── load_dim_branch.py
│   ├── load_dim_account.py
│   ├── load_dim_customer.py      # customer LEFT JOIN city LEFT JOIN state, upper-cased name/address/gender
│   └── load_fact_transaction.py  # SQL + Excel + CSV union, dedup on transaction_id (SQL row wins),
│                                 # rows without a matching dim_account/dim_branch are rejected
├── analytics/
│   └── reporting.py     # daily_transaction(), balance_per_customer()
├── workflows/
│   └── etl_workflow.json  # Databricks Workflow (Jobs API 2.1) definition
├── run_all.py           # runs the same DAG without the Jobs service (notebook / local Spark)
├── requirements.txt     # local dev / test dependencies (pinned to DBR 15.4 LTS)
└── tests/               # pytest suite (local Spark)
```

Each job is a plain Python file with a pure `transform(...)` function and a `run(spark, cfg)` that reads the sources and overwrites its target table, mirroring Talend's truncate-and-reload.

### Required configuration

**Secrets.** The source `sample` database credentials are read from a Databricks secret scope (default `banking-etl`, override with `--secret-scope`):

| Key | Example |
|---|---|
| `sqlserver-jdbc-url` | `jdbc:sqlserver://<host>:1433;databaseName=sample;encrypt=true;trustServerCertificate=true` |
| `sqlserver-user` | `etl_reader` |
| `sqlserver-password` | `...` |

```bash
databricks secrets create-scope banking-etl
databricks secrets put-secret banking-etl sqlserver-jdbc-url
databricks secrets put-secret banking-etl sqlserver-user
databricks secrets put-secret banking-etl sqlserver-password
```

Outside Databricks (no `dbutils`), the same keys are read from environment variables: `SQLSERVER_JDBC_URL`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`.

**Landing files.** Upload `data_sources/transaction_csv.csv` and `data_sources/transaction_excel.xlsx` to the landing path (default `/Volumes/main/dwh/landing`):

```bash
databricks fs cp data_sources/transaction_csv.csv   dbfs:/Volumes/main/dwh/landing/
databricks fs cp data_sources/transaction_excel.xlsx dbfs:/Volumes/main/dwh/landing/
```

**Job arguments** (all jobs and `run_all.py` accept the same flags):

| Flag | Default | Notes |
|---|---|---|
| `--catalog` | `main` in the workflow, none locally | Pass `""` to use the default catalog / `hive_metastore` |
| `--schema` | `dwh` | Created if missing |
| `--landing-path` | `/Volumes/main/dwh/landing` | Directory with the CSV and Excel files |
| `--secret-scope` | `banking-etl` | |
| `--table-format` | `delta` | |

The SQL Server JDBC driver ships with Databricks Runtime. `openpyxl` (Excel reader) is installed as a task library by the workflow.

### Run the workflow

`databricks/workflows/etl_workflow.json` defines one job: `load_dim_branch`, `load_dim_account` and `load_dim_customer` run in parallel, and `load_fact_transaction` runs once all three succeed (`depends_on` + `run_if: ALL_SUCCESS`). Tasks run from this Git repo (`git_source`, branch `main`) on a shared job cluster. Adjust `node_type_id` for your cloud (`i3.xlarge` is AWS; e.g. `Standard_DS3_v2` on Azure) and the `parameters` defaults for your catalog.

```bash
databricks jobs create --json @databricks/workflows/etl_workflow.json   # prints {"job_id": ...}
databricks jobs run-now <job_id>
databricks jobs run-now <job_id> --json '{"job_parameters": {"catalog": "dev", "schema": "dwh"}}'
```

To update an existing job after editing the JSON: `databricks jobs reset --job-id <job_id> --json @databricks/workflows/etl_workflow.json` (the `reset` payload wraps the file under `new_settings`).

Without the Jobs service (e.g. from a notebook attached to a cluster, or local Spark), `run_all.py` reads the same JSON and runs the tasks in dependency waves, stopping before the fact load if a dimension fails:

```bash
python databricks/run_all.py --catalog main --schema dwh --landing-path /Volumes/main/dwh/landing
```

### Run a single job

Each job can be run on its own as a `spark_python_task` or with `python` on a cluster / local Spark (the fact job expects the dimension tables to exist):

```bash
python databricks/jobs/load_dim_branch.py --catalog main --schema dwh
python databricks/jobs/load_dim_account.py --catalog main --schema dwh
python databricks/jobs/load_dim_customer.py --catalog main --schema dwh
python databricks/jobs/load_fact_transaction.py --catalog main --schema dwh --landing-path /Volumes/main/dwh/landing
```

### Reporting

```python
import sys
sys.path.append("/Workspace/Repos/<user>/banking-etl-warehouse/databricks")  # repo checkout in the workspace

from analytics.reporting import daily_transaction, balance_per_customer

display(daily_transaction("2024-01-18", "2024-01-20"))   # Date, TotalTransactions, TotalAmount
display(balance_per_customer("shelly"))                   # CustomerName, AccountType, InitialBalance, CurrentBalance
```

Both functions default to the `dwh` schema of the current catalog; pass `catalog=` / `schema=` to point elsewhere. `balance_per_customer` matches names like `LIKE '%name%'` under SQL Server's default case-insensitive collation.

### Tests

```bash
pip install -r databricks/requirements.txt
pytest databricks/tests
```

Spark 3.5 needs Java 8/11/17.

---

## Historical reference: Talend + SQL Server

Everything below describes the original Talend Open Studio / SQL Server implementation.

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

To replicate this solution, follow these steps:

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
