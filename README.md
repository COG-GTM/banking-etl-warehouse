# End-to-End Data Warehouse and ETL Pipeline for Banking Analytics

## Project Overview

This project is a comprehensive, end-to-end simulation of a real-world data engineering task developed during my project-based internship with **ID/X Partners** and **Rakamin Academy**. The primary objective was to address a common business challenge for a banking client: inefficient and delayed reporting due to operational data being scattered across multiple, disparate systems.

This repository contains the complete solution, which transforms raw data from various sources into a centralized, analytics-ready Data Warehouse (Databricks + Delta Lake), complete with automated ETL pipelines and pre-built analytical notebooks.

> **Platform migration:** the warehouse and analytics now run on **Databricks (Delta Lake, Spark SQL / PySpark)** — see [`databricks/`](databricks/). The original SQL Server + Talend + T-SQL implementation (`sql_scripts/`, `talend_jobs/`) is retained for reference only and is no longer the supported path.

---

## 🏛️ Solution Architecture

The solution follows a classic ETL (Extract, Transform, Load) architecture, designed to create a robust and scalable single source of truth.

**The data flows through four key stages:**
1.  **Data Sources:** Raw data is ingested from three different types of systems:
    - Relational Database (SQL Server)
    - Excel Files (`.xlsx`)
    - CSV Files (`.csv`)
2.  **ETL Processing (PySpark on Databricks):** A PySpark ingestion notebook is the core ETL engine:
    - **Extraction:** Pulling data from all 8 distinct sources (JDBC, CSV, Excel).
    - **Transformation:** Cleansing data, joining multiple tables, unifying different data streams, and deduplicating records.
    - **Loading:** Writing the clean, transformed data to the target Delta tables.
3.  **Data Warehouse (Delta Lake):** A centralized DWH built on Databricks Delta Lake using a Star Schema data model. It consists of:
    - **3 Dimension Tables:** `DimCustomer`, `DimAccount`, `DimBranch`
    - **1 Fact Table:** `FactTransaction`
4.  **Data Access & Analytics:** Parameterized Databricks notebooks (with equivalent Spark SQL) provide quick, aggregated insights for business users and analysts, enabling faster decision-making.

---

## 🛠️ Tech Stack

*   **Platform:** Databricks
*   **Storage / Tables:** Delta Lake
*   **ETL:** PySpark notebook (`databricks/04_ingest_sources.py`)
*   **Data Modeling:** Star Schema
*   **Language:** Spark SQL & PySpark
*   **Version Control:** Git & GitHub
*   **Legacy (reference only):** Microsoft SQL Server, Talend Open Studio, T-SQL

---

## ✨ Key Features & Implementation Details

### 1. Data Warehouse Design (Star Schema)
The DWH was designed from scratch with a focus on analytical performance and clarity.

- **`DimCustomer`**: A consolidated view of customer information, enriched with city and state data from separate tables.
- **`DimAccount` & `DimBranch`**: Dimension tables providing descriptive context for accounts and bank branches.
- **`FactTransaction`**: The core table containing all unique transaction records from the three source systems. Delta Lake does not enforce PK/FK constraints, so the original keys are documented as informational (column comments plus optional Unity Catalog informational constraints) and key columns are `NOT NULL`.

SQL Server types are mapped to Spark/Delta equivalents: `MONEY` → `DECIMAL(19,4)`, `DATETIME` → `TIMESTAMP`, `VARCHAR(n)` → `STRING`.

### 2. PySpark Ingestion Pipeline
`databricks/04_ingest_sources.py` replaces the four Talend jobs in a single notebook:

- **Dimensions**: `DimBranch` and `DimAccount` load master data directly; `DimCustomer` joins `customer`, `city` and `state` and uppercases text fields (the old `tMap` logic).
- **Fact**: the relational, CSV and Excel transaction streams are unioned (`tUnite`) and deduplicated on `transaction_id` (`tUniqRow`) before being written to Delta.

### 3. Automated Business Reports (Notebooks)
Two parameterized notebooks replace the stored procedures, each also embedding the equivalent Spark SQL:

- **`databricks/02_daily_transaction.py`** (was `sp_DailyTransaction`): daily summary of transaction volume and total amount for a given date range, **plus a moving-average smoothing column**.
- **`databricks/03_balance_per_customer.py`** (was `sp_BalancePerCustomer`): current balance of each active account for a customer, applying the deposit/withdrawal business logic.

### 4. Moving-Average Smoothing (new)
The daily transaction report adds `SmoothedTotalAmount`, a trailing moving average over the daily `TotalAmount` series:

```sql
AVG(TotalAmount) OVER (ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS SmoothedTotalAmount
```

The window size is configurable through the `window_size` widget (default 7 days), the series is ordered by `Date`, and the first `window_size - 1` rows are a running mean over the days available so far.

---

## 🚀 How to Run This Project

To replicate this solution, follow these steps:

1.  **Prerequisites:**
    - A Databricks workspace and a cluster (or SQL warehouse) with Unity Catalog or Hive metastore access.
    - To read the `.xlsx` source, the `com.crealytics:spark-excel` library installed on the cluster.

2.  **Import the notebooks:**
    - Sync this repo with Databricks Repos, or import the files in `databricks/` into your workspace.

3.  **Build the Data Warehouse:**
    - Run `databricks/01_create_tables.sql` with the `catalog` and `schema` parameters (e.g. `main` / `dwh`) to create the Delta dimension and fact tables.

4.  **Load the data:**
    - Upload `data_sources/` to a Unity Catalog volume or DBFS and run `databricks/04_ingest_sources.py`, setting the JDBC and file-path widgets. Dimensions load before the fact table within the notebook, so data dependencies are met automatically.

5.  **Run the analytics notebooks:**
    - `databricks/02_daily_transaction.py` with e.g. `start_date = 2024-01-18`, `end_date = 2024-01-20`, `window_size = 7`.
    - `databricks/03_balance_per_customer.py` with the `customer_name` widget.

6.  **Tests:**
    - `pip install pyspark pytest && python -m pytest databricks/tests` runs the analytics logic against a local Spark session — no Databricks cluster required.

See [`databricks/README.md`](databricks/README.md) for the full file-by-file mapping from the legacy implementation.

---

## 🌟 Project Outcomes

This project successfully demonstrates a complete data engineering lifecycle. The final solution transforms a chaotic, multi-source data environment into a clean, reliable, and high-performance lakehouse Data Warehouse on Databricks, ready to power business intelligence and analytics.
