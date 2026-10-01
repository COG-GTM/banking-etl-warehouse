# End-to-End Data Warehouse and ETL Pipeline for Banking Analytics

## Project Overview

This project is a comprehensive, end-to-end simulation of a real-world data engineering task developed during my project-based internship with **ID/X Partners** and **Rakamin Academy**. The primary objective was to address a common business challenge for a banking client: inefficient and delayed reporting due to operational data being scattered across multiple, disparate systems.

This repository contains the complete solution, which transforms raw data from various sources into a centralized, analytics-ready Data Warehouse, complete with automated ETL pipelines and pre-built analytical queries.

---

## 🏛️ Solution Architecture

The warehouse is built by PySpark jobs that write Delta Lake tables. The same Python package runs as a
Databricks Workflow (Unity Catalog tables) or as Spark applications on AWS EKS (Delta tables on S3).

**The data flows through four key stages:**
1.  **Data Sources:** Raw data is ingested from three different types of systems:
    - Relational Database (SQL Server, read over JDBC)
    - Excel Files (`.xlsx`)
    - CSV Files (`.csv`, timestamps in `dd-MM-yyyy HH:mm:ss`)
2.  **ETL Processing (PySpark):** Four jobs under [`pyspark/`](pyspark/) replace the Talend jobs:
    - **Extraction:** JDBC, Excel and explicit-schema CSV readers; configuration via `config.yaml` + environment
      variables, credentials from AWS Secrets Manager.
    - **Transformation:** joins and upper-case cleansing (`tMap`), union of the three transaction streams (`tUnite`),
      de-duplication on `transaction_id` (`tUniqRow`), type casting and foreign-key checks.
    - **Loading:** Delta `MERGE` for the dimensions, Delta overwrite (truncate + insert) for the fact table.
3.  **Data Warehouse (Delta Lake):** Star schema in the `dwh` schema/database, matching the original T-SQL DDL
    (`MONEY` → `DECIMAL(19,4)`, `DATETIME` → `TIMESTAMP`):
    - **3 Dimension Tables:** `dwh.dim_customer`, `dwh.dim_account`, `dwh.dim_branch`
    - **1 Fact Table:** `dwh.fact_transaction`
4.  **Data Access & Analytics:** `daily_transaction()` and `balance_per_customer()` re-implement the stored
    procedures as PySpark functions, runnable as jobs or from a Databricks notebook.
5.  **Orchestration:** Databricks Workflow (`databricks.yml`) or Argo Workflows + Spark Operator on EKS
    ([`deploy/`](deploy/)), running `DimBranch`, `DimAccount`, `DimCustomer` in parallel, then `FactTransaction`.

---

## 🛠️ Tech Stack

*   **Processing:** Apache Spark 3.5 / PySpark (Python 3.10+)
*   **Storage:** Delta Lake — Unity Catalog managed tables on Databricks, or Delta on Amazon S3 for EKS
*   **Platforms:** Databricks (Asset Bundle + Workflows) and AWS EKS (Spark Operator, Argo Workflows, IRSA)
*   **Packaging:** Python wheel (`pyspark/pyproject.toml`) and a `linux/amd64` Spark container image (`pyspark/Dockerfile`)
*   **Infrastructure:** Terraform stubs for S3, ECR, IAM (IRSA, Unity Catalog), Secrets Manager, Spark Operator
*   **Sources:** Microsoft SQL Server (JDBC), Excel, CSV
*   **Data Modeling:** Star Schema
*   **Testing / linting:** pytest (unit + SQL Server parity tests), ruff
*   **Legacy (reference only):** Talend Open Studio jobs (`talend_jobs/`) and T-SQL scripts (`sql_scripts/`)

---

## ✨ Key Features & Implementation Details

### 1. Data Warehouse Design (Star Schema)
The DWH was designed from scratch with a focus on analytical performance and clarity.

- **`dim_customer`**: A consolidated view of customer information, enriched with city and state data from separate tables.
- **`dim_account` & `dim_branch`**: Dimension tables providing descriptive context for accounts and bank branches.
- **`fact_transaction`**: The core table containing all unique transaction records from the three source systems.
  Primary keys drive the Delta merges; the legacy foreign keys are enforced by the fact job
  (`FACT_ORPHAN_POLICY=reject|fail|keep`).

### 2. Modular PySpark ETL Jobs
Each Talend job has a PySpark counterpart in `pyspark/src/banking_etl/jobs/`:

| Talend job (legacy)     | PySpark job                | Logic |
|-------------------------|----------------------------|-------|
| `Load_DimBranch`        | `load_dim_branch.py`       | `dbo.branch` → `dwh.dim_branch` |
| `Load_DimAccount`       | `load_dim_account.py`      | `dbo.account` → `dwh.dim_account` |
| `Load_DimCustomer`      | `load_dim_customer.py`     | `customer ⟕ city ⟕ state`, upper-cased name/address/gender |
| `Load_FactTransaction`  | `load_fact_transaction.py` | SQL Server ∪ Excel ∪ CSV, de-duplicated on `transaction_id`, typed |

### 3. Business Reports
- **`daily_transaction(start_date, end_date)`** (was `sp_DailyTransaction`): daily transaction count and total
  amount for an inclusive date range.
- **`balance_per_customer(customer_name)`** (was `sp_BalancePerCustomer`): current balance of each active account
  of matching customers — initial balance plus `SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)`.

The integration tests run the original stored procedures on SQL Server against the same data and assert
identical results.

---

## 🚀 How to Run This Project

### Locally
See [`pyspark/README.md`](pyspark/README.md): restore `data_sources/sample.bak` into a SQL Server container, then

```bash
cd pyspark
pip install -r requirements-dev.txt -e .
pytest                                    # unit tests
export SOURCE_DB_USER=sa SOURCE_DB_PASSWORD=...   # local only; use SOURCE_DB_SECRET_ID elsewhere
load_dim_branch && load_dim_account && load_dim_customer && load_fact_transaction
daily_transaction --start-date 2024-01-18 --end-date 2024-01-20
balance_per_customer --customer-name shelly
```

### On Databricks
1. Store the SQL Server credentials in AWS Secrets Manager and upload the Excel/CSV extracts to a
   Unity Catalog volume (default `/Volumes/banking/landing/transactions`).
2. Adjust the variables in `databricks.yml` (catalog, landing path, JDBC URL, secret id, instance profile).
3. Deploy and run the workflow:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run -t dev banking_etl_warehouse
   ```

### On AWS EKS
1. Provision the AWS resources (`deploy/terraform`) — see [`deploy/README.md`](deploy/README.md).
2. Build and push the image:
   `docker buildx build --platform linux/amd64 -f pyspark/Dockerfile -t <ecr>/banking-etl:<tag> --push .`
3. Apply the manifests and start the workflow:
   ```bash
   kubectl apply -k deploy/k8s/base
   kubectl apply -k deploy/argo
   argo submit -n banking-etl --from workflowtemplate/banking-etl --watch
   ```

### Legacy SQL Server / Talend (reference only)
The original implementation is kept for reference and is no longer the supported run path:
`sql_scripts/01_create_tables.sql` (SQL Server DDL), `sql_scripts/02_create_procedures.sql` (stored procedures) and
`talend_jobs/*.zip` (Talend Open Studio exports, run manually in the order `Load_DimBranch`, `Load_DimAccount`,
`Load_DimCustomer`, `Load_FactTransaction`).

---

## 🌟 Project Outcomes

This project successfully demonstrates a complete data engineering lifecycle. The final solution transforms a chaotic, multi-source data environment into a clean, reliable, and high-performance Data Warehouse, ready to power business intelligence and analytics.
