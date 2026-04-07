# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Upload Source Data to Unity Catalog Volumes
# MAGIC
# MAGIC This notebook prepares the Databricks environment by:
# MAGIC 1. Creating the Unity Catalog schemas (bronze, silver, gold)
# MAGIC 2. Creating a Volume for raw source files
# MAGIC 3. Documenting where to upload source files
# MAGIC
# MAGIC **Prerequisites:** A Unity Catalog called `banking_dwh` must exist.
# MAGIC Run this notebook once before the first pipeline execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog (if needed)
# MAGIC
# MAGIC Uncomment and run if the catalog does not exist yet.
# MAGIC Requires CREATE CATALOG privilege.

# COMMAND ----------

# Uncomment the following line if the catalog does not exist:
# spark.sql("CREATE CATALOG IF NOT EXISTS banking_dwh")

spark.sql("USE CATALOG banking_dwh")
print("Using catalog: banking_dwh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schemas for Medallion Architecture

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS banking_dwh.bronze COMMENT 'Raw ingested data from source systems'")
spark.sql("CREATE SCHEMA IF NOT EXISTS banking_dwh.silver COMMENT 'Cleansed and transformed dimension/fact tables'")
spark.sql("CREATE SCHEMA IF NOT EXISTS banking_dwh.gold COMMENT 'Business-level aggregated analytical tables'")
spark.sql("CREATE SCHEMA IF NOT EXISTS banking_dwh.raw_data COMMENT 'Schema for source file volumes'")

print("Schemas created: bronze, silver, gold, raw_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volume for Source Files

# COMMAND ----------

spark.sql("""
    CREATE VOLUME IF NOT EXISTS banking_dwh.raw_data.source_files
    COMMENT 'Volume for raw source data files (CSV, Excel) uploaded from legacy systems'
""")

print("Volume created: banking_dwh.raw_data.source_files")
print("Volume path: /Volumes/banking_dwh/raw_data/source_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Upload Source Files
# MAGIC
# MAGIC Upload the following files from the `data_sources/` directory to the Volume.
# MAGIC You can do this via:
# MAGIC - **Databricks UI:** Navigate to Catalog > banking_dwh > raw_data > source_files > Upload
# MAGIC - **Databricks CLI:** `databricks fs cp data_sources/ /Volumes/banking_dwh/raw_data/source_files/ --recursive`
# MAGIC - **dbutils:** See code cell below
# MAGIC
# MAGIC ### Required files:
# MAGIC
# MAGIC | File | Description | Target Path |
# MAGIC |------|-------------|-------------|
# MAGIC | `transaction_csv.csv` | Transaction data (CSV feed) | `/Volumes/banking_dwh/raw_data/source_files/transaction_csv.csv` |
# MAGIC | `transaction_excel.xlsx` | Transaction data (Excel feed) | `/Volumes/banking_dwh/raw_data/source_files/transaction_excel.xlsx` |
# MAGIC | `src_customer.csv` | Customer table export from SQL Server | `/Volumes/banking_dwh/raw_data/source_files/src_customer.csv` |
# MAGIC | `src_city.csv` | City lookup table export | `/Volumes/banking_dwh/raw_data/source_files/src_city.csv` |
# MAGIC | `src_state.csv` | State lookup table export | `/Volumes/banking_dwh/raw_data/source_files/src_state.csv` |
# MAGIC | `src_account.csv` | Account table export | `/Volumes/banking_dwh/raw_data/source_files/src_account.csv` |
# MAGIC | `src_branch.csv` | Branch table export | `/Volumes/banking_dwh/raw_data/source_files/src_branch.csv` |
# MAGIC | `src_transaction.csv` | Transaction table export from SQL Server | `/Volumes/banking_dwh/raw_data/source_files/src_transaction.csv` |
# MAGIC
# MAGIC **Note:** The SQL Server source tables (`src_*.csv`) must be exported from the
# MAGIC `sample.bak` backup first. Restore it in SSMS and export each table as CSV.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Uploaded Files

# COMMAND ----------

volume_path = "/Volumes/banking_dwh/raw_data/source_files"

try:
    files = dbutils.fs.ls(volume_path)
    print(f"Files in {volume_path}:")
    for f in files:
        print(f"  {f.name} ({f.size} bytes)")
except Exception as e:
    print(f"Volume not yet populated or not accessible: {e}")
    print(f"\nPlease upload the source files to: {volume_path}")
    print("See the file list in the markdown cell above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: (Optional) Convert Excel to CSV Fallback
# MAGIC
# MAGIC If the `spark-excel` library is not installed on your cluster,
# MAGIC convert the Excel file to CSV using pandas as a fallback.

# COMMAND ----------

import os

volume_path = "/Volumes/banking_dwh/raw_data/source_files"
excel_path = f"{volume_path}/transaction_excel.xlsx"
csv_fallback_path = f"{volume_path}/transaction_excel_converted.csv"

try:
    # Check if Excel file exists
    dbutils.fs.ls(excel_path)

    # Convert using pandas
    import pandas as pd
    pdf = pd.read_excel(excel_path.replace("/Volumes", "/dbfs/Volumes"))
    pdf.to_csv(csv_fallback_path.replace("/Volumes", "/dbfs/Volumes"), index=False)
    print(f"Excel converted to CSV: {csv_fallback_path}")
except Exception as e:
    print(f"Excel conversion skipped (file may not be uploaded yet): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC The environment is now ready for the ETL pipeline. Run the workflow
# MAGIC defined in `workflow.json` or execute notebooks individually in order:
# MAGIC 1. Bronze notebooks (parallel)
# MAGIC 2. Silver notebooks (after Bronze completes)
# MAGIC 3. Gold notebooks (after Silver completes)
