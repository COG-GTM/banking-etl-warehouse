"""Shared configuration for BigQuery ETL pipelines.

All pipeline scripts read configuration from environment variables so that
the same code works unchanged across dev, staging, and production
environments.  Sensible defaults are provided for local development.
"""

import os

# GCP / BigQuery settings
GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "my-gcp-project")
BQ_DATASET_DWH: str = os.getenv("BQ_DATASET_DWH", "DWH")
BQ_DATASET_STAGING: str = os.getenv("BQ_DATASET_STAGING", "staging")

# Source data paths (used by Load_FactTransaction for CSV/Excel ingestion)
TRANSACTION_CSV_PATH: str = os.getenv(
    "TRANSACTION_CSV_PATH", "data_sources/transaction_csv.csv"
)
TRANSACTION_EXCEL_PATH: str = os.getenv(
    "TRANSACTION_EXCEL_PATH", "data_sources/transaction_excel.xlsx"
)

# BigQuery write disposition — controls how data is loaded into target tables.
# WRITE_TRUNCATE replaces the table contents on each run (full-refresh pattern).
WRITE_DISPOSITION: str = os.getenv("BQ_WRITE_DISPOSITION", "WRITE_TRUNCATE")
