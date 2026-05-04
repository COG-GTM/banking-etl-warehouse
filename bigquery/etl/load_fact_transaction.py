"""Load FactTransaction — BigQuery-native ETL pipeline.

Replicates the Talend ``Load_FactTransaction`` job which unions
transaction data from three heterogeneous sources, deduplicates on
``transaction_id``, and loads into the ``FactTransaction`` fact table.

Talend flow
-----------
tMSSqlInput (sample.dbo.transaction_db) → row1 ─┐
tFileInputExcel (transaction_excel.xlsx) → row2 ─┤ tUnite → tUniqRow → tMap → tMSSqlOutput
tFileInputDelimited (transaction_csv.csv)→ row3 ─┘                          (DWH.FactTransaction, TRUNCATE)

tUnite:  merges the three streams into a single schema.
tUniqRow: deduplicates on transaction_id (KEY_ATTRIBUTE=true, first occurrence kept).
tMap:     renames columns to PascalCase for the DWH schema.

BigQuery equivalent
-------------------
1. Load CSV and Excel files into temporary staging tables using the
   BigQuery Python client ``load_table_from_dataframe``.
2. UNION ALL the SQL source table with the two file-based staging tables.
3. Deduplicate on ``transaction_id`` keeping the first occurrence
   (deterministic via ROW_NUMBER ordered by source priority).
4. Write the deduplicated result to ``DWH.FactTransaction`` (full refresh / TRUNCATE).
"""

import logging
import sys

import pandas as pd
from google.cloud import bigquery

from config import (
    BQ_DATASET_DWH,
    BQ_DATASET_STAGING,
    GCP_PROJECT_ID,
    TRANSACTION_CSV_PATH,
    TRANSACTION_EXCEL_PATH,
    WRITE_DISPOSITION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("load_fact_transaction")

TRANSACTION_SCHEMA = [
    bigquery.SchemaField("transaction_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("account_id", "INT64"),
    bigquery.SchemaField("transaction_date", "DATETIME"),
    bigquery.SchemaField("amount", "INT64"),
    bigquery.SchemaField("transaction_type", "STRING"),
    bigquery.SchemaField("branch_id", "INT64"),
]


def _load_csv_to_staging(client: bigquery.Client, staging_csv: str) -> None:
    """Load the CSV transaction file into a BigQuery staging table."""
    logger.info("Loading CSV file: %s", TRANSACTION_CSV_PATH)
    df = pd.read_csv(
        TRANSACTION_CSV_PATH,
        parse_dates=["transaction_date"],
        dayfirst=True,
    )
    job_config = bigquery.LoadJobConfig(
        schema=TRANSACTION_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(df, staging_csv, job_config=job_config)
    job.result()
    logger.info("Staged %d CSV rows into %s", len(df), staging_csv)


def _load_excel_to_staging(client: bigquery.Client, staging_excel: str) -> None:
    """Load the Excel transaction file into a BigQuery staging table."""
    logger.info("Loading Excel file: %s", TRANSACTION_EXCEL_PATH)
    df = pd.read_excel(
        TRANSACTION_EXCEL_PATH,
        parse_dates=["transaction_date"],
    )
    job_config = bigquery.LoadJobConfig(
        schema=TRANSACTION_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(df, staging_excel, job_config=job_config)
    job.result()
    logger.info("Staged %d Excel rows into %s", len(df), staging_excel)


def run() -> None:
    """Execute the FactTransaction pipeline."""
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Staging table references for file-based sources
    staging_csv = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_transaction_csv"
    staging_excel = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.stg_transaction_excel"

    # Step 1: Load CSV and Excel into staging tables
    _load_csv_to_staging(client, staging_csv)
    _load_excel_to_staging(client, staging_excel)

    # Step 2: UNION ALL three sources, deduplicate, and rename to PascalCase
    sql_source = f"`{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.transaction_db`"
    csv_source = f"`{staging_csv}`"
    excel_source = f"`{staging_excel}`"
    target_table = f"{GCP_PROJECT_ID}.{BQ_DATASET_DWH}.FactTransaction"

    query = f"""
    WITH unioned AS (
        SELECT *, 1 AS _source_priority
        FROM {sql_source}

        UNION ALL

        SELECT *, 2 AS _source_priority
        FROM {excel_source}

        UNION ALL

        SELECT *, 3 AS _source_priority
        FROM {csv_source}
    ),
    deduplicated AS (
        SELECT
            * EXCEPT(_source_priority),
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY _source_priority
            ) AS _rn
        FROM unioned
    )
    SELECT
        transaction_id    AS TransactionID,
        account_id        AS AccountID,
        transaction_date  AS TransactionDate,
        amount            AS Amount,
        transaction_type  AS TransactionType,
        branch_id         AS BranchID
    FROM deduplicated
    WHERE _rn = 1
    """

    logger.info("Unioning SQL, Excel, and CSV sources; deduplicating on transaction_id")

    job_config = bigquery.QueryJobConfig(
        destination=target_table,
        write_disposition=WRITE_DISPOSITION,
    )

    job = client.query(query, job_config=job_config)
    job.result()

    logger.info("Loaded %d rows into %s", job.num_dml_affected_rows or 0, target_table)


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)
