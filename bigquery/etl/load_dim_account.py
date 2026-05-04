"""Load DimAccount — BigQuery-native ETL pipeline.

Replicates the Talend ``Load_DimAccount`` job which performs a simple
extract-transform-load from the ``account`` source table into the
``DimAccount`` dimension table in the Data Warehouse.

Talend flow
-----------
tMSSqlInput (sample.dbo.account)
  → tMap (rename columns to PascalCase)
    → tMSSqlOutput (DWH.DimAccount, CREATE_IF_NOT_EXISTS)

BigQuery equivalent
-------------------
1. Read all rows from the ``staging.account`` table.
2. Rename columns to match the DWH schema (PascalCase).
3. Write the result to ``DWH.DimAccount`` (full refresh).
"""

import logging
import sys

from google.cloud import bigquery

from config import (
    BQ_DATASET_DWH,
    BQ_DATASET_STAGING,
    GCP_PROJECT_ID,
    WRITE_DISPOSITION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("load_dim_account")


def run() -> None:
    """Execute the DimAccount pipeline."""
    client = bigquery.Client(project=GCP_PROJECT_ID)

    source_table = f"`{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.account`"
    target_table = f"{GCP_PROJECT_ID}.{BQ_DATASET_DWH}.DimAccount"

    query = f"""
    SELECT
        account_id    AS AccountID,
        customer_id   AS CustomerID,
        account_type  AS AccountType,
        balance       AS Balance,
        date_opened   AS DateOpened,
        status        AS Status
    FROM {source_table}
    """

    logger.info("Extracting from %s", source_table)

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
