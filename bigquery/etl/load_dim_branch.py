"""Load DimBranch — BigQuery-native ETL pipeline.

Replicates the Talend ``Load_DimBranch`` job which performs a simple
extract-transform-load from the ``branch`` source table into the
``DimBranch`` dimension table in the Data Warehouse.

Talend flow
-----------
tMSSqlInput (sample.dbo.branch)
  → tMap (rename columns to PascalCase)
    → tMSSqlOutput (DWH.DimBranch, CREATE_IF_NOT_EXISTS)

BigQuery equivalent
-------------------
1. Read all rows from the ``staging.branch`` table.
2. Rename columns to match the DWH schema (PascalCase).
3. Write the result to ``DWH.DimBranch`` (full refresh).
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
logger = logging.getLogger("load_dim_branch")


def run() -> None:
    """Execute the DimBranch pipeline."""
    client = bigquery.Client(project=GCP_PROJECT_ID)

    source_table = f"`{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.branch`"
    target_table = f"{GCP_PROJECT_ID}.{BQ_DATASET_DWH}.DimBranch"

    query = f"""
    SELECT
        branch_id   AS BranchID,
        branch_name AS BranchName,
        branch_location AS BranchLocation
    FROM {source_table}
    """

    logger.info("Extracting from %s", source_table)

    job_config = bigquery.QueryJobConfig(
        destination=target_table,
        write_disposition=WRITE_DISPOSITION,
    )

    job = client.query(query, job_config=job_config)
    job.result()  # block until complete

    logger.info("Loaded %d rows into %s", job.num_dml_affected_rows or 0, target_table)


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)
