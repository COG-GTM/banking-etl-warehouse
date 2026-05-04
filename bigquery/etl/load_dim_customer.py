"""Load DimCustomer — BigQuery-native ETL pipeline.

Replicates the Talend ``Load_DimCustomer`` job which joins three source
tables (customer, city, state) and applies uppercase cleansing before
loading into the ``DimCustomer`` dimension table.

Talend flow
-----------
tMSSqlInput (sample.dbo.customer)  → row1 ─┐
tMSSqlInput (sample.dbo.city)      → row2 ─┤  tMap  → tMSSqlOutput (DWH.DimCustomer)
tMSSqlInput (sample.dbo.state)     → row3 ─┘

tMap join logic:
  row2.city_id  = row1.city_id   (lookup, UNIQUE_MATCH)
  row3.state_id = row2.state_id  (lookup, UNIQUE_MATCH)

tMap output expressions:
  CustomerID   = row1.customer_id
  CustomerName = UPCASE(row1.customer_name)
  Address      = UPCASE(row1.address)
  Age          = row1.age
  Gender       = UPCASE(row1.gender)
  Email        = row1.email
  CityName     = row2.city_name
  StateName    = row3.state_name

BigQuery equivalent
-------------------
1. JOIN staging.customer → staging.city → staging.state.
2. Apply UPPER() to customer_name, address, and gender.
3. Write the result to DWH.DimCustomer (full refresh).
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
logger = logging.getLogger("load_dim_customer")


def run() -> None:
    """Execute the DimCustomer pipeline."""
    client = bigquery.Client(project=GCP_PROJECT_ID)

    customer_tbl = f"`{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.customer`"
    city_tbl = f"`{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.city`"
    state_tbl = f"`{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.state`"
    target_table = f"{GCP_PROJECT_ID}.{BQ_DATASET_DWH}.DimCustomer"

    query = f"""
    SELECT
        c.customer_id           AS CustomerID,
        UPPER(c.customer_name)  AS CustomerName,
        UPPER(c.address)        AS Address,
        ci.city_name            AS CityName,
        s.state_name            AS StateName,
        c.age                   AS Age,
        UPPER(c.gender)         AS Gender,
        c.email                 AS Email
    FROM {customer_tbl} AS c
    LEFT JOIN {city_tbl}  AS ci ON ci.city_id  = c.city_id
    LEFT JOIN {state_tbl} AS s  ON s.state_id  = ci.state_id
    """

    logger.info("Joining customer, city, and state from %s", BQ_DATASET_STAGING)

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
