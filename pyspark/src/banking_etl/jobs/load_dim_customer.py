"""Load_DimCustomer: dbo.customer + dbo.city + dbo.state (SQL Server) -> dwh.dim_customer (Delta)."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.config import EtlConfig
from banking_etl.readers import read_sqlserver_table
from banking_etl.runtime import base_parser, bootstrap, parse
from banking_etl.transforms import transform_dim_customer
from banking_etl.writers import write_table

log = logging.getLogger(__name__)


def run(spark: SparkSession, config: EtlConfig) -> int:
    customer = read_sqlserver_table(spark, config.sqlserver, "customer", schemas.SRC_CUSTOMER)
    city = read_sqlserver_table(spark, config.sqlserver, "city", schemas.SRC_CITY)
    state = read_sqlserver_table(spark, config.sqlserver, "state", schemas.SRC_STATE)
    return write_table(
        spark,
        transform_dim_customer(customer, city, state),
        config.warehouse,
        schemas.DIM_CUSTOMER,
        config.table_settings(schemas.DIM_CUSTOMER.name).write_mode,
    )


def main(argv: Sequence[str] | None = None) -> None:
    spark, config = bootstrap("load_dim_customer", parse(base_parser(__doc__), argv))
    log.info("load_dim_customer finished: %d rows", run(spark, config))


if __name__ == "__main__":
    main()
