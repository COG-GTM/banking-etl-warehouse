"""Load_DimAccount: dbo.account (SQL Server) -> dwh.dim_account (Delta)."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.config import EtlConfig
from banking_etl.readers import read_sqlserver_table
from banking_etl.runtime import base_parser, bootstrap, parse
from banking_etl.transforms import transform_dim_account
from banking_etl.writers import write_table

log = logging.getLogger(__name__)


def run(spark: SparkSession, config: EtlConfig) -> int:
    account = read_sqlserver_table(spark, config.sqlserver, "account", schemas.SRC_ACCOUNT)
    return write_table(
        spark,
        transform_dim_account(account),
        config.warehouse,
        schemas.DIM_ACCOUNT,
        config.table_settings(schemas.DIM_ACCOUNT.name).write_mode,
    )


def main(argv: Sequence[str] | None = None) -> None:
    spark, config = bootstrap("load_dim_account", parse(base_parser(__doc__), argv))
    log.info("load_dim_account finished: %d rows", run(spark, config))


if __name__ == "__main__":
    main()
