"""Load_DimAccount: sample.dbo.account -> DWH.dbo.DimAccount."""

from __future__ import annotations

import logging
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_jobs.common.config import EtlConfig, load_config
from pyspark_jobs.common.io import read_jdbc_table, write_jdbc
from pyspark_jobs.common.spark_session import get_spark

log = logging.getLogger(__name__)

TARGET_TABLE = "DimAccount"


def transform(account: DataFrame) -> DataFrame:
    return account.select(
        F.col("account_id").cast("int").alias("AccountID"),
        F.col("customer_id").cast("int").alias("CustomerID"),
        F.col("account_type").alias("AccountType"),
        F.col("balance").cast("decimal(19,4)").alias("Balance"),
        F.to_date("date_opened").alias("DateOpened"),
        F.col("status").alias("Status"),
    )


def run(spark: SparkSession, config: EtlConfig) -> int:
    account = read_jdbc_table(spark, config.source, config.source_table("account"))
    out = transform(account)
    write_jdbc(out, spark, config.target, config.target_table(TARGET_TABLE), config.dim_jdbc_mode)
    rows = out.count()
    log.info("Load_DimAccount: wrote %s rows to %s", rows, TARGET_TABLE)
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(get_spark("Load_DimAccount"), load_config())


if __name__ == "__main__":
    main()
