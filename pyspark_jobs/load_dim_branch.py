"""Load_DimBranch: sample.dbo.branch -> DWH.dbo.DimBranch."""

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

TARGET_TABLE = "DimBranch"


def transform(branch: DataFrame) -> DataFrame:
    return branch.select(
        F.col("branch_id").cast("int").alias("BranchID"),
        F.col("branch_name").alias("BranchName"),
        F.col("branch_location").alias("BranchLocation"),
    )


def run(spark: SparkSession, config: EtlConfig) -> int:
    branch = read_jdbc_table(spark, config.source, config.source_table("branch"))
    out = transform(branch)
    write_jdbc(out, spark, config.target, config.target_table(TARGET_TABLE), config.dim_jdbc_mode)
    rows = out.count()
    log.info("Load_DimBranch: wrote %s rows to %s", rows, TARGET_TABLE)
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(get_spark("Load_DimBranch"), load_config())


if __name__ == "__main__":
    main()
