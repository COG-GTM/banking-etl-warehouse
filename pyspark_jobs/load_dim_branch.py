"""Load_DimBranch: sample.dbo.branch -> DWH.dbo.DimBranch."""

from __future__ import annotations

import argparse

if __package__ in (None, ""):  # allow `spark-submit pyspark_jobs/<job>.py` without --py-files
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_jobs.common.config import EtlConfig
from pyspark_jobs.common.io import read_jdbc
from pyspark_jobs.common.jobs import configure_logging, load_table
from pyspark_jobs.common.spark_session import get_spark

TARGET_TABLE = "DimBranch"


def extract(spark: SparkSession, cfg: EtlConfig) -> DataFrame:
    return read_jdbc(spark, cfg.source_db, table="branch")


def transform(branch: DataFrame) -> DataFrame:
    return branch.select(
        F.col("branch_id").cast("int").alias("BranchID"),
        F.col("branch_name").alias("BranchName"),
        F.col("branch_location").alias("BranchLocation"),
    )


def run(spark: SparkSession, cfg: EtlConfig, mode: str = "append") -> int:
    return load_table(spark, cfg, transform(extract(spark, cfg)), TARGET_TABLE, "BranchID", mode)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["append", "overwrite"], default="append")
    args = parser.parse_args(argv)
    configure_logging()
    cfg = EtlConfig.from_env()
    run(get_spark("load_dim_branch", cfg), cfg, args.mode)


if __name__ == "__main__":
    main()
