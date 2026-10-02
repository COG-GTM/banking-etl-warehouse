"""Load_DimBranch: sample.dbo.branch -> DWH.dbo.DimBranch."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.config import EtlConfig, load_config
from etl.jdbc import read_table, write_table


def transform(branch: DataFrame) -> DataFrame:
    return branch.select(
        F.col("branch_id").cast("int").alias("BranchID"),
        F.col("branch_name").alias("BranchName"),
        F.col("branch_location").alias("BranchLocation"),
    )


def run(spark: SparkSession, cfg: EtlConfig, mode: str | None = None) -> int:
    branch = read_table(spark, cfg.source, cfg.source_tables.branch)
    return write_table(
        spark, transform(branch), cfg.target, cfg.target_tables.dim_branch, mode or cfg.write_mode
    )


if __name__ == "__main__":
    from etl.main import run_single

    run_single(run, load_config())
