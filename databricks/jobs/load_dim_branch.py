"""Load_DimBranch: sample.dbo.branch -> dwh.dim_branch."""

try:
    import _bootstrap  # noqa: F401  (spark_python_task: puts databricks/ on sys.path)
except ImportError:
    pass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import JobConfig, source_jdbc
from common.io import read_jdbc, write_table
from common.runner import main

TARGET = "dim_branch"


def transform(branch: DataFrame) -> DataFrame:
    return branch.select(
        F.col("branch_id").cast("int").alias("BranchID"),
        F.col("branch_name").alias("BranchName"),
        F.col("branch_location").alias("BranchLocation"),
    )


def run(spark: SparkSession, cfg: JobConfig) -> int:
    branch = read_jdbc(spark, source_jdbc(spark, cfg), "dbo.branch")
    return write_table(transform(branch), cfg, TARGET)


if __name__ == "__main__":
    main(run)
