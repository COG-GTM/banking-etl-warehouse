"""Load_DimBranch: SQL Server dbo.branch -> dwh.dim_branch."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F  # noqa: E402

from common import get_spark, qualified, read_jdbc_table, write_delta  # noqa: E402


def main() -> None:
    spark = get_spark()
    src = read_jdbc_table("branch", spark)
    df = src.select(
        F.col("branch_id").alias("BranchID"),
        F.col("branch_name").alias("BranchName"),
        F.col("branch_location").alias("BranchLocation"),
    )
    write_delta(df, qualified("dim_branch", spark), key="BranchID", spark=spark)


if __name__ == "__main__":
    main()
