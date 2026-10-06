# Databricks notebook source
# Load DimBranch
#
# Migrated from Talend job `Load_DimBranch` (tMSSqlInput(branch) -> tMap -> tMSSqlOutput).
# Target: Delta table `<catalog>.dwh.dim_branch`, merged on BranchID.

# COMMAND ----------

import os
import sys

try:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
except NameError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import load_config
from common.readers import read_jdbc_table
from common.writers import merge_into_delta

TARGET = "dim_branch"
KEYS = ["BranchID"]

# COMMAND ----------


def transform_dim_branch(branch: DataFrame) -> DataFrame:
    return branch.select(
        F.col("branch_id").cast("int").alias("BranchID"),
        F.col("branch_name").cast("string").alias("BranchName"),
        F.col("branch_location").cast("string").alias("BranchLocation"),
    )


def main(spark: SparkSession, dbutils=None) -> DataFrame:
    config = load_config(spark, dbutils)
    df = transform_dim_branch(read_jdbc_table("branch", spark, config))
    merge_into_delta(df, config.target_table(TARGET), KEYS)
    return df


# COMMAND ----------

if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
