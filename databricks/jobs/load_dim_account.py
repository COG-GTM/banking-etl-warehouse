# Databricks notebook source
# Load DimAccount
#
# Migrated from Talend job `Load_DimAccount` (tMSSqlInput(account) -> tMap -> tMSSqlOutput).
# Target: Delta table `<catalog>.dwh.dim_account`, merged on AccountID.

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

TARGET = "dim_account"
KEYS = ["AccountID"]

# COMMAND ----------


def transform_dim_account(account: DataFrame) -> DataFrame:
    return account.select(
        F.col("account_id").cast("int").alias("AccountID"),
        F.col("customer_id").cast("int").alias("CustomerID"),
        F.col("account_type").cast("string").alias("AccountType"),
        F.col("balance").cast("decimal(19,4)").alias("Balance"),
        F.col("date_opened").cast("date").alias("DateOpened"),
        F.col("status").cast("string").alias("Status"),
    )


def main(spark: SparkSession, dbutils=None) -> DataFrame:
    config = load_config(spark, dbutils)
    df = transform_dim_account(read_jdbc_table("account", spark, config))
    merge_into_delta(df, config.target_table(TARGET), KEYS)
    return df


# COMMAND ----------

if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
