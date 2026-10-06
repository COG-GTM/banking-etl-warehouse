# Databricks notebook source
# Load DimCustomer
#
# Migrated from Talend job `Load_DimCustomer` (tMap: customer LEFT JOIN city ON city_id LEFT JOIN state ON state_id, UPCASE cleansing).
# Target: Delta table `<catalog>.dwh.dim_customer`, merged on CustomerID.

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

TARGET = "dim_customer"
KEYS = ["CustomerID"]

# COMMAND ----------


def transform_dim_customer(customer: DataFrame, city: DataFrame, state: DataFrame) -> DataFrame:
    """Replicates the Talend tMap.

    Lookups are UNIQUE_MATCH left outer joins (tMap default), so customers without a
    matching city/state are kept with null CityName/StateName. CustomerName, Address and
    Gender are upper-cased (StringHandling.UPCASE) exactly as in the Talend job.
    """
    c = customer.alias("c")
    ci = city.dropDuplicates(["city_id"]).alias("ci")
    s = state.dropDuplicates(["state_id"]).alias("s")
    joined = c.join(ci, F.col("c.city_id") == F.col("ci.city_id"), "left").join(
        s, F.col("ci.state_id") == F.col("s.state_id"), "left"
    )
    return joined.select(
        F.col("c.customer_id").cast("int").alias("CustomerID"),
        F.upper(F.col("c.customer_name")).alias("CustomerName"),
        F.upper(F.col("c.address")).alias("Address"),
        F.col("ci.city_name").cast("string").alias("CityName"),
        F.col("s.state_name").cast("string").alias("StateName"),
        F.expr("try_cast(c.age AS INT)").alias("Age"),
        F.upper(F.col("c.gender")).alias("Gender"),
        F.col("c.email").cast("string").alias("Email"),
    )


def main(spark: SparkSession, dbutils=None) -> DataFrame:
    config = load_config(spark, dbutils)
    df = transform_dim_customer(
        read_jdbc_table("customer", spark, config),
        read_jdbc_table("city", spark, config),
        read_jdbc_table("state", spark, config),
    )
    merge_into_delta(df, config.target_table(TARGET), KEYS)
    return df


# COMMAND ----------

if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
