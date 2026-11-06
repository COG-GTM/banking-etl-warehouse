"""Load_DimCustomer: sample.dbo.customer + city + state -> DWH.dbo.DimCustomer.

Mirrors the Talend tMap: customer is the main flow, city and state are
lookups joined on city_id / state_id (left joins, so customers with an
unknown city keep a NULL CityName/StateName), and text fields are
uppercased. The uppercased columns are configurable through
``DIM_CUSTOMER_UPPERCASE_COLUMNS``.
"""

from __future__ import annotations

import logging
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_jobs.common.config import EtlConfig, load_config
from pyspark_jobs.common.io import read_jdbc_table, write_jdbc
from pyspark_jobs.common.spark_session import get_spark

log = logging.getLogger(__name__)

TARGET_TABLE = "DimCustomer"
TARGET_COLUMNS = (
    "CustomerID",
    "CustomerName",
    "Address",
    "CityName",
    "StateName",
    "Age",
    "Gender",
    "Email",
)


def transform(
    customer: DataFrame,
    city: DataFrame,
    state: DataFrame,
    uppercase_columns: Iterable[str] = ("CustomerName", "Address", "CityName", "StateName", "Gender"),
) -> DataFrame:
    c, ci, s = customer.alias("c"), city.alias("ci"), state.alias("s")
    joined = c.join(ci, F.col("c.city_id") == F.col("ci.city_id"), "left").join(
        s, F.col("ci.state_id") == F.col("s.state_id"), "left"
    )
    out = joined.select(
        F.col("c.customer_id").cast("int").alias("CustomerID"),
        F.col("c.customer_name").alias("CustomerName"),
        F.col("c.address").alias("Address"),
        F.col("ci.city_name").alias("CityName"),
        F.col("s.state_name").alias("StateName"),
        F.col("c.age").cast("int").alias("Age"),
        F.col("c.gender").alias("Gender"),
        F.col("c.email").alias("Email"),
    )
    unknown = set(uppercase_columns) - set(TARGET_COLUMNS)
    if unknown:
        raise ValueError(f"Unknown DimCustomer columns to uppercase: {sorted(unknown)}")
    for column in uppercase_columns:
        out = out.withColumn(column, F.upper(F.col(column)))
    return out.select(*TARGET_COLUMNS)


def run(spark: SparkSession, config: EtlConfig) -> int:
    customer = read_jdbc_table(spark, config.source, config.source_table("customer"))
    city = read_jdbc_table(spark, config.source, config.source_table("city"))
    state = read_jdbc_table(spark, config.source, config.source_table("state"))
    out = transform(customer, city, state, config.dim_customer_uppercase_columns)
    write_jdbc(out, spark, config.target, config.target_table(TARGET_TABLE), config.dim_jdbc_mode)
    rows = out.count()
    log.info("Load_DimCustomer: wrote %s rows to %s", rows, TARGET_TABLE)
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(get_spark("Load_DimCustomer"), load_config())


if __name__ == "__main__":
    main()
