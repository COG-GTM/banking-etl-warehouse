"""Load_DimCustomer: sample.dbo.customer + city + state -> DWH.dbo.DimCustomer.

Mirrors the Talend tMap: customer is the main flow, city (on city_id) and state (on
state_id) are LEFT OUTER lookups, and customer_name/address/gender are upper-cased
(StringHandling.UPCASE).
"""

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

TARGET_TABLE = "DimCustomer"
UPPERCASED_COLUMNS = ("customer_name", "address", "gender")


def extract(spark: SparkSession, cfg: EtlConfig) -> tuple[DataFrame, DataFrame, DataFrame]:
    return (
        read_jdbc(spark, cfg.source_db, table="customer"),
        read_jdbc(spark, cfg.source_db, table="city"),
        read_jdbc(spark, cfg.source_db, table="state"),
    )


def transform(customer: DataFrame, city: DataFrame, state: DataFrame) -> DataFrame:
    for c in UPPERCASED_COLUMNS:
        customer = customer.withColumn(c, F.upper(F.col(c)))
    joined = (
        customer.alias("cu")
        .join(city.alias("ci"), F.col("cu.city_id") == F.col("ci.city_id"), "left")
        .join(state.alias("st"), F.col("ci.state_id") == F.col("st.state_id"), "left")
    )
    return joined.select(
        F.col("cu.customer_id").cast("int").alias("CustomerID"),
        F.col("cu.customer_name").alias("CustomerName"),
        F.col("cu.address").alias("Address"),
        F.col("ci.city_name").alias("CityName"),
        F.col("st.state_name").alias("StateName"),
        F.trim(F.col("cu.age")).cast("int").alias("Age"),
        F.col("cu.gender").alias("Gender"),
        F.col("cu.email").alias("Email"),
    )


def run(spark: SparkSession, cfg: EtlConfig, mode: str = "append") -> int:
    return load_table(spark, cfg, transform(*extract(spark, cfg)), TARGET_TABLE, "CustomerID", mode)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["append", "overwrite"], default="append")
    args = parser.parse_args(argv)
    configure_logging()
    cfg = EtlConfig.from_env()
    run(get_spark("load_dim_customer", cfg), cfg, args.mode)


if __name__ == "__main__":
    main()
