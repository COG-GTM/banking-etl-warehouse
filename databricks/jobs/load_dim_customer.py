"""Load_DimCustomer: sample.dbo.customer + city + state -> dwh.dim_customer.

Mirrors the Talend tMap: customer is the main flow, city and state are left lookups
(customer.city_id = city.city_id, city.state_id = state.state_id), and name, address and
gender are upper-cased.
"""

try:
    import _bootstrap  # noqa: F401  (spark_python_task: puts databricks/ on sys.path)
except ImportError:
    pass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import JobConfig, source_jdbc
from common.io import read_jdbc, write_table
from common.runner import main

TARGET = "dim_customer"


def transform(customer: DataFrame, city: DataFrame, state: DataFrame) -> DataFrame:
    c = customer.alias("c")
    ci = city.select("city_id", "city_name", "state_id").alias("ci")
    s = state.select("state_id", "state_name").alias("s")
    return (
        c.join(ci, F.col("c.city_id") == F.col("ci.city_id"), "left")
        .join(s, F.col("ci.state_id") == F.col("s.state_id"), "left")
        .select(
            F.col("c.customer_id").cast("int").alias("CustomerID"),
            F.upper("c.customer_name").alias("CustomerName"),
            F.upper("c.address").alias("Address"),
            F.col("ci.city_name").alias("CityName"),
            F.col("s.state_name").alias("StateName"),
            F.col("c.age").cast("int").alias("Age"),
            F.upper("c.gender").alias("Gender"),
            F.col("c.email").alias("Email"),
        )
    )


def run(spark: SparkSession, cfg: JobConfig) -> int:
    db = source_jdbc(spark, cfg)
    df = transform(
        read_jdbc(spark, db, "dbo.customer"),
        read_jdbc(spark, db, "dbo.city"),
        read_jdbc(spark, db, "dbo.state"),
    )
    return write_table(df, cfg, TARGET)


if __name__ == "__main__":
    main(run)
