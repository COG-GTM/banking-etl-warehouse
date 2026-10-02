"""Load_DimCustomer: sample.dbo.customer + city + state -> DWH.dbo.DimCustomer.

Mirrors the Talend tMap: customer is the main flow, city and state are left-outer lookups
(customer.city_id = city.city_id, city.state_id = state.state_id), and name, address and
gender are upper-cased (StringHandling.UPCASE).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.config import EtlConfig, load_config
from etl.jdbc import read_table, write_table


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


def run(spark: SparkSession, cfg: EtlConfig, mode: str | None = None) -> int:
    tables = cfg.source_tables
    df = transform(
        read_table(spark, cfg.source, tables.customer),
        read_table(spark, cfg.source, tables.city),
        read_table(spark, cfg.source, tables.state),
    )
    return write_table(
        spark, df, cfg.target, cfg.target_tables.dim_customer, mode or cfg.write_mode
    )


if __name__ == "__main__":
    from etl.main import run_single

    run_single(run, load_config())
