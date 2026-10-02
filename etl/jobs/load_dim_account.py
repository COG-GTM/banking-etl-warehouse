"""Load_DimAccount: sample.dbo.account -> DWH.dbo.DimAccount."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.config import EtlConfig, load_config
from etl.jdbc import read_table, write_table


def transform(account: DataFrame) -> DataFrame:
    return account.select(
        F.col("account_id").cast("int").alias("AccountID"),
        F.col("customer_id").cast("int").alias("CustomerID"),
        F.col("account_type").alias("AccountType"),
        F.col("balance").cast("decimal(19,4)").alias("Balance"),
        F.to_date("date_opened").alias("DateOpened"),
        F.col("status").alias("Status"),
    )


def run(spark: SparkSession, cfg: EtlConfig, mode: str | None = None) -> int:
    account = read_table(spark, cfg.source, cfg.source_tables.account)
    return write_table(
        spark, transform(account), cfg.target, cfg.target_tables.dim_account, mode or cfg.write_mode
    )


if __name__ == "__main__":
    from etl.main import run_single

    run_single(run, load_config())
