"""Load_DimAccount: sample.dbo.account -> dwh.dim_account."""

try:
    import _bootstrap  # noqa: F401  (spark_python_task: puts databricks/ on sys.path)
except ImportError:
    pass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import JobConfig, source_jdbc
from common.io import read_jdbc, write_table
from common.runner import main

TARGET = "dim_account"


def transform(account: DataFrame) -> DataFrame:
    return account.select(
        F.col("account_id").cast("int").alias("AccountID"),
        F.col("customer_id").cast("int").alias("CustomerID"),
        F.col("account_type").alias("AccountType"),
        F.col("balance").cast("decimal(19,4)").alias("Balance"),
        F.to_date("date_opened").alias("DateOpened"),
        F.col("status").alias("Status"),
    )


def run(spark: SparkSession, cfg: JobConfig) -> int:
    account = read_jdbc(spark, source_jdbc(spark, cfg), "dbo.account")
    return write_table(transform(account), cfg, TARGET)


if __name__ == "__main__":
    main(run)
