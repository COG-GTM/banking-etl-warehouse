"""Load_DimAccount: SQL Server dbo.account -> dwh.dim_account."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F  # noqa: E402

from common import get_spark, qualified, read_jdbc_table, write_delta  # noqa: E402


def main() -> None:
    spark = get_spark()
    src = read_jdbc_table("account", spark)
    df = src.select(
        F.col("account_id").alias("AccountID"),
        F.col("customer_id").alias("CustomerID"),
        F.col("account_type").alias("AccountType"),
        F.col("balance").alias("Balance"),
        F.col("date_opened").alias("DateOpened"),
        F.col("status").alias("Status"),
    )
    write_delta(df, qualified("dim_account", spark), key="AccountID", spark=spark)


if __name__ == "__main__":
    main()
