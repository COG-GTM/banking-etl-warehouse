"""Spark equivalent of ``sp_BalancePerCustomer @customer_name``.

``c.CustomerName LIKE '%' + @customer_name + '%'`` and ``Status = 'active'``
are evaluated case-insensitively, matching SQL Server's default
case-insensitive collation (DimCustomer names are stored uppercased).

Usage:
    spark-submit ... pyspark_jobs/reports/balance_per_customer.py --customer-name Shelly
"""

from __future__ import annotations

import argparse
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark_jobs.common.config import load_config
from pyspark_jobs.common.io import read_jdbc_table
from pyspark_jobs.common.spark_session import get_spark


def balance_per_customer(
    fact: DataFrame, dim_customer: DataFrame, dim_account: DataFrame, customer_name: str
) -> DataFrame:
    summary = fact.groupBy("AccountID").agg(
        F.sum(F.when(F.col("TransactionType") == "Deposit", F.col("Amount")).otherwise(-F.col("Amount"))).alias(
            "TotalTransactionAmount"
        )
    )
    c, a, ts = dim_customer.alias("c"), dim_account.alias("a"), summary.alias("ts")
    return (
        c.join(a, F.col("c.CustomerID") == F.col("a.CustomerID"), "inner")
        .join(ts, F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .where(F.lower(F.col("c.CustomerName")).like(f"%{customer_name.lower()}%"))
        .where(F.lower(F.col("a.Status")) == "active")
        .select(
            F.col("c.CustomerName").alias("CustomerName"),
            F.col("a.AccountType").alias("AccountType"),
            F.col("a.Balance").alias("InitialBalance"),
            (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0))).alias("CurrentBalance"),
        )
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Current balance per active account (sp_BalancePerCustomer)")
    parser.add_argument("--customer-name", required=True)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("BalancePerCustomer")
    fact = read_jdbc_table(spark, config.target, config.target_table("FactTransaction"))
    customer = read_jdbc_table(spark, config.target, config.target_table("DimCustomer"))
    account = read_jdbc_table(spark, config.target, config.target_table("DimAccount"))
    balance_per_customer(fact, customer, account, args.customer_name).show(truncate=False)


if __name__ == "__main__":
    main()
