"""Replaces sp_BalancePerCustomer: current balance of each active account of a customer.

    EXEC sp_BalancePerCustomer @customer_name = 'Shelly';

The procedure filters with `CustomerName LIKE '%' + @customer_name + '%' AND Status =
'active'`. The DWH uses the case-insensitive SQL_Latin1_General_CP1_CI_AS collation (and
DimCustomer names are upper-cased by the ETL), so both comparisons are case-insensitive by
default here too; pass --case-sensitive for a literal `contains` match.
"""

from __future__ import annotations

import argparse

if __package__ in (None, ""):  # allow `spark-submit pyspark_jobs/reports/<report>.py` without --py-files
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_jobs.common.config import EtlConfig
from pyspark_jobs.common.io import read_jdbc
from pyspark_jobs.common.jobs import configure_logging
from pyspark_jobs.common.spark_session import get_spark
from pyspark_jobs.reports import add_output_args, deliver


def _sql_equals(col: str, value: str, case_sensitive: bool):
    """T-SQL ``=``: trailing spaces are ignored, and case too under the DWH's CI collation."""
    trimmed = F.rtrim(F.col(col))
    if case_sensitive:
        return trimmed == value
    return F.lower(trimmed) == value.lower()


def transaction_summary(fact: DataFrame, case_sensitive: bool = False) -> DataFrame:
    is_deposit = _sql_equals("TransactionType", "Deposit", case_sensitive)
    signed = F.when(is_deposit, F.col("Amount")).otherwise(-F.col("Amount"))
    return fact.groupBy("AccountID").agg(F.sum(signed).alias("TotalTransactionAmount"))


def balance_per_customer(
    fact: DataFrame,
    dim_customer: DataFrame,
    dim_account: DataFrame,
    customer_name: str,
    case_sensitive: bool = False,
) -> DataFrame:
    c, a, ts = dim_customer.alias("c"), dim_account.alias("a"), transaction_summary(fact, case_sensitive).alias("ts")
    if case_sensitive:
        name_match = F.col("c.CustomerName").contains(customer_name)
    else:
        name_match = F.upper(F.col("c.CustomerName")).contains(customer_name.upper())
    active = _sql_equals("a.Status", "active", case_sensitive)
    return (
        c.join(a, F.col("c.CustomerID") == F.col("a.CustomerID"))
        .join(ts, F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .where(name_match & active)
        .select(
            F.col("c.CustomerName").alias("CustomerName"),
            F.col("a.AccountType").alias("AccountType"),
            F.col("a.Balance").alias("InitialBalance"),
            (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0))).alias("CurrentBalance"),
        )
    )


def run(spark: SparkSession, cfg: EtlConfig, customer_name: str, case_sensitive: bool = False) -> DataFrame:
    return balance_per_customer(
        read_jdbc(spark, cfg.dwh_db, table="FactTransaction"),
        read_jdbc(spark, cfg.dwh_db, table="DimCustomer"),
        read_jdbc(spark, cfg.dwh_db, table="DimAccount"),
        customer_name,
        case_sensitive,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--customer-name", required=True, help="Substring of DimCustomer.CustomerName")
    parser.add_argument("--case-sensitive", action="store_true", help="Literal contains() instead of CI collation")
    add_output_args(parser)
    args = parser.parse_args(argv)
    configure_logging()
    cfg = EtlConfig.from_env()
    spark = get_spark("balance_per_customer", cfg)
    deliver(run(spark, cfg, args.customer_name, args.case_sensitive), args, cfg)


if __name__ == "__main__":
    main()
