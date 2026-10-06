"""PySpark ports of the SQL Server reporting procedures in sql_scripts/02_create_procedures.sql.

Both functions read the Delta tables written by the load jobs and return a DataFrame with
the same columns as the original procedure's result set.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_SCHEMA = "dwh"
DateLike = str | dt.date


def _spark(spark: SparkSession | None) -> SparkSession:
    return spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _table(name: str, schema: str, catalog: str | None) -> str:
    return ".".join([catalog, schema, name] if catalog else [schema, name])


def daily_transaction(
    start_date: DateLike,
    end_date: DateLike,
    *,
    spark: SparkSession | None = None,
    schema: str = DEFAULT_SCHEMA,
    catalog: str | None = None,
) -> DataFrame:
    """sp_DailyTransaction: per-day transaction count and total amount, both dates inclusive.

    Columns: Date, TotalTransactions, TotalAmount (ordered by Date).
    """
    fact = _spark(spark).table(_table("fact_transaction", schema, catalog))
    tx_date = F.to_date("TransactionDate")
    return (
        fact.where(tx_date.between(F.lit(start_date).cast("date"), F.lit(end_date).cast("date")))
        .groupBy(tx_date.alias("Date"))
        .agg(
            F.count(F.lit(1)).alias("TotalTransactions"),
            F.sum("Amount").alias("TotalAmount"),
        )
        .orderBy("Date")
    )


def balance_per_customer(
    customer_name: str,
    *,
    spark: SparkSession | None = None,
    schema: str = DEFAULT_SCHEMA,
    catalog: str | None = None,
) -> DataFrame:
    """sp_BalancePerCustomer: current balance of each active account for matching customers.

    CurrentBalance = DimAccount.Balance + SUM(Deposit -> +Amount, anything else -> -Amount).
    The name filter is ``LIKE '%name%'``; it is case-insensitive like the SQL Server default
    collation (dim_customer stores names upper-cased).

    Columns: CustomerName, AccountType, InitialBalance, CurrentBalance.
    """
    s = _spark(spark)
    customer = s.table(_table("dim_customer", schema, catalog))
    account = s.table(_table("dim_account", schema, catalog))
    fact = s.table(_table("fact_transaction", schema, catalog))

    transaction_summary = fact.groupBy("AccountID").agg(
        F.sum(F.when(F.col("TransactionType") == "Deposit", F.col("Amount")).otherwise(-F.col("Amount"))).alias(
            "TotalTransactionAmount"
        )
    )

    c, a, ts = customer.alias("c"), account.alias("a"), transaction_summary.alias("ts")
    return (
        c.join(a, F.col("c.CustomerID") == F.col("a.CustomerID"))
        .join(ts, F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .where(F.lower(F.col("c.CustomerName")).contains(customer_name.lower()))
        .where(F.col("a.Status") == "active")
        .select(
            F.col("c.CustomerName"),
            F.col("a.AccountType"),
            F.col("a.Balance").alias("InitialBalance"),
            (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0))).alias("CurrentBalance"),
        )
    )
