"""Spark implementations of the DWH analytics previously written as T-SQL procs.

`daily_transaction` replaces `sp_DailyTransaction` (plus the new moving-average
smoothing) and `balance_per_customer` replaces `sp_BalancePerCustomer`.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "dwh"
DEFAULT_WINDOW_SIZE = 7


def daily_transaction(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    window_size: int = DEFAULT_WINDOW_SIZE,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> DataFrame:
    """Daily transaction volume/amount with a trailing moving average.

    The moving average spans ``window_size`` rows (``window_size - 1`` preceding
    rows plus the current row) ordered by date, i.e. a trailing N-day mean over
    the days that actually have transactions.
    """
    if window_size < 1:
        raise ValueError("window_size must be >= 1")

    fact = spark.table(f"{catalog}.{schema}.FactTransaction")

    daily = (
        fact.withColumn("Date", F.to_date("TransactionDate"))
        .filter(F.col("Date").between(F.lit(start_date), F.lit(end_date)))
        .groupBy("Date")
        .agg(
            F.count("TransactionID").alias("TotalTransactions"),
            F.sum("Amount").alias("TotalAmount"),
        )
    )

    smoothing = Window.orderBy("Date").rowsBetween(-(window_size - 1), 0)

    return daily.withColumn(
        "SmoothedTotalAmount",
        F.avg("TotalAmount").over(smoothing).cast("decimal(19,4)"),
    ).orderBy("Date")


def balance_per_customer(
    spark: SparkSession,
    customer_name: str,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> DataFrame:
    """Initial vs. current balance per active account for matching customers.

    Deposits increase the balance, every other transaction type decreases it.
    """
    fact = spark.table(f"{catalog}.{schema}.FactTransaction")
    accounts = spark.table(f"{catalog}.{schema}.DimAccount")
    customers = spark.table(f"{catalog}.{schema}.DimCustomer")

    transaction_summary = fact.groupBy("AccountID").agg(
        F.sum(
            F.when(F.col("TransactionType") == "Deposit", F.col("Amount")).otherwise(
                -F.col("Amount")
            )
        ).alias("TotalTransactionAmount")
    )

    return (
        customers.alias("c")
        .join(accounts.alias("a"), F.col("c.CustomerID") == F.col("a.CustomerID"))
        .join(
            transaction_summary.alias("ts"),
            F.col("a.AccountID") == F.col("ts.AccountID"),
            "left",
        )
        .filter(F.col("c.CustomerName").contains(customer_name))
        .filter(F.col("a.Status") == "active")
        .select(
            F.col("c.CustomerName"),
            F.col("a.AccountType"),
            F.col("a.Balance").alias("InitialBalance"),
            (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0)))
            .cast("decimal(19,4)")
            .alias("CurrentBalance"),
        )
    )
