"""PySpark port of `sql_scripts/02_create_procedures.sql`.

`sp_DailyTransaction`  -> `daily_transaction()`
`sp_BalancePerCustomer`-> `balance_per_customer()`

Both are pure functions over DataFrames. The Unity Catalog SQL equivalents live in
`databricks/gold/sql/analytics_functions.sql`; `GoldAnalytics` is the thin Python
API with the same arguments as the stored procedures.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType

from .config import GoldConfig

DateLike = str | dt.date


def _as_date_literal(value: DateLike):
    if isinstance(value, dt.date):
        return F.lit(value).cast(DateType())
    return F.to_date(F.lit(value))


def daily_transaction(fact: DataFrame, start_date: DateLike, end_date: DateLike) -> DataFrame:
    """`sp_DailyTransaction @start_date, @end_date`.

    Returns one row per calendar day in the inclusive range with the transaction
    count and total amount, ordered by date.
    """
    transaction_date = F.to_date(F.col("transaction_date")).alias("date")
    return (
        fact.select(transaction_date, "transaction_id", "amount")
        .filter(F.col("date").between(_as_date_literal(start_date), _as_date_literal(end_date)))
        .groupBy("date")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").cast(DecimalType(19, 4)).alias("total_amount"),
        )
        .orderBy("date")
    )


def balance_per_customer(
    dim_customer: DataFrame,
    dim_account: DataFrame,
    fact: DataFrame,
    customer_name: str,
) -> DataFrame:
    """`sp_BalancePerCustomer @customer_name`.

    Net change per account is `SUM(CASE WHEN transaction_type = 'Deposit' THEN
    amount ELSE -amount END)`; current balance is the account's initial balance
    plus that net change (`ISNULL(..., 0)` -> `coalesce`). Only `active`
    accounts of customers whose name matches `%customer_name%` are returned.
    """
    transaction_summary = fact.groupBy("account_id").agg(
        F.sum(
            F.when(F.col("transaction_type") == F.lit("Deposit"), F.col("amount")).otherwise(
                -F.col("amount")
            )
        )
        .cast(DecimalType(19, 4))
        .alias("total_transaction_amount")
    )

    accounts = dim_account.filter(F.col("status") == F.lit("active"))

    return (
        dim_customer.alias("c")
        .join(accounts.alias("a"), F.col("c.customer_id") == F.col("a.customer_id"), "inner")
        .join(
            transaction_summary.alias("ts"),
            F.col("a.account_id") == F.col("ts.account_id"),
            "left",
        )
        .filter(F.col("c.customer_name").like(f"%{customer_name}%"))
        .select(
            F.col("c.customer_name").alias("customer_name"),
            F.col("a.account_type").alias("account_type"),
            F.col("a.balance").cast(DecimalType(19, 4)).alias("initial_balance"),
            (F.col("a.balance") + F.coalesce(F.col("ts.total_transaction_amount"), F.lit(0)))
            .cast(DecimalType(19, 4))
            .alias("current_balance"),
        )
    )


class GoldAnalytics:
    """Table-reading wrapper exposing the stored-procedure signatures."""

    def __init__(self, spark: SparkSession, config: GoldConfig | None = None) -> None:
        self.spark = spark
        self.config = config or GoldConfig()

    def daily_transaction(self, start_date: DateLike, end_date: DateLike) -> DataFrame:
        return daily_transaction(
            self.spark.table(self.config.fact_fqn), start_date, end_date
        )

    def balance_per_customer(self, customer_name: str) -> DataFrame:
        return balance_per_customer(
            self.spark.table(self.config.dim_customer_fqn),
            self.spark.table(self.config.dim_account_fqn),
            self.spark.table(self.config.fact_fqn),
            customer_name,
        )
