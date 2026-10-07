"""PySpark DataFrame equivalents of ``sp_DailyTransaction`` / ``sp_BalancePerCustomer``.

Same semantics as the SQL table functions in ``sql/analytics`` (the SQL Server parity rules are documented
in ``02_fn_balance_per_customer.sql``); use these when composing the logic into DataFrame pipelines and local tests.
"""
from __future__ import annotations

import datetime as dt
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from banking_etl.analytics.target import GOLD_TABLES, AnalyticsTarget

MONEY = "decimal(19,4)"
CUSTOMER_NAME_MAX_LENGTH = 100  # T-SQL @customer_name VARCHAR(100): longer input is silently truncated
DAILY_COLUMNS = ("date", "total_transactions", "total_amount")
BALANCE_COLUMNS = ("customer_name", "account_type", "initial_balance", "current_balance")


def gold_frames(spark, target: AnalyticsTarget) -> dict[str, DataFrame]:
    """The gold tables the procedures read: ``fact_transaction``, ``dim_account``, ``dim_customer``."""
    return {name: spark.table(target.table(name)) for name in GOLD_TABLES}


def _ci_equals(col: str, value: str):
    """T-SQL ``col = 'value'`` under a CI collation: case-insensitive, trailing spaces ignored."""
    return F.lower(F.rtrim(F.col(col))) == value.lower()


def like_pattern(customer_name: str | None) -> str | None:
    """Spark ILIKE pattern equivalent to T-SQL ``'%' + @customer_name + '%'`` (None when the name is NULL).

    Truncated to VARCHAR(100), backslashes doubled (T-SQL LIKE has no escape character) and runs of ``%``
    collapsed: Spark compiles LIKE to a Java regex, where a run of wildcards backtracks exponentially.
    """
    if customer_name is None:
        return None
    escaped = customer_name[:CUSTOMER_NAME_MAX_LENGTH].replace("\\", "\\\\")
    return re.sub("%+", "%", "%" + escaped + "%")


def _as_date(value):
    return dt.date.fromisoformat(value.strip()) if isinstance(value, str) else value


def daily_transaction_df(fact: DataFrame, start_date: dt.date | str | None, end_date: dt.date | str | None) -> DataFrame:
    """Daily COUNT(transaction_id) / SUM(amount) for CAST(transaction_date AS DATE) BETWEEN start AND end."""
    day = F.col("transaction_date").cast("date")
    start = F.lit(_as_date(start_date)).cast("date")
    end = F.lit(_as_date(end_date)).cast("date")
    return (
        fact.where(day.between(start, end))
        .groupBy(day.alias("date"))
        .agg(
            F.count("transaction_id").cast("int").alias("total_transactions"),
            F.sum("amount").cast(MONEY).alias("total_amount"),
        )
        .orderBy("date")
    )


def balance_per_customer_df(
    customer: DataFrame, account: DataFrame, fact: DataFrame, customer_name: str | None
) -> DataFrame:
    """Active accounts of matching customers with balance + signed transaction total (Deposit +, else -)."""
    signed = F.when(_ci_equals("transaction_type", "deposit"), F.col("amount")).otherwise(-F.col("amount"))
    summary = fact.groupBy("account_id").agg(F.sum(signed).alias("total_transaction_amount"))
    pattern = like_pattern(customer_name)
    name_matches = F.lit(None).cast("boolean") if pattern is None else F.col("c.customer_name").ilike(pattern)
    return (
        customer.alias("c")
        .join(account.alias("a"), F.col("c.customer_id") == F.col("a.customer_id"))
        .join(summary.alias("ts"), F.col("a.account_id") == F.col("ts.account_id"), "left")
        .where(name_matches & (F.lower(F.rtrim(F.col("a.status"))) == "active"))
        .select(
            F.col("c.customer_name").cast("string").alias("customer_name"),
            F.col("a.account_type").cast("string").alias("account_type"),
            F.col("a.balance").cast(MONEY).alias("initial_balance"),
            (F.col("a.balance") + F.coalesce(F.col("ts.total_transaction_amount"), F.lit(0)))
            .cast(MONEY)
            .alias("current_balance"),
        )
    )
