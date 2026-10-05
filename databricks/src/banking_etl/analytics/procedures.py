"""PySpark DataFrame equivalents of ``sp_DailyTransaction`` / ``sp_BalancePerCustomer``.

Same semantics as the SQL table functions in ``sql/analytics`` (see those files for the SQL Server
parity rules); use these when composing the logic into other DataFrame pipelines.
"""
from __future__ import annotations

import datetime as dt
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from banking_etl.config import GOLD_DIM_ACCOUNT, GOLD_DIM_CUSTOMER, GOLD_FACT_TRANSACTION, Settings

MONEY = "decimal(19,4)"
CUSTOMER_NAME_MAX_LENGTH = 100  # T-SQL @customer_name VARCHAR(100): longer input is silently truncated
DAILY_COLUMNS = ("Date", "TotalTransactions", "TotalAmount")
BALANCE_COLUMNS = ("CustomerName", "AccountType", "InitialBalance", "CurrentBalance")


def gold_frames(spark, settings: Settings) -> dict[str, DataFrame]:
    """The gold tables the procedures read: ``fact_transaction``, ``dim_account``, ``dim_customer``."""
    return {
        name: spark.table(settings.table("gold", name))
        for name in (GOLD_FACT_TRANSACTION, GOLD_DIM_ACCOUNT, GOLD_DIM_CUSTOMER)
    }


def _ci_equals(col: str, value: str):
    """T-SQL ``col = 'value'`` under a CI collation: case-insensitive, trailing spaces ignored."""
    return F.lower(F.rtrim(F.col(col))) == value.lower()


def like_pattern(customer_name: str | None) -> str | None:
    """Spark ILIKE pattern equivalent to T-SQL ``'%' + @customer_name + '%'`` (None when the name is NULL).

    Backslashes are doubled (T-SQL LIKE has no escape character) and runs of ``%`` collapsed: Spark
    compiles LIKE to a Java regex, where a run of wildcards backtracks exponentially.
    """
    if customer_name is None:
        return None
    escaped = customer_name[:CUSTOMER_NAME_MAX_LENGTH].replace("\\", "\\\\")
    return re.sub("%+", "%", "%" + escaped + "%")


def daily_transaction_df(
    fact: DataFrame, start_date: dt.date | str | None, end_date: dt.date | str | None
) -> DataFrame:
    """Daily COUNT(TransactionID) / SUM(Amount) for CAST(TransactionDate AS DATE) BETWEEN start AND end."""
    day = F.col("TransactionDate").cast("date")
    start = F.lit(start_date).cast("date")
    end = F.lit(end_date).cast("date")
    return (
        fact.where(day.between(start, end))
        .groupBy(day.alias("Date"))
        .agg(
            F.count("TransactionID").cast("int").alias("TotalTransactions"),
            F.sum("Amount").cast(MONEY).alias("TotalAmount"),
        )
        .orderBy("Date")
    )


def balance_per_customer_df(
    customer: DataFrame, account: DataFrame, fact: DataFrame, customer_name: str | None
) -> DataFrame:
    """Active accounts of matching customers with Balance + signed transaction total (Deposit +, else -)."""
    signed = F.when(_ci_equals("TransactionType", "deposit"), F.col("Amount")).otherwise(-F.col("Amount"))
    summary = fact.groupBy("AccountID").agg(F.sum(signed).alias("TotalTransactionAmount"))
    pattern = like_pattern(customer_name)
    name_matches = F.lit(None).cast("boolean") if pattern is None else F.col("c.CustomerName").ilike(pattern)
    return (
        customer.alias("c")
        .join(account.alias("a"), F.col("c.CustomerID") == F.col("a.CustomerID"))
        .join(summary.alias("ts"), F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .where(name_matches & (F.lower(F.rtrim(F.col("a.Status"))) == "active"))
        .select(
            F.col("c.CustomerName").alias("CustomerName"),
            F.col("a.AccountType").alias("AccountType"),
            F.col("a.Balance").cast(MONEY).alias("InitialBalance"),
            (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0)))
            .cast(MONEY)
            .alias("CurrentBalance"),
        )
    )
