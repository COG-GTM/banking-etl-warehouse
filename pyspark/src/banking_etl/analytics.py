"""PySpark equivalents of the T-SQL stored procedures in sql_scripts/02_create_procedures.sql.

String comparisons are case-insensitive and ignore trailing spaces to match the legacy
database collation (SQL_Latin1_General_CP1_CI_AS).
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from banking_etl.schemas import MONEY

DateLike = str | dt.date


def _date_literal(value: DateLike) -> F.Column:
    if isinstance(value, dt.date):
        return F.lit(value.isoformat()).cast("date")
    return F.to_date(F.lit(value))


def _ci_equals(column: str, value: str) -> F.Column:
    return F.lower(F.rtrim(F.col(column))) == value.lower().rstrip()


def daily_transaction(fact_transaction: DataFrame, start_date: DateLike, end_date: DateLike) -> DataFrame:
    """sp_DailyTransaction @start_date, @end_date.

    Daily transaction count and total amount for days in [start_date, end_date].
    """
    day = F.to_date("TransactionDate")
    return (
        fact_transaction.where(day.between(_date_literal(start_date), _date_literal(end_date)))
        .groupBy(day.alias("Date"))
        .agg(
            F.count("TransactionID").cast(IntegerType()).alias("TotalTransactions"),
            F.sum("Amount").cast(MONEY).alias("TotalAmount"),
        )
        .orderBy("Date")
    )


def transaction_summary(fact_transaction: DataFrame) -> DataFrame:
    """The TransactionSummary CTE: deposits add, every other type subtracts."""
    signed = F.when(_ci_equals("TransactionType", "Deposit"), F.col("Amount")).otherwise(-F.col("Amount"))
    return fact_transaction.groupBy("AccountID").agg(F.sum(signed).alias("TotalTransactionAmount"))


def balance_per_customer(
    fact_transaction: DataFrame,
    dim_account: DataFrame,
    dim_customer: DataFrame,
    customer_name: str,
) -> DataFrame:
    """sp_BalancePerCustomer @customer_name.

    Current balance of every active account whose customer name matches
    ``LIKE '%' + customer_name + '%'``.
    """
    c = dim_customer.alias("c")
    a = dim_account.alias("a")
    ts = transaction_summary(fact_transaction).alias("ts")
    pattern = f"%{customer_name.upper()}%"
    return (
        c.join(a, F.col("c.CustomerID") == F.col("a.CustomerID"), "inner")
        .join(ts, F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .where(F.upper(F.col("c.CustomerName")).like(pattern) & _ci_equals("a.Status", "active"))
        .select(
            F.col("c.CustomerName").alias("CustomerName"),
            F.col("a.AccountType").alias("AccountType"),
            F.col("a.Balance").alias("InitialBalance"),
            (F.col("a.Balance") + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0)))
            .cast(MONEY)
            .alias("CurrentBalance"),
        )
        .orderBy("CustomerName", "AccountType", "InitialBalance")
    )
