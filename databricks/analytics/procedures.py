"""DataFrame equivalents of ``sql_scripts/02_create_procedures.sql``.

Both functions are pure: they take DataFrames in and return a DataFrame out and
never touch a catalog, so they can be unit-tested against in-memory data and
reused from notebooks, jobs or SQL UDTFs.

SQL Server semantics that are replicated deliberately:

* ``MONEY`` arithmetic is emulated with ``DECIMAL(19,4)``.
* The default SQL Server collation (``SQL_Latin1_General_CP1_CI_AS``) is
  case-insensitive, so ``CustomerName LIKE '%name%'`` and ``Status = 'active'``
  are both evaluated case-insensitively here. ``TransactionType = 'Deposit'`` is
  likewise compared case-insensitively.
* ``BETWEEN`` on ``CAST(TransactionDate AS DATE)`` is inclusive on both ends.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(19, 4)

DateLike = str | dt.date


def _as_date(value: DateLike) -> Column:
    if isinstance(value, dt.datetime):
        value = value.date()
    if isinstance(value, dt.date):
        value = value.isoformat()
    return F.to_date(F.lit(value))


def _money(col: Column) -> Column:
    return col.cast(MONEY)


def daily_transaction(
    fact_df: DataFrame, start_date: DateLike, end_date: DateLike
) -> DataFrame:
    """Equivalent of ``EXEC sp_DailyTransaction @start_date, @end_date``.

    Returns ``Date`` (date), ``TotalTransactions`` (long), ``TotalAmount``
    (decimal(19,4)) ordered by ``Date``.
    """
    txn_date = F.to_date(F.col("TransactionDate"))
    return (
        fact_df.withColumn("Date", txn_date)
        .where(F.col("Date").between(_as_date(start_date), _as_date(end_date)))
        .groupBy("Date")
        .agg(
            F.count("TransactionID").alias("TotalTransactions"),
            _money(F.sum(_money(F.col("Amount")))).alias("TotalAmount"),
        )
        .orderBy("Date")
    )


def transaction_summary(fact_df: DataFrame) -> DataFrame:
    """The ``TransactionSummary`` CTE: signed net movement per ``AccountID``."""
    signed = F.when(
        F.lower(F.col("TransactionType")) == "deposit", F.col("Amount")
    ).otherwise(-F.col("Amount"))
    return fact_df.groupBy("AccountID").agg(
        _money(F.sum(_money(signed))).alias("TotalTransactionAmount")
    )


def balance_per_customer(
    customer_df: DataFrame,
    account_df: DataFrame,
    fact_df: DataFrame,
    customer_name: str,
) -> DataFrame:
    """Equivalent of ``EXEC sp_BalancePerCustomer @customer_name``.

    Returns ``CustomerName``, ``AccountType``, ``InitialBalance``,
    ``CurrentBalance`` for every *active* account whose owner's name contains
    ``customer_name`` (case-insensitive substring match, like the T-SQL
    ``LIKE '%' + @customer_name + '%'`` under the default CI collation). As in
    T-SQL, ``%`` and ``_`` inside ``customer_name`` act as LIKE wildcards.
    """
    summary = transaction_summary(fact_df)
    c = customer_df.alias("c")
    a = account_df.alias("a")
    ts = summary.alias("ts")

    pattern = f"%{customer_name.lower()}%"
    return (
        c.join(a, F.col("c.CustomerID") == F.col("a.CustomerID"), "inner")
        .join(ts, F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .where(F.lower(F.col("c.CustomerName")).like(pattern))
        .where(F.lower(F.col("a.Status")) == "active")
        .select(
            F.col("c.CustomerName").alias("CustomerName"),
            F.col("a.AccountType").alias("AccountType"),
            _money(F.col("a.Balance")).alias("InitialBalance"),
            _money(
                _money(F.col("a.Balance"))
                + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0).cast(MONEY))
            ).alias("CurrentBalance"),
        )
    )
