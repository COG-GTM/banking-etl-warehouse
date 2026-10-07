"""Delta-side replicas of the legacy stored procedures, used for output parity.

Ticket 9 owns the production Unity Catalog table functions; these private replicas let the
harness check parity without depending on that ticket. They mirror SQL Server semantics that
matter for parity: the DWH collation is ``SQL_Latin1_General_CP1_CI_AS`` (case-insensitive,
trailing spaces ignored in ``=``), ``COUNT`` returns INT and ``SUM(money)`` returns MONEY.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from banking_etl.validation import specs


def _ci_equals(col: str, literal: str):
    return F.lower(F.rtrim(F.col(col))) == F.lit(literal.lower().rstrip())


def daily_transaction(fact: DataFrame, start_date: str, end_date: str) -> DataFrame:
    d = F.to_date("transaction_date")
    return (
        fact.filter(d.between(F.lit(start_date).cast("date"), F.lit(end_date).cast("date")))
        .groupBy(d.alias("Date"))
        .agg(F.count("transaction_id").cast("int").alias("TotalTransactions"),
             F.sum("amount").cast(specs.MONEY).alias("TotalAmount"))
        .orderBy("Date")
    )


def like_pattern(customer_name: str) -> str:
    # '%' + @name + '%'; runs of '%' are collapsed because Spark compiles LIKE to a Java regex.
    return re.sub("%+", "%", f"%{customer_name}%").upper()


def balance_per_customer(customer: DataFrame, account: DataFrame, fact: DataFrame,
                         customer_name: str) -> DataFrame:
    summary = fact.groupBy("account_id").agg(
        F.sum(F.when(_ci_equals("transaction_type", "Deposit"), F.col("amount"))
              .otherwise(-F.col("amount"))).alias("total"))
    c, a, ts = customer.alias("c"), account.alias("a"), summary.alias("ts")
    return (
        c.join(a, F.col("c.customer_id") == F.col("a.customer_id"))
        .join(ts, F.col("a.account_id") == F.col("ts.account_id"), "left")
        .filter(F.upper(F.col("c.customer_name")).like(like_pattern(customer_name)))
        .filter(_ci_equals("a.status", "active"))
        .select(
            F.col("c.customer_name").alias("CustomerName"),
            F.col("a.account_type").alias("AccountType"),
            F.col("a.balance").cast(specs.MONEY).alias("InitialBalance"),
            (F.col("a.balance") + F.coalesce(F.col("ts.total"), F.lit(0))).cast(specs.MONEY).alias("CurrentBalance"),
        )
    )


def run_all(gold: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Execute every parity parameter set; output schemas match the legacy proc fixtures."""
    daily = None
    for start, end in specs.DAILY_TRANSACTION_PARAMS:
        df = daily_transaction(gold["fact_transaction"], start, end).select(
            F.lit(start).alias("start_date"), F.lit(end).alias("end_date"), "*")
        daily = df if daily is None else daily.unionByName(df)
    bal = None
    for name in specs.BALANCE_PER_CUSTOMER_PARAMS:
        df = balance_per_customer(gold["dim_customer"], gold["dim_account"], gold["fact_transaction"], name).select(
            F.lit(name).alias("customer_name_param"), "*")
        bal = df if bal is None else bal.unionByName(df)
    return {"sp_DailyTransaction": daily, "sp_BalancePerCustomer": bal}
