"""Spark equivalent of ``sp_DailyTransaction @start_date, @end_date``.

Usage:
    spark-submit ... pyspark_jobs/reports/daily_transaction.py --start-date 2024-01-18 --end-date 2024-01-20
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark_jobs.common.config import load_config
from pyspark_jobs.common.io import read_jdbc_table
from pyspark_jobs.common.spark_session import get_spark


def daily_transaction(fact: DataFrame, start_date: dt.date | str, end_date: dt.date | str) -> DataFrame:
    """Per-day COUNT(TransactionID) and SUM(Amount) for dates in [start_date, end_date]."""
    day = F.to_date("TransactionDate")
    return (
        fact.where(day.between(F.lit(str(start_date)).cast("date"), F.lit(str(end_date)).cast("date")))
        .groupBy(day.alias("Date"))
        .agg(
            F.count("TransactionID").alias("TotalTransactions"),
            F.sum("Amount").alias("TotalAmount"),
        )
        .orderBy("Date")
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Daily transaction summary (sp_DailyTransaction)")
    parser.add_argument("--start-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=dt.date.fromisoformat)
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("DailyTransaction")
    fact = read_jdbc_table(spark, config.target, config.target_table("FactTransaction"))
    daily_transaction(fact, args.start_date, args.end_date).show(truncate=False)


if __name__ == "__main__":
    main()
