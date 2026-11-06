"""Replaces sp_DailyTransaction: daily transaction count and amount for a date range.

EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-20';
"""

from __future__ import annotations

import argparse
import datetime as dt

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


def daily_transaction(fact: DataFrame, start_date: dt.date, end_date: dt.date) -> DataFrame:
    tx_date = F.to_date("TransactionDate")
    return (
        fact.where(tx_date.between(F.lit(start_date), F.lit(end_date)))
        .groupBy(tx_date.alias("Date"))
        .agg(
            F.count("TransactionID").alias("TotalTransactions"),
            F.sum("Amount").alias("TotalAmount"),
        )
        .orderBy("Date")
    )


def run(spark: SparkSession, cfg: EtlConfig, start_date: dt.date, end_date: dt.date) -> DataFrame:
    return daily_transaction(read_jdbc(spark, cfg.dwh_db, table="FactTransaction"), start_date, end_date)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", required=True, type=dt.date.fromisoformat, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, type=dt.date.fromisoformat, help="YYYY-MM-DD (inclusive)")
    add_output_args(parser)
    args = parser.parse_args(argv)
    configure_logging()
    cfg = EtlConfig.from_env()
    spark = get_spark("daily_transaction", cfg)
    deliver(run(spark, cfg, args.start_date, args.end_date), args, cfg)


if __name__ == "__main__":
    main()
