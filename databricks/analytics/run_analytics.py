"""Databricks job entry point: run both analytics procedures and write gold tables.

Usage (as a spark_python_task or from a notebook)::

    python run_analytics.py --catalog dev_banking --schema gold \
        --start-date 2024-01-01 --end-date 2024-12-31 --customer-name ""

Reads ``DimCustomer``, ``DimAccount`` and ``FactTransaction`` from
``<catalog>.<schema>`` and overwrites ``<catalog>.<schema>.DailyTransaction``
and ``<catalog>.<schema>.BalancePerCustomer``.  An empty ``--customer-name``
matches every customer (``LIKE '%%'``), which is what a scheduled job wants.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from databricks.analytics.procedures import balance_per_customer, daily_transaction

GOLD_DAILY_TRANSACTION = "DailyTransaction"
GOLD_BALANCE_PER_CUSTOMER = "BalancePerCustomer"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--start-date", default="1900-01-01")
    parser.add_argument(
        "--end-date", default=dt.datetime.now(tz=dt.timezone.utc).date().isoformat()
    )
    parser.add_argument("--customer-name", default="")
    parser.add_argument(
        "--format", default="delta", help="Table format (delta or parquet)"
    )
    return parser.parse_args(argv)


def fq(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def write_gold(df: DataFrame, name: str, fmt: str) -> None:
    df.write.format(fmt).mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(name)


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    def read(table: str) -> DataFrame:
        return spark.table(fq(args.catalog, args.schema, table))

    fact_df = read("FactTransaction")
    customer_df = read("DimCustomer")
    account_df = read("DimAccount")

    write_gold(
        daily_transaction(fact_df, args.start_date, args.end_date),
        fq(args.catalog, args.schema, GOLD_DAILY_TRANSACTION),
        args.format,
    )
    write_gold(
        balance_per_customer(customer_df, account_df, fact_df, args.customer_name),
        fq(args.catalog, args.schema, GOLD_BALANCE_PER_CUSTOMER),
        args.format,
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("banking_dwh_run_analytics").getOrCreate()
    run(spark, args)


if __name__ == "__main__":
    main()
