"""sp_DailyTransaction: daily transaction count and amount for a date range."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from banking_etl import schemas
from banking_etl.analytics import daily_transaction
from banking_etl.config import EtlConfig
from banking_etl.runtime import base_parser, bootstrap, parse
from banking_etl.writers import read_table

log = logging.getLogger(__name__)


def run(spark: SparkSession, config: EtlConfig, start_date: str, end_date: str) -> DataFrame:
    fact = read_table(spark, config.warehouse, schemas.FACT_TRANSACTION)
    return daily_transaction(fact, start_date, end_date)


def main(argv: Sequence[str] | None = None) -> None:
    parser = base_parser(__doc__)
    parser.add_argument("--start-date", required=True, help="yyyy-MM-dd (inclusive)")
    parser.add_argument("--end-date", required=True, help="yyyy-MM-dd (inclusive)")
    parser.add_argument(
        "--output-table", default="", help="Optionally persist the result to this Delta table"
    )
    args = parse(parser, argv)
    spark, config = bootstrap("daily_transaction", args)
    result = run(spark, config, args.start_date, args.end_date)
    result.show(truncate=False)
    if args.output_table:
        result.write.format("delta").mode("overwrite").saveAsTable(args.output_table)
        log.info("Saved daily_transaction result to %s", args.output_table)


if __name__ == "__main__":
    main()
