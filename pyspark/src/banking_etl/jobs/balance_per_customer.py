"""sp_BalancePerCustomer: current balance of a customer's active accounts."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from banking_etl import schemas
from banking_etl.analytics import balance_per_customer
from banking_etl.config import EtlConfig
from banking_etl.runtime import base_parser, bootstrap, parse
from banking_etl.writers import read_table

log = logging.getLogger(__name__)


def run(spark: SparkSession, config: EtlConfig, customer_name: str) -> DataFrame:
    return balance_per_customer(
        read_table(spark, config.warehouse, schemas.FACT_TRANSACTION),
        read_table(spark, config.warehouse, schemas.DIM_ACCOUNT),
        read_table(spark, config.warehouse, schemas.DIM_CUSTOMER),
        customer_name,
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = base_parser(__doc__)
    parser.add_argument(
        "--customer-name", required=True, help="Matched as LIKE '%%<name>%%' (case-insensitive)"
    )
    parser.add_argument(
        "--output-table", default="", help="Optionally persist the result to this Delta table"
    )
    args = parse(parser, argv)
    spark, config = bootstrap("balance_per_customer", args)
    result = run(spark, config, args.customer_name)
    result.show(truncate=False)
    if args.output_table:
        result.write.format("delta").mode("overwrite").saveAsTable(args.output_table)
        log.info("Saved balance_per_customer result to %s", args.output_table)


if __name__ == "__main__":
    main()
