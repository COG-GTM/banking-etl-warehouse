"""Idempotent Unity Catalog setup: catalog, bronze/silver/gold schemas, landing volume.

Run as the ``uc_setup`` bundle job (wheel entry point ``uc_setup``) or locally::

    python -m banking_etl.uc_setup --catalog banking_dwh --print-sql
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import TYPE_CHECKING, Sequence

from banking_etl.config import LAYERS, EtlConfig, parse_bool, quote_identifier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

LAYER_COMMENTS = {
    "bronze": "Raw source data as ingested (sample.bak tables, transaction_csv, transaction_excel)",
    "silver": "Cleansed, conformed and deduplicated source data",
    "gold": "Star schema: dim_account, dim_branch, dim_customer, fact_transaction",
}


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def setup_statements(
    config: EtlConfig,
    *,
    create_catalog: bool = True,
    create_volume: bool = True,
) -> list[str]:
    """Ordered ``CREATE ... IF NOT EXISTS`` statements for the warehouse."""
    catalog = quote_identifier(config.catalog)
    statements: list[str] = []
    if create_catalog:
        statements.append(
            f"CREATE CATALOG IF NOT EXISTS {catalog} "
            f"COMMENT {_sql_string('Banking ETL warehouse (migrated from Talend + SQL Server)')}"
        )
    for layer in LAYERS:
        schema = f"{catalog}.{quote_identifier(config.schema(layer))}"
        statements.append(
            f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT {_sql_string(LAYER_COMMENTS[layer])}"
        )
    if create_volume:
        volume = ".".join(
            quote_identifier(p) for p in (config.catalog, config.landing_schema, config.landing_volume)
        )
        statements.append(
            f"CREATE VOLUME IF NOT EXISTS {volume} "
            f"COMMENT {_sql_string('Landing zone for raw source files (csv, xlsx, bak extracts)')}"
        )
    return statements


def run_setup(
    spark: "SparkSession",
    config: EtlConfig,
    *,
    create_catalog: bool = True,
    create_volume: bool = True,
) -> list[str]:
    """Execute :func:`setup_statements` and return what ran."""
    statements = setup_statements(config, create_catalog=create_catalog, create_volume=create_volume)
    for statement in statements:
        log.info("Executing: %s", statement)
        spark.sql(statement)
    return statements


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="uc_setup", description=__doc__.splitlines()[0])
    EtlConfig.add_arguments(parser)
    parser.add_argument(
        "--create-catalog",
        type=parse_bool,
        default=True,
        help="Run CREATE CATALOG (set false when the catalog already exists / no CREATE CATALOG grant)",
    )
    parser.add_argument("--create-volume", type=parse_bool, default=True, help="Create the landing volume")
    parser.add_argument("--print-sql", action="store_true", help="Print the SQL instead of executing it")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    config = EtlConfig.from_namespace(args)
    if args.print_sql:
        statements = setup_statements(
            config, create_catalog=args.create_catalog, create_volume=args.create_volume
        )
        print(";\n".join(statements) + ";")
        return 0

    from banking_etl.spark import get_spark

    run_setup(
        get_spark("banking_etl_uc_setup"),
        config,
        create_catalog=args.create_catalog,
        create_volume=args.create_volume,
    )
    print(
        json.dumps(
            {"catalog": config.catalog, "schemas": config.schemas, "landing_path": config.landing_path}
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
