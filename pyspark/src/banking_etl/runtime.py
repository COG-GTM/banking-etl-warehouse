"""Shared entry-point plumbing for the job scripts."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from pyspark.sql import SparkSession

from banking_etl.config import EtlConfig, load_config
from banking_etl.spark_session import get_spark

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: $BANKING_ETL_CONFIG or pyspark/config/config.yaml)",
    )
    return parser


def bootstrap(job_name: str, args: argparse.Namespace) -> tuple[SparkSession, EtlConfig]:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    config = load_config(args.config)
    spark = get_spark(f"{config.app_name_prefix}-{job_name}")
    return spark, config


def parse(parser: argparse.ArgumentParser, argv: Sequence[str] | None) -> argparse.Namespace:
    # Databricks python_wheel_task may pass extra platform arguments; ignore unknown ones.
    args, _ = parser.parse_known_args(argv)
    return args
