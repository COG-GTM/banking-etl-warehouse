"""PySpark replacements for the DWH stored procedures."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame

from pyspark_jobs.common.config import EtlConfig
from pyspark_jobs.common.io import OUTPUT_DESTINATIONS, write_output


def add_output_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("output")
    group.add_argument("--output", choices=OUTPUT_DESTINATIONS, default="console", help="Where to deliver the result")
    group.add_argument("--output-path", help="Directory for --output csv")
    group.add_argument("--output-table", help="DWH table for --output jdbc (e.g. dbo.rpt_DailyTransaction)")
    group.add_argument("--output-mode", choices=["overwrite", "append"], default="overwrite")


def deliver(df: DataFrame, args: argparse.Namespace, cfg: EtlConfig) -> None:
    write_output(
        df,
        args.output,
        path=args.output_path,
        table=args.output_table,
        db=cfg.dwh_db,
        mode=args.output_mode,
    )
