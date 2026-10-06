"""Entry-point boilerplate for ``spark_python_task`` scripts."""

from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.sql import SparkSession

from common.config import JobConfig, parse_args
from common.io import ensure_schema

JobFn = Callable[[SparkSession, JobConfig], int]


def get_spark() -> SparkSession:
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def main(job: JobFn, argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = parse_args(argv)
    spark = get_spark()
    ensure_schema(spark, cfg)
    return job(spark, cfg)
