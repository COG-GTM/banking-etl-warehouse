"""Spark read/write helpers shared by the load jobs."""

from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from common.config import JdbcConfig, JobConfig

log = logging.getLogger(__name__)


def read_jdbc(spark: SparkSession, db: JdbcConfig, table: str) -> DataFrame:
    return spark.read.format("jdbc").options(**db.options()).option("dbtable", table).load()


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").option("inferSchema", "false").csv(path)


def read_excel(spark: SparkSession, path: str, sheet: str | int = 0) -> DataFrame:
    """Read a small .xlsx via pandas/openpyxl so no Maven spark-excel library is needed.

    ``/Volumes/...`` and local paths are readable directly by the driver.
    """
    pdf = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    return spark.createDataFrame(pdf)


def write_table(df: DataFrame, cfg: JobConfig, name: str) -> int:
    """Full refresh of a target table (Talend jobs truncated and reloaded on every run)."""
    table = cfg.table(name)
    rows = df.count()
    (df.write.format(cfg.table_format).mode("overwrite").option("overwriteSchema", "true").saveAsTable(table))
    log.info("Wrote %d rows to %s", rows, table)
    return rows


def ensure_schema(spark: SparkSession, cfg: JobConfig) -> None:
    schema = f"{cfg.catalog}.{cfg.schema}" if cfg.catalog else cfg.schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
