"""Source readers: SQL Server over JDBC and staged CSV/Excel files on DBFS / UC volumes."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from common.config import EtlConfig, load_config

EXCEL_FORMAT = "com.crealytics.spark.excel"
EXCEL_EXTENSIONS = (".xlsx", ".xls")


def _active_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")
    return spark


def read_jdbc_table(name: str, spark: SparkSession | None = None, config: EtlConfig | None = None) -> DataFrame:
    """Read one of the SQL Server source tables (see ``SOURCE_TABLES``)."""
    spark = spark or _active_spark()
    config = config or load_config(spark)
    reader = spark.read.format("jdbc").option("url", config.jdbc_url).option("dbtable", config.source_table(name))
    for key, value in config.jdbc_properties.items():
        reader = reader.option(key, value)
    return reader.load()


def _local_path(path: str) -> str:
    """Translate a ``dbfs:/`` URI to its FUSE path so pandas can open it."""
    if path.startswith("dbfs:/Volumes/"):
        return path[len("dbfs:") :]
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/") :]
    return path


def _read_excel_with_pandas(spark: SparkSession, path: str, sheet_name: str | int) -> DataFrame:
    import pandas as pd

    pdf = pd.read_excel(_local_path(path), sheet_name=sheet_name, dtype=str)
    pdf = pdf.astype(object).where(pdf.notna(), None)
    return spark.createDataFrame(pdf)


def read_staged_file(
    path: str,
    spark: SparkSession | None = None,
    file_format: str | None = None,
    sheet_name: str | int = 0,
    header: bool = True,
) -> DataFrame:
    """Read a staged CSV or Excel file. Format is inferred from the extension when not given.

    Excel uses the ``com.crealytics.spark.excel`` connector when it is installed on the
    cluster and falls back to pandas (driver-side) otherwise. All columns are read as strings.
    """
    spark = spark or _active_spark()
    ext = os.path.splitext(path)[1].lower()
    file_format = (file_format or ("excel" if ext in EXCEL_EXTENSIONS else ext.lstrip("."))).lower()

    if file_format == "csv":
        return spark.read.option("header", str(header).lower()).option("inferSchema", "false").csv(path)

    if file_format == "excel":
        try:
            reader = (
                spark.read.format(EXCEL_FORMAT).option("header", str(header).lower()).option("inferSchema", "false")
            )
            if isinstance(sheet_name, str):
                reader = reader.option("dataAddress", f"'{sheet_name}'!A1")
            return reader.load(path)
        except Exception:
            return _read_excel_with_pandas(spark, path, sheet_name)

    raise ValueError(f"Unsupported staged file format {file_format!r} for {path}")
