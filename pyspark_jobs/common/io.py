"""JDBC and file I/O helpers shared by the ETL jobs."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .config import FileSources, JdbcConfig

log = logging.getLogger(__name__)

WRITE_MODES = ("overwrite", "append", "delete_insert")


def read_jdbc_table(spark: SparkSession, jdbc: JdbcConfig, table: str) -> DataFrame:
    return spark.read.format("jdbc").option("url", jdbc.url).option("dbtable", table).options(**jdbc.properties).load()


def read_jdbc_query(spark: SparkSession, jdbc: JdbcConfig, query: str) -> DataFrame:
    return spark.read.format("jdbc").option("url", jdbc.url).option("query", query).options(**jdbc.properties).load()


def execute_jdbc_statement(spark: SparkSession, jdbc: JdbcConfig, sql: str) -> int:
    """Run a DDL/DML statement on the target through the JVM JDBC driver."""
    jvm = spark.sparkContext._jvm
    # Jars added via --jars/--packages live in Spark's class loader, not the
    # system one DriverManager uses, so load and call the driver directly.
    loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    driver = jvm.java.lang.Class.forName(jdbc.properties["driver"], True, loader).newInstance()
    props = jvm.java.util.Properties()
    for key, value in jdbc.properties.items():
        if key != "driver":
            props.setProperty(key, value)
    conn = driver.connect(jdbc.url, props)
    try:
        stmt = conn.createStatement()
        try:
            return stmt.executeUpdate(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def write_jdbc(
    df: DataFrame,
    spark: SparkSession,
    jdbc: JdbcConfig,
    table: str,
    mode: str = "overwrite",
) -> None:
    """Write ``df`` into an existing DWH table without touching its DDL.

    * ``overwrite``     – Spark ``overwrite`` with ``truncate=true``: the table
      (PKs, FKs, column types) is kept and only its rows are replaced. SQL
      Server refuses ``TRUNCATE`` on tables referenced by a foreign key, so
      use this for ``FactTransaction`` (the Talend job's TRUNCATE + INSERT).
    * ``delete_insert`` – ``DELETE FROM`` followed by an append. Full reload
      for FK-referenced dimensions (``DimAccount``, ``DimBranch``).
    * ``append``        – insert only (Talend ``CREATE_IF_NOT_EXISTS`` + ``INSERT``
      on an empty table).
    """
    if mode not in WRITE_MODES:
        raise ValueError(f"Unsupported write mode {mode!r}; expected one of {WRITE_MODES}")

    if mode == "delete_insert":
        try:
            deleted = execute_jdbc_statement(spark, jdbc, f"DELETE FROM {table}")
        except Exception as exc:
            if "REFERENCE constraint" in str(exc):
                raise RuntimeError(
                    f"Cannot reload {table}: FactTransaction rows still reference it. "
                    "Run pyspark_jobs/run_all.py (clears FactTransaction first) or empty "
                    "FactTransaction before reloading this dimension."
                ) from None
            raise
        log.info("Deleted %s existing rows from %s", deleted, table)
        spark_mode = "append"
    else:
        spark_mode = mode

    writer = (
        df.write.format("jdbc")
        .option("url", jdbc.url)
        .option("dbtable", table)
        .options(**jdbc.properties)
        .mode(spark_mode)
    )
    if spark_mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.save()


def read_csv(
    spark: SparkSession,
    path: str,
    schema: StructType | None = None,
    timestamp_format: str | None = None,
) -> DataFrame:
    reader = spark.read.option("header", "true").option("mode", "FAILFAST")
    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    if timestamp_format:
        reader = reader.option("timestampFormat", timestamp_format)
    return reader.csv(path)


def read_excel(spark: SparkSession, files: FileSources, path: str | None = None) -> DataFrame:
    """Read the first header row + data from an .xlsx sheet.

    ``EXCEL_READER=pandas`` (default) reads the sheet with pandas/openpyxl on the
    driver and converts it with ``spark.createDataFrame`` (the transaction
    workbook is small; pyspark 3.5's ``pyspark.pandas.read_excel`` is not
    compatible with pandas 2.x);
    ``EXCEL_READER=crealytics`` uses the ``com.crealytics.spark.excel`` data
    source (the package is added to ``spark.jars.packages`` automatically).
    """
    path = path or files.transaction_excel
    if files.excel_reader == "crealytics":
        return (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dataAddress", f"'{files.excel_sheet}'!A1")
            .load(path)
        )
    if files.excel_reader != "pandas":
        raise ValueError(f"Unsupported EXCEL_READER {files.excel_reader!r}")

    import pandas as pd

    pdf = pd.read_excel(path, sheet_name=files.excel_sheet, engine="openpyxl")
    return spark.createDataFrame(pdf)


def select_as(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Project ``df`` to ``{target_column: source_column}`` in mapping order."""
    return df.select(*[F.col(src).alias(dst) for dst, src in mapping.items()])
