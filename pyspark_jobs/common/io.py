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
    return execute_jdbc_transaction(spark, jdbc, [sql])[0]


def execute_jdbc_transaction(spark: SparkSession, jdbc: JdbcConfig, statements: list[str]) -> list[int]:
    """Run ``statements`` in one transaction; roll back all of them if any fails."""
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
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        try:
            counts = [stmt.executeUpdate(sql) for sql in statements]
            conn.commit()
            return counts
        except Exception:
            conn.rollback()
            raise
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

    * ``overwrite`` / ``delete_insert`` – full reload (the Talend job's
      TRUNCATE/DELETE + INSERT). Rows are first written to a ``<table>__staging``
      table, then ``DELETE FROM`` + ``INSERT ... SELECT`` run in one SQL Server
      transaction, so a failed load leaves the previous rows in place. ``DELETE``
      rather than ``TRUNCATE`` because SQL Server refuses ``TRUNCATE`` on tables
      referenced by a foreign key (``DimAccount``, ``DimBranch``). The table's
      PKs, FKs and column types are never touched.
    * ``append`` – insert only (Talend ``CREATE_IF_NOT_EXISTS`` + ``INSERT``
      on an empty table).
    """
    if mode not in WRITE_MODES:
        raise ValueError(f"Unsupported write mode {mode!r}; expected one of {WRITE_MODES}")

    if mode == "append":
        _jdbc_writer(df, jdbc, table).mode("append").save()
        return

    staging = f"{table}__staging"
    columns = ", ".join(f"[{c}]" for c in df.columns)
    _jdbc_writer(df, jdbc, staging).mode("overwrite").save()
    try:
        deleted, inserted = execute_jdbc_transaction(
            spark,
            jdbc,
            [f"DELETE FROM {table}", f"INSERT INTO {table} ({columns}) SELECT {columns} FROM {staging}"],
        )
    except Exception as exc:
        if "REFERENCE constraint" in str(exc):
            raise RuntimeError(
                f"Cannot reload {table}: FactTransaction rows still reference it. "
                "Run pyspark_jobs/run_all.py (clears FactTransaction first) or empty "
                "FactTransaction before reloading this dimension."
            ) from None
        raise
    finally:
        execute_jdbc_statement(spark, jdbc, f"DROP TABLE IF EXISTS {staging}")
    log.info("Replaced %s rows in %s with %s rows", deleted, table, inserted)


def _jdbc_writer(df: DataFrame, jdbc: JdbcConfig, table: str):
    return df.write.format("jdbc").option("url", jdbc.url).option("dbtable", table).options(**jdbc.properties)


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
