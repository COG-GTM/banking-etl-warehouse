"""I/O helpers: SQL Server JDBC read/write, CSV/Excel reads and report destinations."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .config import MSSQL_JDBC_DRIVER, DbConfig

log = logging.getLogger(__name__)

TABLE_WRITE_MODES = ("append", "overwrite")


def jdbc_options(db: DbConfig) -> dict[str, str]:
    options = {"url": db.jdbc_url, "driver": MSSQL_JDBC_DRIVER}
    if db.user:
        options["user"] = db.user
    if db.password:
        options["password"] = db.password
    return options


def read_jdbc(spark: SparkSession, db: DbConfig, table: str | None = None, query: str | None = None) -> DataFrame:
    if (table is None) == (query is None):
        raise ValueError("Pass exactly one of table or query")
    reader = spark.read.format("jdbc").options(**jdbc_options(db))
    reader = reader.option("dbtable", db.qualified(table)) if table else reader.option("query", query)
    return reader.load()


def write_jdbc(df: DataFrame, db: DbConfig, table: str, mode: str) -> None:
    """Writes into an existing DWH table.

    ``overwrite`` uses ``truncate=true`` so SQL Server keeps the table's DDL (PKs/FKs)
    instead of Spark dropping and recreating it.
    """
    if mode not in TABLE_WRITE_MODES:
        raise ValueError(f"Unsupported JDBC write mode {mode!r}; expected one of {TABLE_WRITE_MODES}")
    (
        df.write.format("jdbc")
        .options(**jdbc_options(db))
        .option("dbtable", db.qualified(table))
        .option("truncate", "true")
        .mode(mode)
        .save()
    )


def _jdbc_connection(spark: SparkSession, db: DbConfig):
    jvm = spark._jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = jvm.java.lang.Class.forName(MSSQL_JDBC_DRIVER, True, loader).newInstance()
    props = jvm.java.util.Properties()
    if db.user:
        props.setProperty("user", db.user)
    if db.password:
        props.setProperty("password", db.password)
    return driver.connect(db.jdbc_url, props)


def execute_jdbc(spark: SparkSession, db: DbConfig, statements: Iterable[str]) -> None:
    """Runs DDL/DML statements (e.g. TRUNCATE/DELETE) in a single transaction."""
    conn = _jdbc_connection(spark, db)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        for sql in statements:
            log.info("Executing on %s: %s", db.database, sql)
            stmt.executeUpdate(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def table_exists(spark: SparkSession, db: DbConfig, table: str) -> bool:
    schema, name = db.qualified(table).split(".", 1)
    conn = _jdbc_connection(spark, db)
    try:
        rs = conn.getMetaData().getTables(db.database, schema, name, None)
        return bool(rs.next())
    finally:
        conn.close()


def read_csv_as_strings(spark: SparkSession, path: str, columns: list[str]) -> DataFrame:
    """Reads a headered CSV with every column as string so casting rules live in the job."""
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return (
        spark.read.option("header", "true")
        .option("encoding", "ISO-8859-15")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def read_excel_as_strings(
    spark: SparkSession,
    path: str,
    columns: list[str],
    sheet: str | None = None,
    engine: str = "pandas",
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss",
) -> DataFrame:
    """Reads an .xlsx sheet into a DataFrame of strings.

    ``engine="crealytics"`` uses the ``com.crealytics.spark.excel`` data source (distributed,
    needs the spark-excel package). ``engine="pandas"`` reads on the driver with openpyxl,
    which is fine for the small Excel feed and avoids the extra JVM dependency. Datetime
    cells are rendered as ``yyyy-MM-dd HH:mm:ss`` strings.
    """
    if engine == "crealytics":
        reader = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("timestampFormat", timestamp_format)
        )
        if sheet:
            reader = reader.option("dataAddress", f"'{sheet}'!A1")
        df = reader.load(path)
        return df.select([F.col(c).cast("string").alias(c) for c in columns])

    if engine != "pandas":
        raise ValueError(f"Unsupported EXCEL_READER {engine!r}; expected 'pandas' or 'crealytics'")

    import pandas as pd

    pdf = pd.read_excel(path, sheet_name=sheet or 0, engine="openpyxl", usecols=columns)
    rows = []
    for record in pdf.to_dict(orient="records"):
        row = []
        for c in columns:
            value = record[c]
            if value is None or (not isinstance(value, str) and pd.isna(value)):
                row.append(None)
            elif isinstance(value, pd.Timestamp):
                row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
            elif isinstance(value, float) and value.is_integer():
                row.append(str(int(value)))
            else:
                row.append(str(value))
        rows.append(row)
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


OUTPUT_DESTINATIONS = ("console", "csv", "jdbc")


def write_output(
    df: DataFrame,
    destination: str,
    path: str | None = None,
    table: str | None = None,
    db: DbConfig | None = None,
    mode: str = "overwrite",
    show_rows: int = 1000,
) -> None:
    """Delivers a report DataFrame to the console, a CSV directory or a JDBC table."""
    if destination == "console":
        df.show(n=show_rows, truncate=False)
    elif destination == "csv":
        if not path:
            raise ValueError("--output-path is required for csv output")
        df.coalesce(1).write.mode(mode).option("header", "true").csv(path)
        log.info("Wrote report CSV to %s", path)
    elif destination == "jdbc":
        if not table or db is None:
            raise ValueError("--output-table is required for jdbc output")
        (
            df.write.format("jdbc")
            .options(**jdbc_options(db))
            .option("dbtable", db.qualified(table))
            .option("truncate", "true")
            .mode(mode)
            .save()
        )
        log.info("Wrote report to %s.%s", db.database, db.qualified(table))
    else:
        raise ValueError(f"Unsupported output {destination!r}; expected one of {OUTPUT_DESTINATIONS}")
