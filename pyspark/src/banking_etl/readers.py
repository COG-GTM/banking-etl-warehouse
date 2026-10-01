"""Source readers replacing the Talend tMSSqlInput, tFileInputExcel and
tFileInputDelimited components."""

from __future__ import annotations

import datetime as dt
import logging
from io import BytesIO
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from banking_etl.config import CsvSource, ExcelSource, SqlServerSource

log = logging.getLogger(__name__)

SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
SPARK_EXCEL_FORMAT = "com.crealytics.spark.excel"
ISO_TIMESTAMP = "yyyy-MM-dd HH:mm:ss"


def conform(df: DataFrame, schema: StructType) -> DataFrame:
    """Select ``schema``'s columns in order, cast to their declared types."""
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def sqlserver_query(source: SqlServerSource, table: str, schema: StructType) -> str:
    columns = ",\n       ".join(f"[{table}].[{f.name}]" for f in schema.fields)
    return f"SELECT {columns}\nFROM [{source.db_schema}].[{table}]"


def read_sqlserver_table(
    spark: SparkSession, source: SqlServerSource, table: str, schema: StructType
) -> DataFrame:
    user, password = source.credentials()
    reader = (
        spark.read.format("jdbc")
        .option("url", source.jdbc_url)
        .option("driver", SQLSERVER_DRIVER)
        .option("query", sqlserver_query(source, table, schema))
        .option("fetchsize", str(source.fetch_size))
    )
    if user:
        reader = reader.option("user", user).option("password", password)
    log.info("Reading SQL Server table %s.%s", source.db_schema, table)
    return conform(reader.load(), schema)


def read_transaction_csv(spark: SparkSession, source: CsvSource, schema: StructType) -> DataFrame:
    log.info("Reading CSV %s", source.path)
    df = (
        spark.read.schema(schema)
        .option("header", "true")
        .option("sep", source.delimiter)
        .option("encoding", source.encoding)
        .option("timestampFormat", source.timestamp_format)
        .option("mode", source.mode)
        .csv(source.path)
    )
    return _drop_empty_rows(df)


def read_transaction_excel(spark: SparkSession, source: ExcelSource, schema: StructType) -> DataFrame:
    log.info("Reading Excel %s (sheet=%s, engine=%s)", source.path, source.sheet, source.engine)
    if source.engine == "spark_excel":
        df = (
            spark.read.format(SPARK_EXCEL_FORMAT)
            .option("header", "true")
            .option("dataAddress", f"'{source.sheet}'!A1")
            .option("timestampFormat", source.timestamp_format)
            .schema(schema)
            .load(source.path)
        )
    else:
        df = _read_excel_with_pandas(spark, source, schema)
    return _drop_empty_rows(conform(df, schema))


def _read_excel_with_pandas(spark: SparkSession, source: ExcelSource, schema: StructType) -> DataFrame:
    """Load the workbook bytes through Spark (local, s3a://, dbfs:/, /Volumes) and parse
    them with pandas/openpyxl on the driver; workbooks are small landing files."""
    content = spark.read.format("binaryFile").load(source.path).select("content").head()
    if content is None:
        raise FileNotFoundError(source.path)
    pdf = pd.read_excel(BytesIO(bytes(content[0])), sheet_name=source.sheet, dtype=object)
    names = [f.name for f in schema.fields]
    missing = sorted(set(names) - set(pdf.columns))
    if missing:
        raise ValueError(f"{source.path} sheet {source.sheet!r} is missing columns {missing}")
    rows = [tuple(_cell_to_str(v) for v in record) for record in pdf[names].itertuples(index=False)]
    raw = spark.createDataFrame(rows, StructType([StructField(n, StringType()) for n in names]))
    return _parse_string_timestamps(raw, schema, source.timestamp_format)


def _cell_to_str(value: Any) -> str | None:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def _parse_string_timestamps(df: DataFrame, schema: StructType, fmt: str) -> DataFrame:
    """Native Excel dates arrive as ISO strings; text cells are parsed with ``fmt``."""
    types = dict(df.dtypes)
    for field in schema.fields:
        if isinstance(field.dataType, TimestampType) and types.get(field.name) == StringType().simpleString():
            col = F.col(field.name)
            df = df.withColumn(
                field.name,
                F.coalesce(
                    F.try_to_timestamp(col, F.lit(fmt)), F.try_to_timestamp(col, F.lit(ISO_TIMESTAMP))
                ),
            )
    return df


def _drop_empty_rows(df: DataFrame) -> DataFrame:
    """Mirror Talend REMOVE_EMPTY_ROW=true."""
    return df.dropna(how="all")
