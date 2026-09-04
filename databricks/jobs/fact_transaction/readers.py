"""Source readers for the FactTransaction pipeline.

Each reader returns a DataFrame conforming to :data:`NORMALIZED_SCHEMA` so the
three streams can be unioned the way the Talend ``tUnite_1`` component did.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SOURCE_DB = "db"
SOURCE_EXCEL = "excel"
SOURCE_CSV = "csv"

# Timestamp pattern used by the Talend tFileInputDelimited / tFileInputExcel
# schemas ("dd-MM-yyyy HH:mm:ss").
FILE_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"

# Raw column names shared by all three sources (SQL Server transaction_db,
# transaction_excel.xlsx and transaction_csv.csv all use these headers).
RAW_COLUMNS = (
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
)

RAW_CSV_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), nullable=False),
        StructField("account_id", IntegerType(), nullable=True),
        StructField("transaction_date", StringType(), nullable=True),
        StructField("amount", DecimalType(19, 4), nullable=True),
        StructField("transaction_type", StringType(), nullable=True),
        StructField("branch_id", IntegerType(), nullable=True),
    ]
)

NORMALIZED_SCHEMA = StructType(
    [
        StructField("TransactionID", IntegerType(), nullable=True),
        StructField("AccountID", IntegerType(), nullable=True),
        StructField("TransactionDate", TimestampType(), nullable=True),
        StructField("Amount", DecimalType(19, 4), nullable=True),
        StructField("TransactionType", StringType(), nullable=True),
        StructField("BranchID", IntegerType(), nullable=True),
        StructField("_source", StringType(), nullable=True),
    ]
)


def normalize(df: DataFrame, source: str, timestamp_format: str | None = None) -> DataFrame:
    """Cast a raw source DataFrame to :data:`NORMALIZED_SCHEMA`.

    ``timestamp_format`` is applied when ``transaction_date`` arrives as a
    string; otherwise the column is cast directly to a timestamp.
    """
    date_col = F.col("transaction_date")
    if timestamp_format is not None and isinstance(
        df.schema["transaction_date"].dataType, StringType
    ):
        date_col = F.to_timestamp(date_col, timestamp_format)

    return df.select(
        F.col("transaction_id").cast(IntegerType()).alias("TransactionID"),
        F.col("account_id").cast(IntegerType()).alias("AccountID"),
        date_col.cast(TimestampType()).alias("TransactionDate"),
        F.col("amount").cast(DecimalType(19, 4)).alias("Amount"),
        F.col("transaction_type").cast(StringType()).alias("TransactionType"),
        F.col("branch_id").cast(IntegerType()).alias("BranchID"),
        F.lit(source).alias("_source"),
    )


def read_csv_transactions(
    spark: SparkSession,
    path: str,
    timestamp_format: str = FILE_TIMESTAMP_FORMAT,
) -> DataFrame:
    """Read ``transaction_csv.csv`` (Talend ``tFileInputDelimited_1``)."""
    raw = (
        spark.read.option("header", True)
        .option("sep", ",")
        .option("mode", "PERMISSIVE")
        .schema(RAW_CSV_SCHEMA)
        .csv(path)
    )
    return normalize(raw, SOURCE_CSV, timestamp_format)


def read_excel_transactions(
    spark: SparkSession,
    path: str,
    sheet: str = "Sheet1",
    use_spark_excel: bool = False,
    timestamp_format: str = FILE_TIMESTAMP_FORMAT,
) -> DataFrame:
    """Read ``transaction_excel.xlsx`` (Talend ``tFileInputExcel_1``).

    Defaults to a pandas + openpyxl round-trip so the job runs anywhere. Set
    ``use_spark_excel`` to route through the ``com.crealytics.spark.excel``
    datasource when that jar is available on the cluster.
    """
    if use_spark_excel:
        raw = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", True)
            .option("inferSchema", True)
            .option("dataAddress", f"'{sheet}'!A1")
            .load(path)
        )
        raw = raw.select([F.col(c).alias(c.strip().lower()) for c in raw.columns])
        return normalize(raw, SOURCE_EXCEL, timestamp_format)

    pdf = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    pdf.columns = [str(c).strip().lower() for c in pdf.columns]
    pdf = pdf[list(RAW_COLUMNS)]
    pdf["transaction_date"] = pd.to_datetime(pdf["transaction_date"], errors="coerce")
    pdf = pdf.astype(
        {
            "transaction_id": "Int64",
            "account_id": "Int64",
            "branch_id": "Int64",
            "transaction_type": "string",
        }
    )
    records: list[dict[str, Any]] = [
        {
            "transaction_id": None if pd.isna(r.transaction_id) else int(r.transaction_id),
            "account_id": None if pd.isna(r.account_id) else int(r.account_id),
            "transaction_date": None if pd.isna(r.transaction_date) else r.transaction_date.to_pydatetime(),
            "amount": None if pd.isna(r.amount) else Decimal(str(r.amount)),
            "transaction_type": None if pd.isna(r.transaction_type) else str(r.transaction_type),
            "branch_id": None if pd.isna(r.branch_id) else int(r.branch_id),
        }
        for r in pdf.itertuples(index=False)
    ]
    raw_schema = StructType(
        [
            StructField("transaction_id", IntegerType(), nullable=True),
            StructField("account_id", IntegerType(), nullable=True),
            StructField("transaction_date", TimestampType(), nullable=True),
            StructField("amount", DecimalType(19, 4), nullable=True),
            StructField("transaction_type", StringType(), nullable=True),
            StructField("branch_id", IntegerType(), nullable=True),
        ]
    )
    raw = spark.createDataFrame(records, schema=raw_schema)
    return normalize(raw, SOURCE_EXCEL, timestamp_format)


def read_table_transactions(
    spark: SparkSession,
    table: str,
    timestamp_format: str = FILE_TIMESTAMP_FORMAT,
) -> DataFrame:
    """Read the bronze transaction table (Talend ``tMSSqlInput_1``).

    ``table`` is a fully qualified Unity Catalog name, e.g.
    ``main.bronze.transaction_db``. The Talend job selected exactly
    :data:`RAW_COLUMNS` from ``dbo.transaction_db``.
    """
    raw = spark.table(table).select(*[F.col(c) for c in RAW_COLUMNS])
    return normalize(raw, SOURCE_DB, timestamp_format)
