from __future__ import annotations

import datetime as dt
import sys
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
JOBS_ROOT = REPO_ROOT / "databricks" / "jobs"
if str(JOBS_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBS_ROOT))

DATA_SOURCES = REPO_ROOT / "data_sources"


def ts(*args: int) -> dt.datetime:
    """Naive local timestamp, matching Spark's ``TimestampType`` semantics."""
    return dt.datetime(*args)  # noqa: DTZ001


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fact_transaction_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def csv_path() -> str:
    return str(DATA_SOURCES / "transaction_csv.csv")


@pytest.fixture(scope="session")
def excel_path() -> str:
    return str(DATA_SOURCES / "transaction_excel.xlsx")


@pytest.fixture()
def db_df(spark):
    """Stand-in for the bronze SQL Server ``transaction_db`` table.

    TransactionID 6 also exists in the Excel extract, with a different amount,
    so dedupe precedence is observable.
    """
    from fact_transaction.readers import SOURCE_DB, normalize
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("transaction_id", IntegerType()),
            StructField("account_id", IntegerType()),
            StructField("transaction_date", TimestampType()),
            StructField("amount", DecimalType(19, 4)),
            StructField("transaction_type", StringType()),
            StructField("branch_id", IntegerType()),
        ]
    )
    rows = [
        (1, 1, ts(2024, 1, 15, 9, 0), Decimal("250000.0000"), "Deposit", 1),
        (2, 2, ts(2024, 1, 15, 10, 30), Decimal("75000.0000"), "Withdrawal", 2),
        (3, 3, ts(2024, 1, 16, 11, 0), Decimal("120000.0000"), "Payment", 3),
        (4, 4, ts(2024, 1, 16, 12, 0), Decimal("900000.0000"), "Transfer", 4),
        (5, 5, ts(2024, 1, 17, 13, 45), Decimal("60000.0000"), "Deposit", 5),
        (6, 6, ts(2024, 1, 18, 13, 10), Decimal("999999.0000"), "Withdrawal", 1),
    ]
    return normalize(spark.createDataFrame(rows, schema=schema), SOURCE_DB)
