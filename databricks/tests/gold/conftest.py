from __future__ import annotations

import datetime as dt
import decimal
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BRONZE_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType()),
        StructField("account_id", IntegerType()),
        StructField("transaction_date", StringType()),
        StructField("amount", StringType()),
        StructField("transaction_type", StringType()),
        StructField("branch_id", IntegerType()),
        StructField("_ingest_ts", TimestampType()),
        StructField("_source_file", StringType()),
    ]
)

DIM_ACCOUNT_SCHEMA = StructType(
    [
        StructField("account_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("account_type", StringType()),
        StructField("balance", DecimalType(19, 4)),
        StructField("date_opened", DateType()),
        StructField("status", StringType()),
    ]
)

DIM_BRANCH_SCHEMA = StructType(
    [
        StructField("branch_id", IntegerType()),
        StructField("branch_name", StringType()),
        StructField("branch_location", StringType()),
    ]
)

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType()),
        StructField("customer_name", StringType()),
        StructField("address", StringType()),
        StructField("city_name", StringType()),
        StructField("state_name", StringType()),
        StructField("age", IntegerType()),
        StructField("gender", StringType()),
        StructField("email", StringType()),
    ]
)


def ts(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def dec(value: str) -> decimal.Decimal:
    return decimal.Decimal(value)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gold-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def mssql_source(spark: SparkSession):
    """`transaction_db` extract: DATETIME2 already parsed, ingest at 08:00."""
    rows = [
        (1, 11, "21-01-2024 14:00:00", "1500000.00", "Deposit", 4, ts("2024-02-01 08:00:00"), "sample.bak"),
        (2, 12, "21-01-2024 08:00:00", "500000.50", "Transfer", 3, ts("2024-02-01 08:00:00"), "sample.bak"),
        # 3 also arrives from excel and csv with a different amount
        (3, 13, "22-01-2024 09:00:00", "100000.00", "Deposit", 1, ts("2024-02-01 08:00:00"), "sample.bak"),
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


@pytest.fixture()
def excel_source(spark: SparkSession):
    rows = [
        (3, 13, "22-01-2024 09:00:00", "999999.00", "Deposit", 1, ts("2024-02-01 09:00:00"), "transaction_excel.xlsx"),
        (4, 14, "22-01-2024 13:10:00", "100000.25", "Withdrawal", 5, ts("2024-02-01 09:00:00"), "transaction_excel.xlsx"),
        # orphan branch_id 99
        (5, 14, "23-01-2024 10:00:00", "250000.00", "Deposit", 99, ts("2024-02-01 09:00:00"), "transaction_excel.xlsx"),
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


@pytest.fixture()
def csv_source(spark: SparkSession):
    rows = [
        (3, 13, "22-01-2024 09:00:00", "111111.00", "Deposit", 1, ts("2024-02-01 10:00:00"), "transaction_csv.csv"),
        (6, 15, "24-01-2024 16:45:30", "75000.75", "Withdrawal", 3, ts("2024-02-01 10:00:00"), "transaction_csv.csv"),
        # orphan account_id 404, NULL account_id is allowed (T-SQL FKs allow NULL)
        (7, 404, "24-01-2024 17:00:00", "10000.00", "Deposit", 3, ts("2024-02-01 10:00:00"), "transaction_csv.csv"),
        (8, None, "24-01-2024 18:00:00", "20000.00", "Deposit", None, ts("2024-02-01 10:00:00"), "transaction_csv.csv"),
        # unparseable date -> NULL timestamp, row is kept (Talend "die on error" unchecked)
        (9, 15, "not-a-date", "30000.00", "Transfer", 3, ts("2024-02-01 10:00:00"), "transaction_csv.csv"),
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


@pytest.fixture()
def dim_account(spark: SparkSession):
    rows = [
        (11, 101, "Savings", dec("1000000.0000"), dt.date(2020, 1, 1), "active"),
        (12, 101, "Checking", dec("500000.0000"), dt.date(2021, 5, 4), "active"),
        (13, 102, "Savings", dec("250000.0000"), dt.date(2019, 7, 9), "active"),
        (14, 103, "Savings", dec("300000.0000"), dt.date(2022, 3, 3), "inactive"),
        (15, 103, "Checking", dec("100000.0000"), dt.date(2023, 8, 8), "active"),
    ]
    return spark.createDataFrame(rows, DIM_ACCOUNT_SCHEMA)


@pytest.fixture()
def dim_branch(spark: SparkSession):
    rows = [
        (1, "Sudirman", "Jakarta"),
        (3, "Braga", "Bandung"),
        (4, "Tunjungan", "Surabaya"),
        (5, "Malioboro", "Yogyakarta"),
    ]
    return spark.createDataFrame(rows, DIM_BRANCH_SCHEMA)


@pytest.fixture()
def dim_customer(spark: SparkSession):
    rows = [
        (101, "BUDI SANTOSO", "Jl. Merdeka 1", "JAKARTA", "DKI JAKARTA", 35, "M", "budi@example.com"),
        (102, "SITI RAHAYU", "Jl. Asia Afrika 2", "BANDUNG", "JAWA BARAT", 41, "F", "siti@example.com"),
        (103, "AGUS BUDIMAN", "Jl. Pahlawan 3", "SURABAYA", "JAWA TIMUR", 29, "M", "agus@example.com"),
    ]
    return spark.createDataFrame(rows, DIM_CUSTOMER_SCHEMA)
