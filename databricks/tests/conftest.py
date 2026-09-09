from __future__ import annotations

import csv
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from banking_dwh.balance_per_customer import MONEY
from banking_dwh.tsql_reference import build_database

FIXTURES = Path(__file__).parent / "fixtures"

CUSTOMER_SCHEMA = StructType(
    [
        StructField("CustomerID", IntegerType()),
        StructField("CustomerName", StringType()),
    ]
)
ACCOUNT_SCHEMA = StructType(
    [
        StructField("AccountID", IntegerType()),
        StructField("CustomerID", IntegerType()),
        StructField("AccountType", StringType()),
        StructField("Balance", MONEY),
        StructField("Status", StringType()),
    ]
)
FACT_SCHEMA = StructType(
    [
        StructField("TransactionID", IntegerType()),
        StructField("AccountID", IntegerType()),
        StructField("Amount", MONEY),
        StructField("TransactionType", StringType()),
    ]
)


def _read_fixture(name: str) -> list[dict]:
    with (FIXTURES / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("balance_per_customer_equivalence")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fixture_rows() -> dict[str, list[dict]]:
    return {
        "customers": _read_fixture("dim_customer.csv"),
        "accounts": _read_fixture("dim_account.csv"),
        "transactions": _read_fixture("fact_transaction.csv"),
    }


@pytest.fixture(scope="session")
def spark_tables(spark, fixture_rows):
    def frame(name: str, schema: StructType, path: str):
        return spark.read.csv(str(FIXTURES / path), header=True, schema=schema)

    return {
        "dim_customer": frame("dim_customer", CUSTOMER_SCHEMA, "dim_customer.csv"),
        "dim_account": frame("dim_account", ACCOUNT_SCHEMA, "dim_account.csv"),
        "fact_transaction": frame("fact_transaction", FACT_SCHEMA, "fact_transaction.csv"),
    }


@pytest.fixture(scope="session")
def sqlite_connection(fixture_rows):
    connection = build_database(
        fixture_rows["customers"], fixture_rows["accounts"], fixture_rows["transactions"]
    )
    yield connection
    connection.close()
