"""Shared fixtures for the analytics unit tests.

The analytics modules live under ``databricks/analytics`` in Databricks notebook source
format, so they are loaded by path rather than imported as a package (``databricks`` is
also a PyPI namespace and the directory is not a Python package).
"""

from __future__ import annotations

import datetime
import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ANALYTICS_DIR = Path(__file__).resolve().parents[2] / "databricks" / "analytics"

FACT_TRANSACTION_VIEW = "fact_transaction"
DIM_CUSTOMER_VIEW = "dim_customer"
DIM_ACCOUNT_VIEW = "dim_account"

MONEY = DecimalType(19, 4)


def _load_module(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ANALYTICS_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def daily_transaction_module() -> ModuleType:
    return _load_module("daily_transaction")


@pytest.fixture(scope="session")
def balance_per_customer_module() -> ModuleType:
    return _load_module("balance_per_customer")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("analytics-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _ts(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


@pytest.fixture(scope="session")
def fixtures(spark: SparkSession) -> None:
    """Small in-memory star schema registered as temp views.

    Accounts:
      1  Alice   savings  active    balance 1000, deposits 250 - withdrawal 100 => 1150
      2  Alice   checking active    balance  500, no transactions               =>  500 (coalesce path)
      3  Bob     savings  inactive  balance  700, deposit 50                    => filtered out
      4  ALICIA  savings  ACTIVE    balance  100, withdrawal 25                 =>   75
    """
    transaction_schema = StructType(
        [
            StructField("transaction_id", IntegerType(), False),
            StructField("account_id", IntegerType(), True),
            StructField("transaction_date", TimestampType(), True),
            StructField("amount", MONEY, True),
            StructField("transaction_type", StringType(), True),
            StructField("branch_id", IntegerType(), True),
        ]
    )
    transactions = [
        (1, 1, _ts("2024-01-01T09:30:00"), Decimal("100.0000"), "Deposit", 1),
        (2, 1, _ts("2024-01-01T18:00:00"), Decimal("150.0000"), "Deposit", 1),
        (3, 1, _ts("2024-01-02T10:00:00"), Decimal("100.0000"), "Withdrawal", 1),
        (4, 3, _ts("2024-01-03T11:00:00"), Decimal("50.0000"), "Deposit", 2),
        (5, 4, _ts("2024-01-05T12:00:00"), Decimal("25.0000"), "Withdrawal", 2),
    ]
    spark.createDataFrame(transactions, transaction_schema).createOrReplaceTempView(
        FACT_TRANSACTION_VIEW
    )

    customer_schema = StructType(
        [
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), True),
        ]
    )
    customers = [(10, "Alice Smith"), (20, "Bob Jones"), (30, "ALICIA Brown")]
    spark.createDataFrame(customers, customer_schema).createOrReplaceTempView(DIM_CUSTOMER_VIEW)

    account_schema = StructType(
        [
            StructField("account_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("account_type", StringType(), True),
            StructField("balance", MONEY, True),
            StructField("status", StringType(), True),
        ]
    )
    accounts = [
        (1, 10, "savings", Decimal("1000.0000"), "active"),
        (2, 10, "checking", Decimal("500.0000"), "active"),
        (3, 20, "savings", Decimal("700.0000"), "inactive"),
        (4, 30, "savings", Decimal("100.0000"), "ACTIVE"),
    ]
    spark.createDataFrame(accounts, account_schema).createOrReplaceTempView(DIM_ACCOUNT_VIEW)
