"""Local Spark tests for the DWH analytics (no Databricks cluster required).

Run with: `python -m pytest databricks/tests`
"""

import datetime
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dwh_analytics import balance_per_customer, daily_transaction  # noqa: E402

CATALOG = "spark_catalog"
SCHEMA = "dwh_test"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dwh-analytics-tests")
        .getOrCreate()
    )
    session.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")

    fact = session.createDataFrame(
        [
            (1, 10, datetime.datetime(2024, 1, 18, 9, 0), Decimal("100.0000"), "Deposit", 1),
            (2, 10, datetime.datetime(2024, 1, 18, 15, 0), Decimal("40.0000"), "Withdrawal", 1),
            (3, 11, datetime.datetime(2024, 1, 19, 11, 0), Decimal("200.0000"), "Deposit", 2),
            (4, 12, datetime.datetime(2024, 1, 20, 11, 0), Decimal("300.0000"), "Transfer", 2),
            (5, 10, datetime.datetime(2024, 2, 1, 11, 0), Decimal("999.0000"), "Deposit", 1),
        ],
        "TransactionID int, AccountID int, TransactionDate timestamp, "
        "Amount decimal(19,4), TransactionType string, BranchID int",
    )
    accounts = session.createDataFrame(
        [
            (10, 100, "Savings", Decimal("1000.0000"), datetime.date(2020, 1, 1), "active"),
            (11, 100, "Checking", Decimal("500.0000"), datetime.date(2021, 1, 1), "inactive"),
            (12, 101, "Savings", Decimal("800.0000"), datetime.date(2022, 1, 1), "active"),
            (13, 101, "Checking", Decimal("50.0000"), datetime.date(2023, 1, 1), "active"),
        ],
        "AccountID int, CustomerID int, AccountType string, Balance decimal(19,4), "
        "DateOpened date, Status string",
    )
    customers = session.createDataFrame(
        [
            (100, "ALICE SMITH", "1 Main St", "JAKARTA", "DKI", 30, "F", "a@example.com"),
            (101, "BOB JONES", "2 Main St", "BANDUNG", "JABAR", 40, "M", "b@example.com"),
        ],
        "CustomerID int, CustomerName string, Address string, CityName string, "
        "StateName string, Age int, Gender string, Email string",
    )

    for df, name in [
        (fact, "FactTransaction"),
        (accounts, "DimAccount"),
        (customers, "DimCustomer"),
    ]:
        df.write.mode("overwrite").saveAsTable(f"{SCHEMA}.{name}")

    yield session
    session.stop()


def test_daily_transaction_aggregates_and_filters_by_date(spark):
    rows = daily_transaction(
        spark, "2024-01-18", "2024-01-20", catalog=CATALOG, schema=SCHEMA
    ).collect()

    assert [r["Date"] for r in rows] == [
        datetime.date(2024, 1, 18),
        datetime.date(2024, 1, 19),
        datetime.date(2024, 1, 20),
    ]
    assert [r["TotalTransactions"] for r in rows] == [2, 1, 1]
    assert [r["TotalAmount"] for r in rows] == [
        Decimal("140.0000"),
        Decimal("200.0000"),
        Decimal("300.0000"),
    ]


def test_daily_transaction_default_window_is_trailing_seven_rows(spark):
    rows = daily_transaction(
        spark, "2024-01-18", "2024-01-20", catalog=CATALOG, schema=SCHEMA
    ).collect()

    # Fewer than 7 rows available, so each value is the running mean.
    assert [r["SmoothedTotalAmount"] for r in rows] == [
        Decimal("140.0000"),
        Decimal("170.0000"),
        Decimal("213.3333"),
    ]


def test_daily_transaction_window_size_is_configurable(spark):
    rows = daily_transaction(
        spark, "2024-01-18", "2024-01-20", window_size=2, catalog=CATALOG, schema=SCHEMA
    ).collect()

    assert [r["SmoothedTotalAmount"] for r in rows] == [
        Decimal("140.0000"),
        Decimal("170.0000"),
        Decimal("250.0000"),
    ]


def test_daily_transaction_rejects_invalid_window(spark):
    with pytest.raises(ValueError):
        daily_transaction(spark, "2024-01-18", "2024-01-20", window_size=0)


def test_balance_per_customer_applies_deposit_withdrawal_logic(spark):
    rows = balance_per_customer(spark, "ALICE", catalog=CATALOG, schema=SCHEMA).collect()

    # Only the active account 10: 1000 + 100 (deposit) - 40 (withdrawal) + 999 (deposit)
    assert len(rows) == 1
    assert rows[0]["AccountType"] == "Savings"
    assert rows[0]["InitialBalance"] == Decimal("1000.0000")
    assert rows[0]["CurrentBalance"] == Decimal("2059.0000")


def test_balance_per_customer_handles_accounts_without_transactions(spark):
    rows = {
        r["AccountType"]: r
        for r in balance_per_customer(spark, "BOB", catalog=CATALOG, schema=SCHEMA).collect()
    }

    assert rows["Savings"]["CurrentBalance"] == Decimal("500.0000")  # 800 - 300 transfer
    assert rows["Checking"]["CurrentBalance"] == Decimal("50.0000")  # no transactions
