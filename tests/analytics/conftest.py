from __future__ import annotations

import datetime as dt
import sys
from dataclasses import dataclass
from decimal import Decimal
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

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MONEY = DecimalType(19, 4)
UTC = dt.timezone.utc


@dataclass(frozen=True)
class Customer:
    CustomerID: int
    CustomerName: str


@dataclass(frozen=True)
class Account:
    AccountID: int
    CustomerID: int
    AccountType: str
    Balance: Decimal
    Status: str


@dataclass(frozen=True)
class Txn:
    TransactionID: int
    AccountID: int
    TransactionDate: dt.datetime
    Amount: Decimal
    TransactionType: str
    BranchID: int


def D(v: str) -> Decimal:
    return Decimal(v)


CUSTOMERS = [
    Customer(1, "JOHN SMITH"),
    Customer(2, "Jane Smithson"),
    Customer(3, "ALICE JONES"),
    Customer(4, "Bob 100% Sure"),
]

ACCOUNTS = [
    Account(10, 1, "SAVINGS", D("1000.00"), "active"),
    Account(11, 1, "CHECKING", D("250.50"), "ACTIVE"),  # active but upper-case
    Account(12, 1, "LOAN", D("-5000.00"), "inactive"),  # excluded
    Account(13, 2, "SAVINGS", D("300.00"), "Active"),  # no transactions
    Account(14, 3, "SAVINGS", D("42.00"), "active"),
    Account(15, 4, "SAVINGS", D("1.00"), "active"),
    Account(16, 2, "CHECKING", D("99.00"), "closed"),
]

TRANSACTIONS = [
    Txn(
        1, 10, dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC), D("100.00"), "Deposit", 1
    ),  # start boundary
    Txn(
        2,
        10,
        dt.datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC),
        D("40.25"),
        "Withdrawal",
        1,
    ),
    Txn(
        3,
        10,
        dt.datetime(2024, 1, 31, 23, 59, 59, tzinfo=UTC),
        D("10.00"),
        "Transfer",
        2,
    ),  # end boundary
    Txn(
        4, 11, dt.datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC), D("500.00"), "Deposit", 1
    ),  # just outside
    Txn(
        5,
        11,
        dt.datetime(2023, 12, 31, 23, 59, 59, tzinfo=UTC),
        D("5.00"),
        "Withdrawal",
        1,
    ),  # just before
    Txn(
        6, 12, dt.datetime(2024, 1, 10, 9, 0, 0, tzinfo=UTC), D("1000.00"), "Deposit", 1
    ),  # inactive acct
    Txn(
        7, 14, dt.datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC), D("2.50"), "deposit", 1
    ),  # lower-case type
    Txn(
        8, 14, dt.datetime(2024, 1, 15, 9, 0, 0, tzinfo=UTC), D("1.25"), "Withdrawal", 1
    ),
    Txn(
        9, 15, dt.datetime(2024, 1, 20, 0, 0, 0, tzinfo=UTC), D("0.0001"), "Deposit", 1
    ),
    Txn(
        10, 16, dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=UTC), D("77.00"), "Deposit", 1
    ),  # closed acct
]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("analytics-parity-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def customer_df(spark):
    schema = StructType(
        [
            StructField("CustomerID", IntegerType(), False),
            StructField("CustomerName", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("CityName", StringType(), True),
            StructField("StateName", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("Email", StringType(), True),
        ]
    )
    rows = [
        (c.CustomerID, c.CustomerName, None, None, None, None, None, None)
        for c in CUSTOMERS
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture(scope="session")
def account_df(spark):
    schema = StructType(
        [
            StructField("AccountID", IntegerType(), False),
            StructField("CustomerID", IntegerType(), True),
            StructField("AccountType", StringType(), True),
            StructField("Balance", MONEY, True),
            StructField("DateOpened", DateType(), True),
            StructField("Status", StringType(), True),
        ]
    )
    rows = [
        (
            a.AccountID,
            a.CustomerID,
            a.AccountType,
            a.Balance,
            dt.date(2020, 1, 1),
            a.Status,
        )
        for a in ACCOUNTS
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture(scope="session")
def fact_df(spark):
    schema = StructType(
        [
            StructField("TransactionID", IntegerType(), False),
            StructField("AccountID", IntegerType(), True),
            StructField("TransactionDate", TimestampType(), True),
            StructField("Amount", MONEY, True),
            StructField("TransactionType", StringType(), True),
            StructField("BranchID", IntegerType(), True),
        ]
    )
    rows = [
        (
            t.TransactionID,
            t.AccountID,
            t.TransactionDate,
            t.Amount,
            t.TransactionType,
            t.BranchID,
        )
        for t in TRANSACTIONS
    ]
    return spark.createDataFrame(rows, schema)
