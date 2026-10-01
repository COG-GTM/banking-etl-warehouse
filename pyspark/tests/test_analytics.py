from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from banking_etl import schemas
from banking_etl.analytics import balance_per_customer, daily_transaction


@pytest.fixture()
def fact(spark: SparkSession) -> DataFrame:
    rows = [
        (1, 1, dt.datetime(2024, 1, 18, 9, 0), Decimal(100), "Deposit", 1),
        (2, 1, dt.datetime(2024, 1, 18, 23, 59, 59), Decimal(30), "Withdrawal", 1),
        (3, 2, dt.datetime(2024, 1, 19, 0, 0), Decimal(50), "deposit ", 1),
        (4, 3, dt.datetime(2024, 1, 20, 12, 0), Decimal(10), "Payment", 1),
        (5, 3, dt.datetime(2024, 1, 21, 12, 0), Decimal(5), "Transfer", 1),
    ]
    return spark.createDataFrame(rows, schemas.FACT_TRANSACTION.struct)


@pytest.fixture()
def dim_account(spark: SparkSession) -> DataFrame:
    rows = [
        (1, 1, "saving", Decimal(1000), dt.date(2020, 1, 1), "active"),
        (2, 1, "checking", Decimal(500), dt.date(2020, 1, 1), "Active"),
        (3, 2, "saving", Decimal(200), dt.date(2020, 1, 1), "terminated"),
        (4, 1, "deposit", Decimal(70), dt.date(2020, 1, 1), "active"),
    ]
    return spark.createDataFrame(rows, schemas.DIM_ACCOUNT.struct)


@pytest.fixture()
def dim_customer(spark: SparkSession) -> DataFrame:
    rows = [
        (1, "SHELLY JUWITA", "A", "C", "S", 25, "FEMALE", "s@x"),
        (2, "BOBI RINALDO", "B", "C", "S", 31, "MALE", "b@x"),
    ]
    return spark.createDataFrame(rows, schemas.DIM_CUSTOMER.struct)


def test_daily_transaction_groups_inclusive_range(fact: DataFrame) -> None:
    out = daily_transaction(fact, "2024-01-18", dt.date(2024, 1, 20)).collect()
    assert [r.asDict() for r in out] == [
        {"Date": dt.date(2024, 1, 18), "TotalTransactions": 2, "TotalAmount": Decimal("130.0000")},
        {"Date": dt.date(2024, 1, 19), "TotalTransactions": 1, "TotalAmount": Decimal("50.0000")},
        {"Date": dt.date(2024, 1, 20), "TotalTransactions": 1, "TotalAmount": Decimal("10.0000")},
    ]


def test_daily_transaction_empty_range(fact: DataFrame) -> None:
    assert daily_transaction(fact, "2023-01-01", "2023-12-31").count() == 0


def test_balance_per_customer(fact: DataFrame, dim_account: DataFrame, dim_customer: DataFrame) -> None:
    out = balance_per_customer(fact, dim_account, dim_customer, "shelly").collect()
    assert [r.asDict() for r in out] == [
        # account 2: 500 + 50 ("deposit " matches case/trailing-space insensitively)
        {
            "CustomerName": "SHELLY JUWITA",
            "AccountType": "checking",
            "InitialBalance": Decimal("500.0000"),
            "CurrentBalance": Decimal("550.0000"),
        },
        # account 4: no transactions -> ISNULL(..., 0)
        {
            "CustomerName": "SHELLY JUWITA",
            "AccountType": "deposit",
            "InitialBalance": Decimal("70.0000"),
            "CurrentBalance": Decimal("70.0000"),
        },
        # account 1: 1000 + 100 - 30
        {
            "CustomerName": "SHELLY JUWITA",
            "AccountType": "saving",
            "InitialBalance": Decimal("1000.0000"),
            "CurrentBalance": Decimal("1070.0000"),
        },
    ]


def test_balance_per_customer_filters_inactive_and_supports_like_wildcards(
    fact: DataFrame, dim_account: DataFrame, dim_customer: DataFrame
) -> None:
    assert balance_per_customer(fact, dim_account, dim_customer, "Bobi").count() == 0
    assert balance_per_customer(fact, dim_account, dim_customer, "sh_lly%wita").count() == 3
    assert balance_per_customer(fact, dim_account, dim_customer, "nobody").count() == 0
