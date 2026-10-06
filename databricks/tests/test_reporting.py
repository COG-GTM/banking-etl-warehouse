import datetime as dt
from decimal import Decimal

import pytest

from analytics.reporting import balance_per_customer, daily_transaction

SCHEMA = "dwh_reporting_test"


@pytest.fixture(scope="module", autouse=True)
def dwh_tables(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    ts = dt.datetime
    spark.createDataFrame(
        [
            (1, "SHELLY JUWITA"),
            (2, "BOBI RINALDO"),
            (3, "ADAM MALIK"),
        ],
        "CustomerID int, CustomerName string",
    ).write.mode("overwrite").saveAsTable(f"{SCHEMA}.dim_customer")
    spark.createDataFrame(
        [
            (1, 1, "saving", Decimal("1500000"), "active"),
            (3, 1, "checking", Decimal("25000000"), "active"),
            (2, 2, "saving", Decimal("500000"), "active"),
            (4, 3, "checking", Decimal("4500000"), "terminated"),
            (9, 1, "saving", Decimal("100"), "active"),
        ],
        "AccountID int, CustomerID int, AccountType string, Balance decimal(19,4), Status string",
    ).write.mode("overwrite").saveAsTable(f"{SCHEMA}.dim_account")
    spark.createDataFrame(
        [
            (1, 1, ts(2024, 1, 18, 9, 0), Decimal("100000"), "Deposit"),
            (2, 3, ts(2024, 1, 18, 23, 59), Decimal("10000000"), "Transfer"),
            (3, 3, ts(2024, 1, 19, 10, 0), Decimal("1000000"), "Withdrawal"),
            (4, 3, ts(2024, 1, 20, 23, 30), Decimal("2000000"), "Deposit"),
            (5, 2, ts(2024, 1, 21, 0, 0), Decimal("50000"), "Payment"),
            (6, 4, ts(2024, 1, 17, 12, 0), Decimal("700"), "Deposit"),
        ],
        "TransactionID int, AccountID int, TransactionDate timestamp, Amount decimal(19,4), TransactionType string",
    ).write.mode("overwrite").saveAsTable(f"{SCHEMA}.fact_transaction")


def test_daily_transaction_inclusive_date_range(spark):
    rows = daily_transaction("2024-01-18", "2024-01-20", spark=spark, schema=SCHEMA).collect()
    assert [(r.Date, r.TotalTransactions, r.TotalAmount) for r in rows] == [
        (dt.date(2024, 1, 18), 2, Decimal("10100000")),
        (dt.date(2024, 1, 19), 1, Decimal("1000000")),
        (dt.date(2024, 1, 20), 1, Decimal("2000000")),
    ]
    assert daily_transaction("2024-01-18", "2024-01-20", spark=spark, schema=SCHEMA).columns == [
        "Date",
        "TotalTransactions",
        "TotalAmount",
    ]


def test_daily_transaction_accepts_date_objects(spark):
    rows = daily_transaction(dt.date(2024, 1, 21), dt.date(2024, 1, 21), spark=spark, schema=SCHEMA)
    assert [(r.Date, r.TotalTransactions) for r in rows.collect()] == [(dt.date(2024, 1, 21), 1)]


def test_daily_transaction_empty_range(spark):
    assert daily_transaction("2030-01-01", "2030-01-31", spark=spark, schema=SCHEMA).count() == 0


def test_balance_per_customer_signed_amounts_and_active_only(spark):
    df = balance_per_customer("shelly", spark=spark, schema=SCHEMA)
    assert df.columns == ["CustomerName", "AccountType", "InitialBalance", "CurrentBalance"]
    got = sorted((r.AccountType, r.InitialBalance, r.CurrentBalance) for r in df.collect())
    assert got == [
        ("checking", Decimal("25000000"), Decimal("16000000")),  # 25M - 10M - 1M + 2M
        ("saving", Decimal("100"), Decimal("100")),  # no transactions -> ISNULL(..., 0)
        ("saving", Decimal("1500000"), Decimal("1600000")),
    ]


def test_balance_per_customer_partial_match(spark):
    rows = balance_per_customer("RINAL", spark=spark, schema=SCHEMA).collect()
    assert [(r.CustomerName, r.CurrentBalance) for r in rows] == [("BOBI RINALDO", Decimal("450000"))]


def test_balance_per_customer_excludes_terminated_accounts(spark):
    assert balance_per_customer("adam", spark=spark, schema=SCHEMA).count() == 0
