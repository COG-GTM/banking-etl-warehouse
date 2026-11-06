import datetime as dt
from decimal import Decimal

from pyspark_jobs.reports.balance_per_customer import balance_per_customer
from pyspark_jobs.reports.daily_transaction import daily_transaction

FACT_SCHEMA = "TransactionID int, AccountID int, TransactionDate timestamp, Amount decimal(19,4), TransactionType string, BranchID int"


def _fact(spark):
    return spark.createDataFrame(
        [
            (1, 1, dt.datetime(2024, 1, 18, 9, 0), Decimal("100"), "Deposit", 1),
            (2, 1, dt.datetime(2024, 1, 18, 23, 59, 59), Decimal("30"), "Withdrawal", 1),
            (3, 2, dt.datetime(2024, 1, 20, 0, 0), Decimal("50"), "Transfer", 1),
            (4, 2, dt.datetime(2024, 1, 21, 0, 0), Decimal("5"), "Payment", 1),
            (5, 3, dt.datetime(2024, 1, 17, 12, 0), Decimal("7"), "Deposit", 1),
        ],
        FACT_SCHEMA,
    )


def test_daily_transaction_is_inclusive_and_ordered(spark):
    out = daily_transaction(_fact(spark), dt.date(2024, 1, 18), dt.date(2024, 1, 20))
    assert out.columns == ["Date", "TotalTransactions", "TotalAmount"]
    assert [tuple(r) for r in out.collect()] == [
        (dt.date(2024, 1, 18), 2, Decimal("130.0000")),
        (dt.date(2024, 1, 20), 1, Decimal("50.0000")),
    ]


def test_balance_per_customer(spark):
    customers = spark.createDataFrame(
        [(1, "SHELLY JUWITA"), (2, "BOBI RINALDO")], "CustomerID int, CustomerName string"
    )
    accounts = spark.createDataFrame(
        [
            (1, 1, "saving", Decimal("1000"), "active"),
            (2, 1, "checking", Decimal("500"), "active"),
            (3, 1, "saving", Decimal("900"), "terminated"),
            (4, 1, "checking", Decimal("200"), "active"),
            (5, 2, "saving", Decimal("10"), "active"),
        ],
        "AccountID int, CustomerID int, AccountType string, Balance decimal(19,4), Status string",
    )
    out = balance_per_customer(_fact(spark), customers, accounts, "shelly")
    assert out.columns == ["CustomerName", "AccountType", "InitialBalance", "CurrentBalance"]
    rows = sorted((r.AccountType, r.InitialBalance, r.CurrentBalance) for r in out.collect())
    assert rows == [
        # account 2: -(50) transfer -(5) payment
        ("checking", Decimal("200.0000"), Decimal("200.0000")),  # account 4: no transactions -> ISNULL(..., 0)
        ("checking", Decimal("500.0000"), Decimal("445.0000")),
        # account 1: +100 deposit -30 withdrawal
        ("saving", Decimal("1000.0000"), Decimal("1070.0000")),
    ]

    assert balance_per_customer(_fact(spark), customers, accounts, "shelly", case_sensitive=True).count() == 0
    assert balance_per_customer(_fact(spark), customers, accounts, "SHELLY", case_sensitive=True).count() == 3
