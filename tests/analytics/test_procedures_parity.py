"""Parity tests: PySpark procedures vs. a plain-Python re-implementation of the T-SQL."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from decimal import Decimal

import pytest

from databricks.analytics.procedures import balance_per_customer, daily_transaction
from tests.analytics.conftest import ACCOUNTS, CUSTOMERS, TRANSACTIONS

Q = Decimal("0.0001")


def expected_daily(start: dt.date, end: dt.date) -> list[tuple]:
    buckets: dict[dt.date, list[Decimal]] = defaultdict(list)
    for t in TRANSACTIONS:
        d = t.TransactionDate.date()
        if start <= d <= end:
            buckets[d].append(t.Amount)
    return [(d, len(v), sum(v).quantize(Q)) for d, v in sorted(buckets.items())]


def expected_balance(name: str) -> set[tuple]:
    net: dict[int, Decimal] = defaultdict(Decimal)
    for t in TRANSACTIONS:
        net[t.AccountID] += (
            t.Amount if t.TransactionType.lower() == "deposit" else -t.Amount
        )
    out = set()
    for c in CUSTOMERS:
        if name.lower() not in c.CustomerName.lower():
            continue
        for a in ACCOUNTS:
            if a.CustomerID != c.CustomerID or a.Status.lower() != "active":
                continue
            current = (a.Balance + net.get(a.AccountID, Decimal(0))).quantize(Q)
            out.add((c.CustomerName, a.AccountType, a.Balance.quantize(Q), current))
    return out


def rows(df) -> list[tuple]:
    return [tuple(r) for r in df.collect()]


# --------------------------------------------------------------------------- daily_transaction


def test_daily_transaction_schema(fact_df):
    df = daily_transaction(fact_df, "2024-01-01", "2024-01-31")
    assert df.columns == ["Date", "TotalTransactions", "TotalAmount"]
    types = dict(df.dtypes)
    assert types == {
        "Date": "date",
        "TotalTransactions": "bigint",
        "TotalAmount": "decimal(19,4)",
    }


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (dt.date(2024, 1, 1), dt.date(2024, 1, 31)),  # both boundaries hit rows 1 & 3
        (dt.date(2024, 1, 2), dt.date(2024, 1, 30)),  # boundaries exclude rows 1 & 3
        (dt.date(2023, 12, 31), dt.date(2024, 2, 1)),  # everything
        (dt.date(2025, 1, 1), dt.date(2025, 12, 31)),  # nothing
        (dt.date(2024, 1, 15), dt.date(2024, 1, 15)),  # single day, multiple accounts
    ],
)
def test_daily_transaction_parity(fact_df, start, end):
    assert rows(daily_transaction(fact_df, start, end)) == expected_daily(start, end)


def test_daily_transaction_accepts_iso_strings(fact_df):
    a = rows(daily_transaction(fact_df, "2024-01-01", "2024-01-31"))
    b = rows(daily_transaction(fact_df, dt.date(2024, 1, 1), dt.date(2024, 1, 31)))
    assert a == b
    assert len(a) == 6  # 01-01, 01-10, 01-15, 01-20, 01-21, 01-31


def test_daily_transaction_boundaries_inclusive(fact_df):
    dates = [r[0] for r in rows(daily_transaction(fact_df, "2024-01-01", "2024-01-31"))]
    assert dt.date(2024, 1, 1) in dates and dt.date(2024, 1, 31) in dates
    assert dt.date(2023, 12, 31) not in dates and dt.date(2024, 2, 1) not in dates


def test_daily_transaction_is_ordered(fact_df):
    dates = [r[0] for r in rows(daily_transaction(fact_df, "2000-01-01", "2030-01-01"))]
    assert dates == sorted(dates)


# ------------------------------------------------------------------------ balance_per_customer


def test_balance_schema(customer_df, account_df, fact_df):
    df = balance_per_customer(customer_df, account_df, fact_df, "smith")
    assert df.columns == [
        "CustomerName",
        "AccountType",
        "InitialBalance",
        "CurrentBalance",
    ]
    types = dict(df.dtypes)
    assert types["InitialBalance"] == "decimal(19,4)"
    assert types["CurrentBalance"] == "decimal(19,4)"


@pytest.mark.parametrize(
    "name", ["smith", "SMITH", "Smith", "jOhN", "son", "alice", "", "zzz", "100"]
)
def test_balance_parity(customer_df, account_df, fact_df, name):
    got = set(rows(balance_per_customer(customer_df, account_df, fact_df, name)))
    assert got == expected_balance(name)


def test_balance_partial_name_case_insensitive(customer_df, account_df, fact_df):
    got = set(rows(balance_per_customer(customer_df, account_df, fact_df, "smith")))
    names = {r[0] for r in got}
    assert names == {"JOHN SMITH", "Jane Smithson"}


def test_balance_inactive_accounts_excluded(customer_df, account_df, fact_df):
    got = rows(balance_per_customer(customer_df, account_df, fact_df, "john"))
    assert {r[1] for r in got} == {"SAVINGS", "CHECKING"}  # LOAN (inactive) excluded
    upper = next(r for r in got if r[1] == "CHECKING")  # Status='ACTIVE' still matches
    assert upper[3] == Decimal("745.5000")  # 250.50 + 500 - 5


def test_balance_account_without_transactions(customer_df, account_df, fact_df):
    got = rows(balance_per_customer(customer_df, account_df, fact_df, "jane"))
    assert got == [
        ("Jane Smithson", "SAVINGS", Decimal("300.0000"), Decimal("300.0000"))
    ]


def test_balance_signed_sum(customer_df, account_df, fact_df):
    got = rows(balance_per_customer(customer_df, account_df, fact_df, "john smith"))
    savings = next(r for r in got if r[1] == "SAVINGS")
    # 1000 + 100 (Deposit) - 40.25 (Withdrawal) - 10 (Transfer)
    assert savings[3] == Decimal("1049.7500")


def test_balance_like_wildcards_behave_as_in_tsql(customer_df, account_df, fact_df):
    assert rows(balance_per_customer(customer_df, account_df, fact_df, "100")) == [
        ("Bob 100% Sure", "SAVINGS", Decimal("1.0000"), Decimal("1.0001"))
    ]
    # '_' is a single-character wildcard in LIKE, exactly as in T-SQL
    got = rows(balance_per_customer(customer_df, account_df, fact_df, "J_hn"))
    assert {r[0] for r in got} == {"JOHN SMITH"}
