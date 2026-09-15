from __future__ import annotations

import datetime as dt
import decimal

from gold.analytics import balance_per_customer, daily_transaction
from gold.config import GoldConfig
from gold.fact_transaction import build_fact_transaction


def _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch):
    specs = {s.name: s for s in GoldConfig().sources()}
    return build_fact_transaction(
        [
            (specs["mssql"], mssql_source),
            (specs["excel"], excel_source),
            (specs["csv"], csv_source),
        ],
        dim_account,
        dim_branch,
    ).fact


def test_daily_transaction_matches_hand_computed_totals(
    mssql_source, excel_source, csv_source, dim_account, dim_branch
):
    fact = _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch)

    rows = [r.asDict() for r in daily_transaction(fact, "2024-01-21", "2024-01-24").collect()]

    assert rows == [
        {
            "date": dt.date(2024, 1, 21),
            "total_transactions": 2,
            "total_amount": decimal.Decimal("2000000.5000"),
        },
        {
            "date": dt.date(2024, 1, 22),
            "total_transactions": 2,
            "total_amount": decimal.Decimal("200000.2500"),
        },
        {
            "date": dt.date(2024, 1, 24),
            "total_transactions": 2,
            "total_amount": decimal.Decimal("95000.7500"),
        },
    ]


def test_daily_transaction_range_is_inclusive_and_excludes_null_dates(
    mssql_source, excel_source, csv_source, dim_account, dim_branch
):
    fact = _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch)

    single_day = daily_transaction(fact, "2024-01-22", "2024-01-22").collect()
    assert [r.date for r in single_day] == [dt.date(2024, 1, 22)]

    whole_year = daily_transaction(fact, dt.date(2024, 1, 1), dt.date(2024, 12, 31))
    assert sum(r.total_transactions for r in whole_year.collect()) == 6  # row 9 has a NULL date


def test_balance_per_customer_applies_deposit_withdrawal_logic(
    mssql_source, excel_source, csv_source, dim_account, dim_branch, dim_customer
):
    fact = _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch)

    rows = sorted(
        (r.customer_name, r.account_type, r.initial_balance, r.current_balance)
        for r in balance_per_customer(dim_customer, dim_account, fact, "BUDI").collect()
    )

    assert rows == [
        # 100000 - 75000.75 (withdrawal) - 30000 (transfer)
        ("AGUS BUDIMAN", "Checking", decimal.Decimal("100000.0000"), decimal.Decimal("-5000.7500")),
        # 500000 - 500000.50 (transfer)
        ("BUDI SANTOSO", "Checking", decimal.Decimal("500000.0000"), decimal.Decimal("-0.5000")),
        # 1000000 + 1500000 (deposit)
        ("BUDI SANTOSO", "Savings", decimal.Decimal("1000000.0000"), decimal.Decimal("2500000.0000")),
    ]


def test_balance_per_customer_excludes_inactive_accounts(
    mssql_source, excel_source, csv_source, dim_account, dim_branch, dim_customer
):
    fact = _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch)
    rows = balance_per_customer(dim_customer, dim_account, fact, "AGUS").collect()

    assert [r.account_type for r in rows] == ["Checking"]  # account 14 is inactive


def test_balance_per_customer_coalesces_accounts_without_transactions(
    mssql_source, excel_source, csv_source, dim_account, dim_branch, dim_customer
):
    empty_fact = _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch).limit(0)

    rows = balance_per_customer(dim_customer, dim_account, empty_fact, "SITI").collect()

    assert len(rows) == 1
    assert rows[0].initial_balance == rows[0].current_balance == decimal.Decimal("250000.0000")


def test_balance_per_customer_substring_match(
    mssql_source, excel_source, csv_source, dim_account, dim_branch, dim_customer
):
    fact = _fact(mssql_source, excel_source, csv_source, dim_account, dim_branch)

    assert balance_per_customer(dim_customer, dim_account, fact, "SANTOSO").count() == 2
    assert balance_per_customer(dim_customer, dim_account, fact, "").count() == 4
    assert balance_per_customer(dim_customer, dim_account, fact, "nobody").count() == 0
