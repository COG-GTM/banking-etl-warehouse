"""Equivalence tests: PySpark conversion vs. SQLite transliteration of the T-SQL body.

See ``banking_dwh.tsql_reference`` for exactly what the reference engine does and does not
establish about SQL Server behaviour.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from banking_dwh.balance_per_customer import (
    DEFAULT_CATALOG,
    GOLD_TABLE,
    account_balance,
    balance_per_customer,
    qualify,
)
from banking_dwh.tsql_reference import BalanceRow, sp_balance_per_customer

PARAMETERS = [
    "budi",  # matches two customers, differing only by case
    "BUDI",  # same match set: LIKE is case-insensitive under the default collation
    "Siti",  # account with no transactions at all -> ISNULL(..., 0) branch
    "Andi",  # negative opening balance
    "%",  # wildcard inside the parameter matches every customer
    "Wija",  # mid-string match
    "nobody",  # empty result set
    "100%",  # parameter containing a literal-looking wildcard
    "Rina",  # trailing spaces in Status / TransactionType, which VARCHAR '=' pads away
    None,  # NULL parameter: the concatenated pattern is NULL, so nothing matches
]


def _spark_rows(spark_tables, customer_name: str | None) -> list[BalanceRow]:
    result = balance_per_customer(
        spark_tables["dim_customer"],
        spark_tables["dim_account"],
        spark_tables["fact_transaction"],
        customer_name,
    )
    return sorted(
        BalanceRow(
            row["CustomerName"],
            row["AccountType"],
            Decimal(row["InitialBalance"]).quantize(Decimal("0.0001")),
            Decimal(row["CurrentBalance"]).quantize(Decimal("0.0001")),
        )
        for row in result.collect()
    )


def _reference_rows(sqlite_connection, customer_name: str | None) -> list[BalanceRow]:
    return sorted(sp_balance_per_customer(sqlite_connection, customer_name))


@pytest.mark.parametrize("customer_name", PARAMETERS)
def test_conversion_matches_tsql_reference(spark_tables, sqlite_connection, customer_name):
    assert _spark_rows(spark_tables, customer_name) == _reference_rows(
        sqlite_connection, customer_name
    )


def test_reference_returns_expected_balances(sqlite_connection):
    """Guard the reference itself: hand-computed values from the fixture."""
    rows = {
        (row.CustomerName, row.AccountType): row
        for row in sp_balance_per_customer(sqlite_connection, "BUDI")
    }

    # Savings 11: 1_000_000 + 1_500_000 - 500_000 - 250_000.50
    assert rows[("BUDI SANTOSO", "Savings")].CurrentBalance == Decimal("1749999.5000")
    # Checking 12: 250_000.50 + 0.75, deposit written in lower case
    assert rows[("BUDI SANTOSO", "Checking")].CurrentBalance == Decimal("250001.2500")
    # Deposito 13 belongs to the same customer but is closed, so it must not appear.
    assert ("BUDI SANTOSO", "Deposito") not in rows
    # 'budi hartono' matches the same case-insensitive LIKE.
    assert rows[("budi hartono", "Savings")].CurrentBalance == Decimal("-10.0000")


def test_inactive_and_orphan_rows_are_excluded(spark_tables):
    rows = account_balance(
        spark_tables["dim_customer"],
        spark_tables["dim_account"],
        spark_tables["fact_transaction"],
    ).collect()

    assert 13 not in {row["AccountID"] for row in rows}  # Status = 'closed'
    assert 99 not in {row["AccountID"] for row in rows}  # transaction with no matching account


def test_trailing_spaces_do_not_change_results(spark_tables):
    """'active ' is still active and 'Deposit ' still adds: VARCHAR '=' pads its operands."""
    row = _spark_rows(spark_tables, "Rina")[0]

    assert row.CurrentBalance == Decimal("125.0000")


def test_null_customer_name_returns_no_rows(spark_tables):
    assert _spark_rows(spark_tables, None) == []


def test_table_names_follow_the_bundle_catalog():
    """A dev run must not resolve to the prod catalog."""
    assert qualify(GOLD_TABLE, "dev_banking") == "dev_banking.banking_dwh_gold.account_balance"
    assert qualify(GOLD_TABLE) == f"{DEFAULT_CATALOG}.banking_dwh_gold.account_balance"


def test_money_arithmetic_is_exact(spark_tables, sqlite_connection):
    """DECIMAL(19,4) both sides: no float drift on sub-cent amounts."""
    spark_row = _spark_rows(spark_tables, "CASH HOLDINGS")[0]

    assert spark_row.CurrentBalance == Decimal("10.0003")
    assert spark_row == _reference_rows(sqlite_connection, "CASH HOLDINGS")[0]
