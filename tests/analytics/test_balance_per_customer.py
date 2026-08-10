"""Unit tests for the sp_BalancePerCustomer replacement."""

from __future__ import annotations

from decimal import Decimal

from conftest import DIM_ACCOUNT_VIEW, DIM_CUSTOMER_VIEW, FACT_TRANSACTION_VIEW


def _run(module, spark, customer_name):
    df = module.balance_per_customer(
        spark,
        customer_name,
        fact_transaction_table=FACT_TRANSACTION_VIEW,
        dim_customer_table=DIM_CUSTOMER_VIEW,
        dim_account_table=DIM_ACCOUNT_VIEW,
    )
    return sorted(tuple(row) for row in df.collect())


def test_current_balance_applies_deposit_and_withdrawal_signs(
    balance_per_customer_module, spark, fixtures
):
    rows = _run(balance_per_customer_module, spark, "Alice Smith")

    assert rows == [
        ("Alice Smith", "checking", Decimal("500.0000"), Decimal("500.0000")),
        ("Alice Smith", "savings", Decimal("1000.0000"), Decimal("1150.0000")),
    ]


def test_account_without_transactions_uses_coalesce_zero(
    balance_per_customer_module, spark, fixtures
):
    rows = _run(balance_per_customer_module, spark, "Alice Smith")
    checking = next(row for row in rows if row[1] == "checking")

    # LEFT JOIN yields NULL for account 2; coalesce(...,0) keeps the initial balance.
    assert checking[2] == checking[3] == Decimal("500.0000")


def test_inactive_accounts_are_filtered_out(balance_per_customer_module, spark, fixtures):
    assert _run(balance_per_customer_module, spark, "Bob") == []


def test_active_status_match_is_case_insensitive(balance_per_customer_module, spark, fixtures):
    # Account 4 has status 'ACTIVE'; SQL Server's default collation would match it.
    rows = _run(balance_per_customer_module, spark, "ALICIA Brown")

    assert rows == [("ALICIA Brown", "savings", Decimal("100.0000"), Decimal("75.0000"))]


def test_name_match_is_case_insensitive_substring(balance_per_customer_module, spark, fixtures):
    rows = _run(balance_per_customer_module, spark, "alic")

    assert {(row[0], row[1]) for row in rows} == {
        ("Alice Smith", "savings"),
        ("Alice Smith", "checking"),
        ("ALICIA Brown", "savings"),
    }


def test_empty_name_matches_every_active_account(balance_per_customer_module, spark, fixtures):
    rows = _run(balance_per_customer_module, spark, "")

    assert len(rows) == 3
    assert all(row[0] != "Bob Jones" for row in rows)


def test_output_schema_matches_money_scale(balance_per_customer_module, spark, fixtures):
    df = balance_per_customer_module.balance_per_customer(
        spark,
        "alice",
        fact_transaction_table=FACT_TRANSACTION_VIEW,
        dim_customer_table=DIM_CUSTOMER_VIEW,
        dim_account_table=DIM_ACCOUNT_VIEW,
    )

    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [
        ("customer_name", "string"),
        ("account_type", "string"),
        ("initial_balance", "decimal(19,4)"),
        ("current_balance", "decimal(19,4)"),
    ]


def test_like_wildcards_in_input_are_escaped(balance_per_customer_module, spark, fixtures):
    # '%' would match everything if it were passed through as a wildcard.
    assert _run(balance_per_customer_module, spark, "%") == []
    assert _run(balance_per_customer_module, spark, "Ali_e") == []


def test_build_like_pattern_escapes_and_lowercases(balance_per_customer_module):
    build = balance_per_customer_module.build_like_pattern

    assert build("Alice") == "%alice%"
    assert build("50%") == "%50\\%%"
    assert build("a_b") == "%a\\_b%"
    assert build("back\\slash") == "%back\\\\slash%"
    assert build("") == "%%"


def test_parameter_is_bound_not_interpolated(balance_per_customer_module, spark, fixtures):
    # An injection payload is matched literally against the name, returning nothing.
    assert _run(balance_per_customer_module, spark, "' OR 1=1 --") == []
