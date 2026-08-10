"""Unit tests for the sp_DailyTransaction replacement."""

from __future__ import annotations

import datetime
from decimal import Decimal

from conftest import FACT_TRANSACTION_VIEW


def _run(module, spark, start_date, end_date):
    df = module.daily_transaction(spark, start_date, end_date, table=FACT_TRANSACTION_VIEW)
    return [tuple(row) for row in df.collect()]


def test_aggregates_per_day_and_orders_by_date(daily_transaction_module, spark, fixtures):
    rows = _run(daily_transaction_module, spark, "2024-01-01", "2024-01-05")

    assert rows == [
        (datetime.date(2024, 1, 1), 2, Decimal("250.0000")),
        (datetime.date(2024, 1, 2), 1, Decimal("100.0000")),
        (datetime.date(2024, 1, 3), 1, Decimal("50.0000")),
        (datetime.date(2024, 1, 5), 1, Decimal("25.0000")),
    ]


def test_output_schema_matches_money_scale(daily_transaction_module, spark, fixtures):
    df = daily_transaction_module.daily_transaction(
        spark, "2024-01-01", "2024-01-05", table=FACT_TRANSACTION_VIEW
    )

    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [
        ("date", "date"),
        ("total_transactions", "bigint"),
        ("total_amount", "decimal(19,4)"),
    ]


def test_date_bounds_are_inclusive(daily_transaction_module, spark, fixtures):
    rows = _run(daily_transaction_module, spark, "2024-01-01", "2024-01-02")

    assert [row[0] for row in rows] == [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]


def test_single_day_range_includes_all_times_of_that_day(
    daily_transaction_module, spark, fixtures
):
    rows = _run(daily_transaction_module, spark, "2024-01-01", "2024-01-01")

    # Both 09:30 and 18:00 transactions fall in the range: the filter truncates to DATE.
    assert rows == [(datetime.date(2024, 1, 1), 2, Decimal("250.0000"))]


def test_accepts_date_objects(daily_transaction_module, spark, fixtures):
    rows = _run(
        daily_transaction_module, spark, datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)
    )

    assert [row[0] for row in rows] == [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]


def test_empty_range_returns_no_rows(daily_transaction_module, spark, fixtures):
    assert _run(daily_transaction_module, spark, "2023-12-01", "2023-12-31") == []


def test_parameters_are_bound_not_interpolated(daily_transaction_module, spark, fixtures):
    # A classic injection payload must be treated as a (invalid) date value, not as SQL.
    df = daily_transaction_module.daily_transaction(
        spark,
        "2024-01-01' OR '1'='1",
        "2024-01-05",
        table=FACT_TRANSACTION_VIEW,
    )

    # CAST of an unparseable date yields NULL, so the BETWEEN filter matches nothing.
    assert df.collect() == []
