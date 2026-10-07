"""Ticket 9: sp_DailyTransaction / sp_BalancePerCustomer ports vs SQL Server 2022 results.

tests/analytics/expected/ was produced by tests/analytics/sqlserver_parity.py: the ORIGINAL procedures
run on SQL Server 2022 (SQL_Latin1_General_CP1_CI_AS) over a DWH loaded like the Talend jobs
(sqlserver_dwh.json) and over synthetic edge rows (sqlserver_dwh_edge.json). Each scenario is seeded
into gold-shaped local tables, and both the SQL table function and the PySpark equivalent must return
exactly the SQL Server rows (values, DECIMAL(19,4) scale, NULLs, row order for the daily report).
"""
import ast
import datetime as dt
from pathlib import Path

import pytest

from banking_etl.analytics.functions import (
    FN_BALANCE_PER_CUSTOMER,
    FN_DAILY_TRANSACTION,
    apply_analytics_functions,
    balance_per_customer,
    daily_transaction,
    function_name,
    function_parameters,
    render_functions,
)
from banking_etl.analytics.parity import SCENARIOS, load_expected, run_case, seed_gold
from banking_etl.analytics.procedures import (
    balance_per_customer_df,
    daily_transaction_df,
    gold_frames,
    like_pattern,
)
from banking_etl.analytics.target import AnalyticsTarget

DATABRICKS_ROOT = Path(__file__).resolve().parents[2]
LOCAL_TARGETS = {
    "dwh": AnalyticsTarget(catalog=None, schema_prefix="t9_local_", layer="gold"),
    "dwh_edge": AnalyticsTarget(catalog=None, schema_prefix="t9_local_", layer="gold", name_prefix="edge_"),
}


def scenario_cases():
    return [
        pytest.param(scenario, i, id=f"{scenario}-{c['procedure']}-{i}")
        for scenario in SCENARIOS
        for i, c in enumerate(load_expected(scenario)["cases"])
    ]


@pytest.fixture(scope="module")
def targets(spark):
    for scenario, target in LOCAL_TARGETS.items():
        seed_gold(spark, target, load_expected(scenario)["tables"])
        apply_analytics_functions(spark, target)
    return LOCAL_TARGETS


@pytest.mark.parametrize(("scenario", "index"), scenario_cases())
def test_matches_sqlserver(spark, targets, scenario, index):
    case = load_expected(scenario)["cases"][index]
    result = run_case(spark, targets[scenario], case)
    assert result.sql_rows == result.expected
    assert result.df_rows == result.expected


def test_expectations_cover_ticket_examples():
    cases = {(c["procedure"], tuple(c["params"])): c["rows"] for c in load_expected("dwh")["cases"]}
    assert cases[("sp_DailyTransaction", ("2024-01-18", "2024-01-20"))] == [
        {"Date": "2024-01-20", "TotalTransactions": 3, "TotalAmount": "2000000.0000"}
    ]
    assert len(cases[("sp_BalancePerCustomer", ("",))]) == 18  # every active account with a customer
    assert cases[("sp_BalancePerCustomer", (None,))] == []
    assert len(load_expected("dwh")["cases"]) == 20 and len(load_expected("dwh_edge")["cases"]) == 16


def test_seeded_row_counts(spark, targets):
    tables = load_expected("dwh")["tables"]
    target = targets["dwh"]
    assert spark.table(target.table("fact_transaction")).count() == len(tables["FactTransaction"]) == 22
    assert spark.table(target.table("dim_account")).count() == len(tables["DimAccount"]) == 21
    assert spark.table(target.table("dim_customer")).count() == len(tables["DimCustomer"]) == 20


def test_function_result_types(spark, targets):
    target = targets["dwh"]
    daily = daily_transaction(spark, target, "2024-01-18", "2024-01-20").schema
    assert [(f.name, f.dataType.simpleString()) for f in daily] == [
        ("date", "date"),
        ("total_transactions", "int"),
        ("total_amount", "decimal(19,4)"),
    ]
    balance = balance_per_customer(spark, target, "shelly").schema
    assert [(f.name, f.dataType.simpleString()) for f in balance] == [
        ("customer_name", "string"),
        ("account_type", "string"),
        ("initial_balance", "decimal(19,4)"),
        ("current_balance", "decimal(19,4)"),
    ]
    frames = gold_frames(spark, target)
    py_daily = daily_transaction_df(frames["fact_transaction"], "2024-01-18", "2024-01-20").schema
    assert [(f.name, f.dataType) for f in py_daily] == [(f.name, f.dataType) for f in daily]
    py_balance = balance_per_customer_df(frames["dim_customer"], frames["dim_account"], frames["fact_transaction"], "x")
    assert [(f.name, f.dataType) for f in py_balance.schema] == [(f.name, f.dataType) for f in balance]


def test_daily_transaction_ordered_by_date(spark, targets):
    rows = daily_transaction(spark, targets["dwh"], "2022-01-01", "2024-12-31").collect()
    assert [r["date"] for r in rows] == sorted(r["date"] for r in rows) and len(rows) == 9


def test_daily_transaction_accepts_dates(spark, targets):
    target = targets["dwh"]
    as_str = daily_transaction(spark, target, " 2024-01-18 ", "2024-01-20").collect()
    as_date = daily_transaction(spark, target, dt.date(2024, 1, 18), dt.date(2024, 1, 20)).collect()
    assert as_str == as_date and len(as_str) == 1
    with pytest.raises(ValueError):
        daily_transaction(spark, target, "18-01-2024", "2024-01-20")


def test_apply_is_idempotent(spark, targets):
    first = apply_analytics_functions(spark, targets["dwh"])
    second = apply_analytics_functions(spark, targets["dwh"])
    assert first == second and len(first) == 2


def test_render_unity_catalog_gold():
    daily, balance = render_functions(AnalyticsTarget())
    assert daily.startswith("CREATE OR REPLACE FUNCTION migration_demo.banking_mig_gold.fn_daily_transaction(")
    assert "FROM migration_demo.banking_mig_gold.fact_transaction" in daily
    assert "fn_daily_transaction.start_date AND fn_daily_transaction.end_date" in daily
    assert balance.startswith("CREATE OR REPLACE FUNCTION migration_demo.banking_mig_gold.fn_balance_per_customer(")
    assert "migration_demo.banking_mig_gold.dim_customer c" in balance
    assert "migration_demo.banking_mig_gold.dim_account a" in balance
    assert "left(fn_balance_per_customer.customer_name, 100)" in balance
    for statement in (daily, balance):
        assert "${" not in statement and "TEMPORARY" not in statement and "--" not in statement
        assert not statement.endswith(";")


def test_render_ticket_scoped_schema_with_prefix():
    target = AnalyticsTarget(layer="t9", name_prefix="edge_")
    daily, balance = render_functions(target)
    assert daily.startswith("CREATE OR REPLACE FUNCTION migration_demo.banking_mig_t9.edge_fn_daily_transaction(")
    assert "FROM migration_demo.banking_mig_t9.edge_fact_transaction" in daily
    assert "edge_fn_daily_transaction.start_date" in daily
    assert "left(edge_fn_balance_per_customer.customer_name, 100)" in balance
    assert function_name(target, FN_BALANCE_PER_CUSTOMER) == "migration_demo.banking_mig_t9.edge_fn_balance_per_customer"


def test_render_local_is_temporary():
    target = AnalyticsTarget(catalog=None, schema_prefix="x_")
    params = function_parameters(target)
    assert params["create"] == "CREATE OR REPLACE TEMPORARY FUNCTION"
    assert params[FN_DAILY_TRANSACTION] == "fn_daily_transaction"
    assert params["fact_transaction"] == "x_gold.fact_transaction"
    assert all("TEMPORARY FUNCTION fn_" in s for s in render_functions(target))
    assert function_name(target, FN_BALANCE_PER_CUSTOMER, temporary=False) == "x_gold.fn_balance_per_customer"
    with pytest.raises(ValueError):
        function_name(target, "sp_DailyTransaction")


@pytest.mark.parametrize(
    ("name", "pattern"),
    [
        (None, None),
        ("", "%"),
        ("shelly", "%shelly%"),
        ("50%_", "%50%_%"),
        ("%" * 95 + "SHELLQ", "%SHELL%"),
        ("K\\S", "%K\\\\S%"),
        ("A" * 150, "%" + "A" * 100 + "%"),
        ("a%%b", "%a%b%"),
    ],
)
def test_like_pattern(name, pattern):
    assert like_pattern(name) == pattern


def test_notebook_is_databricks_source():
    source = (DATABRICKS_ROOT / "notebooks" / "analytics.py").read_text(encoding="utf-8")
    assert source.startswith("# Databricks notebook source")
    ast.parse(source)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "migration_demo; DROP TABLE x"},
        {"catalog": "a.b"},
        {"schema_prefix": "banking_mig_`x"},
        {"layer": "gold --"},
        {"name_prefix": "edge.x_"},
        {"schema_prefix": "", "layer": ""},
    ],
)
def test_target_rejects_unsafe_identifiers(kwargs):
    with pytest.raises(ValueError):
        AnalyticsTarget(**kwargs)


def test_seed_refuses_shared_gold():
    with pytest.raises(ValueError, match="shared gold"):
        seed_gold(None, AnalyticsTarget(), {})
