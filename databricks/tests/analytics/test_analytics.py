"""Ticket 9: sp_DailyTransaction / sp_BalancePerCustomer ports vs SQL Server 2022 results.

The expectations in tests/analytics/expected/ were produced by scripts/analytics/sqlserver_parity.py:
the original procedures run on SQL Server 2022 (SQL_Latin1_General_CP1_CI_AS) over a DWH loaded like
the Talend jobs (sqlserver_dwh.json) and over synthetic edge rows (sqlserver_dwh_edge.json).
Each scenario's DWH tables are loaded into an isolated local gold schema, and both the SQL table
functions and the PySpark equivalents must return exactly the SQL Server rows.
"""
import datetime as dt
import json
from decimal import Decimal

import pytest
from conftest import ROOT

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
from banking_etl.analytics.procedures import (
    BALANCE_COLUMNS,
    DAILY_COLUMNS,
    balance_per_customer_df,
    daily_transaction_df,
    gold_frames,
    like_pattern,
)
from banking_etl.config import Settings
from banking_etl.gold.ddl import apply_star_schema

EXPECTED = ROOT / "tests" / "analytics" / "expected"
SCENARIOS = {"dwh": "t9a_", "dwh_edge": "t9b_"}

LOADS = {
    "dim_customer": (
        "DimCustomer",
        "CustomerID INT, CustomerName STRING, Address STRING, CityName STRING, StateName STRING, "
        "Age INT, Gender STRING, Email STRING",
    ),
    "dim_account": (
        "DimAccount",
        "AccountID INT, CustomerID INT, AccountType STRING, Balance DECIMAL(19,4), DateOpened DATE, Status STRING",
    ),
    "fact_transaction": (
        "FactTransaction",
        "TransactionID INT, AccountID INT, TransactionDate TIMESTAMP, Amount DECIMAL(19,4), "
        "TransactionType STRING, BranchID INT",
    ),
}
CONVERTERS = {
    "Balance": Decimal,
    "Amount": Decimal,
    "DateOpened": dt.date.fromisoformat,
    "TransactionDate": dt.datetime.fromisoformat,
}


def load_expected(scenario):
    return json.loads((EXPECTED / f"sqlserver_{scenario}.json").read_text(encoding="utf-8"))


def scenario_cases():
    return [
        pytest.param(scenario, i, id=f"{scenario}-{c['procedure']}-{i}")
        for scenario in SCENARIOS
        for i, c in enumerate(load_expected(scenario)["cases"])
    ]


def to_python(row, columns):
    out = []
    for col in columns:
        value = row[col]
        out.append(None if value is None else CONVERTERS.get(col, lambda v: v)(value))
    return tuple(out)


def load_scenario(spark, scenario):
    settings = Settings(catalog=None, schema_prefix=SCENARIOS[scenario])
    spark.sql(f"DROP DATABASE IF EXISTS {settings.schema('gold')} CASCADE")
    apply_star_schema(spark, settings, unity_catalog=False)
    tables = load_expected(scenario)["tables"]
    for table, (source, ddl) in LOADS.items():
        columns = [c.split()[0] for c in ddl.split(", ")]
        df = spark.createDataFrame([to_python(r, columns) for r in tables[source]], ddl)
        view = f"t9_{scenario}_{table}"
        df.createOrReplaceTempView(view)
        cols = ", ".join(columns)
        spark.sql(f"INSERT INTO {settings.table('gold', table)} ({cols}) SELECT {cols} FROM {view}")
    return settings


@pytest.fixture(scope="module")
def scenarios(spark):
    return {scenario: load_scenario(spark, scenario) for scenario in SCENARIOS}


def normalize(rows, columns):
    def cell(value):
        if isinstance(value, (dt.date, Decimal)):
            return str(value)
        return value

    return [tuple(cell(r[c]) for c in columns) for r in rows]


def sort_key(row):
    return tuple((v is not None, v) for v in row)


def run_case(spark, settings, case):
    """(sql_rows, dataframe_rows, expected_rows) normalised to comparable tuples."""
    params = case["params"]
    if case["procedure"] == "sp_DailyTransaction":
        columns = DAILY_COLUMNS
        apply_analytics_functions(spark, settings)
        sql_df = daily_transaction(spark, settings, *params)
        py_df = daily_transaction_df(gold_frames(spark, settings)["fact_transaction"], *params)
        ordered = True
    else:
        columns = BALANCE_COLUMNS
        apply_analytics_functions(spark, settings)
        sql_df = balance_per_customer(spark, settings, *params)
        frames = gold_frames(spark, settings)
        py_df = balance_per_customer_df(frames["dim_customer"], frames["dim_account"], frames["fact_transaction"], *params)
        ordered = False  # the procedure has no ORDER BY
    results = [normalize(df.collect(), columns) for df in (sql_df, py_df)]
    expected = [tuple(r[c] for c in columns) for r in case["rows"]]
    if not ordered:
        results = [sorted(r, key=sort_key) for r in results]
        expected = sorted(expected, key=sort_key)
    return results[0], results[1], expected


@pytest.mark.parametrize(("scenario", "index"), scenario_cases())
def test_matches_sqlserver(spark, scenarios, scenario, index):
    case = load_expected(scenario)["cases"][index]
    sql_rows, py_rows, expected = run_case(spark, scenarios[scenario], case)
    assert sql_rows == expected
    assert py_rows == expected


def test_parity_covers_ticket_examples():
    cases = {(c["procedure"], tuple(c["params"])): c["rows"] for c in load_expected("dwh")["cases"]}
    assert cases[("sp_DailyTransaction", ("2024-01-18", "2024-01-20"))] == [
        {"Date": "2024-01-20", "TotalTransactions": 3, "TotalAmount": "2000000.0000"}
    ]
    assert len(cases[("sp_BalancePerCustomer", ("",))]) == 18  # every active account
    assert cases[("sp_BalancePerCustomer", (None,))] == []


def test_function_result_types(spark, scenarios):
    settings = scenarios["dwh"]
    apply_analytics_functions(spark, settings)
    daily = daily_transaction(spark, settings, "2024-01-18", "2024-01-20").schema
    assert [(f.name, f.dataType.simpleString()) for f in daily] == [
        ("Date", "date"),
        ("TotalTransactions", "int"),
        ("TotalAmount", "decimal(19,4)"),
    ]
    balance = balance_per_customer(spark, settings, "shelly").schema
    assert [(f.name, f.dataType.simpleString()) for f in balance] == [
        ("CustomerName", "string"),
        ("AccountType", "string"),
        ("InitialBalance", "decimal(19,4)"),
        ("CurrentBalance", "decimal(19,4)"),
    ]
    frames = gold_frames(spark, settings)
    assert daily_transaction_df(frames["fact_transaction"], "2024-01-18", "2024-01-20").schema == daily
    py_balance = balance_per_customer_df(frames["dim_customer"], frames["dim_account"], frames["fact_transaction"], "x")
    assert [(f.name, f.dataType) for f in py_balance.schema] == [(f.name, f.dataType) for f in balance]


def test_daily_transaction_accepts_dates(spark, scenarios):
    settings = scenarios["dwh"]
    apply_analytics_functions(spark, settings)
    as_str = daily_transaction(spark, settings, " 2024-01-18 ", "2024-01-20").collect()
    as_date = daily_transaction(spark, settings, dt.date(2024, 1, 18), dt.date(2024, 1, 20)).collect()
    assert as_str == as_date and len(as_str) == 1
    with pytest.raises(ValueError):
        daily_transaction(spark, settings, "18-01-2024", "2024-01-20")


def test_apply_is_idempotent(spark, scenarios):
    settings = scenarios["dwh"]
    first = apply_analytics_functions(spark, settings)
    second = apply_analytics_functions(spark, settings)
    assert first == second and len(first) == 2


def test_render_unity_catalog():
    settings = Settings(catalog="migration_demo", schema_prefix="banking_etl_")
    statements = render_functions(settings)
    assert len(statements) == 2
    daily, balance = statements
    assert "CREATE OR REPLACE FUNCTION migration_demo.banking_etl_gold.fn_daily_transaction(" in daily
    assert "FROM migration_demo.banking_etl_gold.fact_transaction" in daily
    assert "CREATE OR REPLACE FUNCTION migration_demo.banking_etl_gold.fn_balance_per_customer(" in balance
    assert "migration_demo.banking_etl_gold.dim_customer c" in balance
    assert "migration_demo.banking_etl_gold.dim_account a" in balance
    for statement in statements:
        assert statement.startswith("CREATE OR REPLACE FUNCTION migration_demo.banking_etl_gold.fn_")
        assert "${" not in statement and "TEMPORARY" not in statement and "--" not in statement
        assert not statement.endswith(";")


def test_render_local_is_temporary():
    settings = Settings(catalog=None, schema_prefix="x_")
    params = function_parameters(settings)
    assert params["create"] == "CREATE OR REPLACE TEMPORARY FUNCTION"
    assert params[FN_DAILY_TRANSACTION] == "fn_daily_transaction"
    assert params["fact_transaction"] == "x_gold.fact_transaction"
    assert all("TEMPORARY FUNCTION fn_" in s for s in render_functions(settings))
    assert function_name(settings, FN_BALANCE_PER_CUSTOMER, temporary=False) == "x_gold.fn_balance_per_customer"
    with pytest.raises(ValueError):
        function_name(settings, "sp_DailyTransaction")


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
