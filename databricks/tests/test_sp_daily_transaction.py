"""Parity tests: sp_DailyTransaction on Databricks vs. the original T-SQL.

``fixtures/sp_daily_transaction/expected.json`` was produced by running the repository's
own ``sp_DailyTransaction`` on SQL Server 2022 against ``fact_transaction.csv`` for every
case in ``cases.json`` (see ``generate_expected.py``).
"""

from __future__ import annotations

import csv
import datetime as dt
import importlib.util
import json
import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DATABRICKS_ROOT / "src"))

from banking_etl import sp_daily_transaction as sdt  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sp_daily_transaction"
CASES = json.loads((FIXTURES / "cases.json").read_text())
EXPECTED = json.loads((FIXTURES / "expected.json").read_text())["cases"]

FACT_DDL = (
    "transaction_id INT, account_id INT, transaction_date {ts}, amount DECIMAL(19,4), "
    "transaction_type STRING, branch_id INT"
)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_sp_daily_transaction")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    previous_tz = session.conf.get("spark.sql.session.timeZone")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.conf.set("spark.sql.session.timeZone", previous_tz)


def _fixture_rows():
    with open(FIXTURES / "fact_transaction.csv", newline="") as f:
        for r in csv.DictReader(f):
            yield (
                int(r["transaction_id"]),
                int(r["account_id"]) if r["account_id"] else None,
                dt.datetime.fromisoformat(r["transaction_date"]) if r["transaction_date"] else None,
                Decimal(r["amount"]) if r["amount"] else None,
                r["transaction_type"],
                int(r["branch_id"]) if r["branch_id"] else None,
            )


def _create_fact_table(spark, timestamp_type: str) -> str:
    schema = f"gold_{timestamp_type.lower()}_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {schema}")
    df = spark.createDataFrame(list(_fixture_rows()), FACT_DDL.format(ts=timestamp_type))
    df.write.saveAsTable(f"{schema}.fact_transaction")
    return schema


@pytest.fixture(scope="module")
def gold_schema(spark):
    schema = _create_fact_table(spark, "TIMESTAMP")
    yield schema
    spark.sql(f"DROP DATABASE {schema} CASCADE")


@pytest.fixture(scope="module")
def gold_schema_ntz(spark):
    schema = _create_fact_table(spark, "TIMESTAMP_NTZ")
    yield schema
    spark.sql(f"DROP DATABASE {schema} CASCADE")


def _as_expected(df):
    return [
        {
            "date": r["date"].isoformat(),
            "total_transactions": r["total_transactions"],
            "total_amount": None if r["total_amount"] is None else str(r["total_amount"]),
        }
        for r in df.collect()
    ]


def _case_ids():
    return [c["name"] for c in CASES]


def test_fixture_covers_boundaries_and_empty_ranges():
    assert set(EXPECTED) == {c["name"] for c in CASES}
    assert sum(1 for c in CASES if EXPECTED[c["name"]] == []) >= 6
    assert EXPECTED["start_bound_excludes_previous_day_23_59_59_997"][0]["date"] == "2024-01-21"
    assert EXPECTED["all_null_amounts_day"] == [
        {"date": "2024-01-24", "total_transactions": 1, "total_amount": None}
    ]


@pytest.mark.parametrize("case", CASES, ids=_case_ids())
def test_pyspark_function_matches_tsql(spark, gold_schema, case):
    result = sdt.run_daily_transaction(
        spark, case["start_date"], case["end_date"], catalog="spark_catalog", schema=gold_schema
    )
    assert _as_expected(result) == EXPECTED[case["name"]]


@pytest.mark.parametrize("case", CASES, ids=_case_ids())
def test_sql_body_matches_tsql(spark, gold_schema, case):
    """The exact SELECT used as the UC table function body, bound via parameter markers."""
    result = sdt.run_daily_transaction_sql(
        spark, case["start_date"], case["end_date"], catalog="spark_catalog", schema=gold_schema
    )
    assert _as_expected(result) == EXPECTED[case["name"]]


@pytest.mark.parametrize("case", CASES, ids=_case_ids())
def test_timestamp_ntz_is_timezone_independent(spark, gold_schema_ntz, case):
    spark.conf.set("spark.sql.session.timeZone", "Asia/Jakarta")
    try:
        result = sdt.run_daily_transaction(
            spark,
            case["start_date"],
            case["end_date"],
            catalog="spark_catalog",
            schema=gold_schema_ntz,
        )
        assert _as_expected(result) == EXPECTED[case["name"]]
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_accepts_date_and_datetime_parameters(spark, gold_schema):
    result = sdt.run_daily_transaction(
        spark,
        dt.date(2024, 1, 21),
        dt.datetime(2024, 1, 25, 23, 0),
        catalog="spark_catalog",
        schema=gold_schema,
    )
    assert _as_expected(result) == EXPECTED["inclusive_bounds_with_midnight_and_end_of_day"]


def test_output_schema_matches_tsql_types(spark, gold_schema):
    for df in (
        sdt.run_daily_transaction(spark, "2024-01-01", "2024-12-31", "spark_catalog", gold_schema),
        sdt.run_daily_transaction_sql(
            spark, "2024-01-01", "2024-12-31", "spark_catalog", gold_schema
        ),
    ):
        assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [
            ("date", "date"),
            ("total_transactions", "int"),
            ("total_amount", "decimal(19,4)"),
        ]


def test_daily_transaction_on_dataframe_is_ordered(spark):
    fact = spark.createDataFrame(
        [
            (3, 1, dt.datetime(2024, 3, 3, 1), Decimal("3.0000"), "Deposit", 1),
            (1, 1, dt.datetime(2024, 3, 1, 1), Decimal("1.0000"), "Deposit", 1),
            (2, 1, dt.datetime(2024, 3, 2, 1), Decimal("2.0000"), "Deposit", 1),
        ],
        FACT_DDL.format(ts="TIMESTAMP"),
    )
    rows = sdt.daily_transaction(fact, "2024-03-01", "2024-03-03").collect()
    assert [r["date"] for r in rows] == [dt.date(2024, 3, d) for d in (1, 2, 3)]


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("2024-01-21", dt.date(2024, 1, 21)),
        (" 2024-02-29 ", dt.date(2024, 2, 29)),
        (dt.date(2024, 1, 21), dt.date(2024, 1, 21)),
        (dt.datetime(2024, 1, 21, 23, 59, 59), dt.date(2024, 1, 21)),
    ],
)
def test_to_date(value, expected):
    assert sdt.to_date(value) == expected


@pytest.mark.parametrize("value", ["2024-02-30", "21-01-2024", "not a date"])
def test_to_date_rejects_invalid_strings(value):
    with pytest.raises(ValueError):
        sdt.to_date(value)


def test_to_date_rejects_other_types():
    with pytest.raises(TypeError):
        sdt.to_date(20240121)


def test_table_function_ddl_is_configurable_and_reuses_body():
    ddl = sdt.create_table_function_ddl(catalog="migration_demo", schema="run1_gold")
    assert ddl.startswith(
        "CREATE OR REPLACE FUNCTION `migration_demo`.`run1_gold`.`sp_daily_transaction`("
    )
    assert "start_date DATE" in ddl and "end_date DATE" in ddl
    assert "RETURNS TABLE (`date` DATE, total_transactions INT, total_amount DECIMAL(19,4))" in ddl
    assert ddl.endswith(
        sdt.daily_transaction_query(
            "`migration_demo`.`run1_gold`.`fact_transaction`",
            "sp_daily_transaction.start_date",
            "sp_daily_transaction.end_date",
        )
    )
    assert "banking_dwh" not in ddl


def test_procedure_ddl_is_configurable_and_reuses_body():
    ddl = sdt.create_procedure_ddl(catalog="migration_demo", schema="run1_gold")
    assert ddl.startswith(
        "CREATE OR REPLACE PROCEDURE `migration_demo`.`run1_gold`.`sp_daily_transaction_proc`("
    )
    assert "IN start_date DATE" in ddl and "IN end_date DATE" in ddl
    assert "LANGUAGE SQL\nSQL SECURITY INVOKER" in ddl
    body = sdt.daily_transaction_query(
        "`migration_demo`.`run1_gold`.`fact_transaction`", "start_date", "end_date"
    )
    assert ddl.endswith(f"AS BEGIN\n{body};\nEND")
    assert "banking_dwh" not in ddl


def test_default_names():
    ddl = sdt.create_table_function_ddl()
    assert "`banking_dwh`.`gold`.`sp_daily_transaction`" in ddl
    assert "FROM `banking_dwh`.`gold`.`fact_transaction`" in ddl
    assert "`banking_dwh`.`gold`.`sp_daily_transaction_proc`" in sdt.create_procedure_ddl()


def test_query_preserves_tsql_semantics():
    q = sdt.daily_transaction_query("t", ":s", ":e")
    assert "WHERE CAST(transaction_date AS DATE) BETWEEN :s AND :e" in q
    assert "GROUP BY CAST(transaction_date AS DATE)" in q
    assert "COUNT(transaction_id)" in q and "SUM(amount)" in q
    assert q.rstrip().endswith("ORDER BY `date`")


def test_quote_identifier():
    assert sdt.quote_identifier("a", "b`c") == "`a`.`b``c`"
    with pytest.raises(ValueError):
        sdt.quote_identifier("a", "")


def test_oracle_uses_the_repository_procedure():
    spec = importlib.util.spec_from_file_location(
        "generate_expected", FIXTURES / "generate_expected.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    proc = module.extract_procedure()
    assert proc.startswith("CREATE PROCEDURE sp_DailyTransaction")
    assert "CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date" in proc
    assert (
        proc
        in (DATABRICKS_ROOT.parent / "sql_scripts/02_create_procedures.sql").read_text() + "\nGO\n"
    )


def test_job_resource():
    yaml = pytest.importorskip("yaml")
    resource = yaml.safe_load((DATABRICKS_ROOT / "resources/sp_daily_transaction.yml").read_text())
    job = resource["resources"]["jobs"]["sp_daily_transaction"]
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params["catalog"] == "${var.catalog}"
    assert params["gold_schema"] == "gold"
    assert {"start_date", "end_date"} <= set(params)
    (task,) = job["tasks"]
    notebook = (DATABRICKS_ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve()
    assert notebook.is_file()
    source = notebook.read_text()
    for widget in params:
        assert f'dbutils.widgets.get("{widget}")' in source
