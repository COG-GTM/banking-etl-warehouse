"""SQL Server parity harness for the analytics functions, shared by local pytest and the live notebook.

``tests/analytics/expected/sqlserver_<scenario>.json`` (built by ``tests/analytics/sqlserver_parity.py``)
holds the legacy DWH tables and the rows the ORIGINAL procedures returned on SQL Server 2022.
``seed_gold`` loads those tables into gold-shaped (snake_case) tables of an ``AnalyticsTarget`` and
``run_cases`` calls both the SQL table functions and the PySpark equivalents for every case.
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from banking_etl.analytics.functions import (
    apply_analytics_functions,
    balance_per_customer,
    daily_transaction,
)
from banking_etl.analytics.procedures import (
    BALANCE_COLUMNS,
    DAILY_COLUMNS,
    balance_per_customer_df,
    daily_transaction_df,
    gold_frames,
)
from banking_etl.analytics.target import AnalyticsTarget

EXPECTED_DIR = Path(__file__).resolve().parents[3] / "tests" / "analytics" / "expected"
SCENARIOS = ("dwh", "dwh_edge")

# gold table -> (legacy DWH table, [(legacy column, gold column, Spark type)])
GOLD_FROM_LEGACY = {
    "dim_customer": ("DimCustomer", [
        ("CustomerID", "customer_id", "INT"), ("CustomerName", "customer_name", "STRING"),
        ("Address", "address", "STRING"), ("CityName", "city_name", "STRING"),
        ("StateName", "state_name", "STRING"), ("Age", "age", "INT"), ("Gender", "gender", "STRING"),
        ("Email", "email", "STRING"),
    ]),
    "dim_account": ("DimAccount", [
        ("AccountID", "account_id", "INT"), ("CustomerID", "customer_id", "INT"),
        ("AccountType", "account_type", "STRING"), ("Balance", "balance", "DECIMAL(19,4)"),
        ("DateOpened", "date_opened", "DATE"), ("Status", "status", "STRING"),
    ]),
    "fact_transaction": ("FactTransaction", [
        ("TransactionID", "transaction_id", "INT"), ("AccountID", "account_id", "INT"),
        ("TransactionDate", "transaction_date", "TIMESTAMP"), ("Amount", "amount", "DECIMAL(19,4)"),
        ("TransactionType", "transaction_type", "STRING"), ("BranchID", "branch_id", "INT"),
    ]),
}
LEGACY_RESULT_COLUMNS = {
    "sp_DailyTransaction": ("Date", "TotalTransactions", "TotalAmount"),
    "sp_BalancePerCustomer": ("CustomerName", "AccountType", "InitialBalance", "CurrentBalance"),
}


def load_expected(scenario: str, expected_dir: Path | str = EXPECTED_DIR) -> dict:
    return json.loads((Path(expected_dir) / f"sqlserver_{scenario}.json").read_text(encoding="utf-8"))


def _raw(value, sql_type: str) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text.replace("T", " ") if sql_type == "TIMESTAMP" else text  # FOR JSON emits ISO 'T' datetimes


def seed_gold(spark, target: AnalyticsTarget, tables: dict) -> dict[str, int]:
    """(Re)create ``target``'s fact_transaction / dim_account / dim_customer from legacy DWH rows.

    Values are loaded as strings and CAST in Spark SQL, so timestamps are interpreted in the session
    time zone exactly like ``CAST(transaction_date AS DATE)`` later reads them back.
    """
    if target.unity_catalog and target.layer == "gold":
        raise ValueError(f"refusing to overwrite shared gold tables in {target.schema} with parity fixtures")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.schema}")
    counts = {}
    for gold, (legacy, columns) in GOLD_FROM_LEGACY.items():
        rows = [tuple(_raw(r[src], typ) for src, _, typ in columns) for r in tables[legacy]]
        raw = spark.createDataFrame(rows, ", ".join(f"{dst} STRING" for _, dst, _ in columns))
        typed = raw.selectExpr(*(f"CAST({dst} AS {typ}) AS {dst}" for _, dst, typ in columns))
        typed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target.table(gold))
        counts[target.table(gold)] = len(rows)
    return counts


def _cell(value):
    if isinstance(value, (dt.date, Decimal)):
        return str(value)
    return value


def _sort_key(row):
    return tuple((v is not None, v) for v in row)


@dataclass
class CaseResult:
    procedure: str
    params: list
    expected: list = field(default_factory=list)
    sql_rows: list = field(default_factory=list)
    df_rows: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.sql_rows == self.expected and self.df_rows == self.expected

    def summary(self) -> str:
        status = "OK" if self.ok else "MISMATCH"
        return f"[{status}] {self.procedure}{tuple(self.params)!r}: {len(self.sql_rows)} rows (expected {len(self.expected)})"


def run_case(spark, target: AnalyticsTarget, case: dict) -> CaseResult:
    """Call the SQL function and the PySpark equivalent for one SQL Server case."""
    proc, params = case["procedure"], case["params"]
    if proc == "sp_DailyTransaction":
        columns = DAILY_COLUMNS
        sql_df = daily_transaction(spark, target, *params)
        py_df = daily_transaction_df(gold_frames(spark, target)["fact_transaction"], *params)
    elif proc == "sp_BalancePerCustomer":
        columns = BALANCE_COLUMNS
        sql_df = balance_per_customer(spark, target, *params)
        frames = gold_frames(spark, target)
        py_df = balance_per_customer_df(frames["dim_customer"], frames["dim_account"], frames["fact_transaction"], *params)
    else:
        raise ValueError(f"unknown procedure {proc!r}")
    got = [[tuple(_cell(r[c]) for c in columns) for r in df.collect()] for df in (sql_df, py_df)]
    expected = [tuple(r[c] for c in LEGACY_RESULT_COLUMNS[proc]) for r in case["rows"]]
    if proc == "sp_BalancePerCustomer":  # the procedure has no ORDER BY
        got = [sorted(rows, key=_sort_key) for rows in got]
        expected = sorted(expected, key=_sort_key)
    return CaseResult(proc, params, expected, got[0], got[1])


def run_cases(spark, target: AnalyticsTarget, cases: list[dict], *, create: bool = True) -> list[CaseResult]:
    if create:
        apply_analytics_functions(spark, target)
    return [run_case(spark, target, case) for case in cases]
