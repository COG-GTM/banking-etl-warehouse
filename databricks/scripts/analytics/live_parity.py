#!/usr/bin/env python3
"""Run the ticket 9 SQL Server parity cases against the Unity Catalog table functions on a SQL warehouse.

  python scripts/analytics/live_parity.py --catalog migration_demo --schema-prefix banking_etl_t9_ \
      --warehouse-id <id> [--gold-prefix banking_etl_] [--drop]

For each scenario in tests/analytics/expected/ (dwh, dwh_edge) this loads the SQL Server DWH tables
into an isolated ``<catalog>.<schema-prefix><scenario>_gold`` schema, creates the functions there from
sql/analytics/*.sql and checks every procedure call returns exactly the SQL Server rows.
With --gold-prefix it also runs the ``dwh`` cases against the functions already created in the
migrated gold schema (``<catalog>.<gold-prefix>gold``) without loading anything.
Uses the databricks CLI auth (DATABRICKS_HOST + OAuth M2M/PAT/profile). Exit code 1 on any mismatch.
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from banking_etl.analytics.functions import (  # noqa: E402
    FN_BALANCE_PER_CUSTOMER,
    FN_DAILY_TRANSACTION,
    function_name,
    render_functions,
)
from banking_etl.analytics.procedures import BALANCE_COLUMNS, DAILY_COLUMNS  # noqa: E402
from banking_etl.config import Settings  # noqa: E402
from banking_etl.setup.cli import run_cli  # noqa: E402

EXPECTED = Path(__file__).resolve().parents[2] / "tests" / "analytics" / "expected"
TABLES = {
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


def sql_literal(value, sql_type: str) -> str:
    if value is None:
        return "NULL"
    if sql_type in ("INT",):
        return str(int(value))
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"CAST('{text}' AS {sql_type})"


def load_tables(execute, settings: Settings, tables: dict) -> None:
    execute(f"CREATE SCHEMA IF NOT EXISTS {settings.schema('gold')}")
    for table, (source, ddl) in TABLES.items():
        name = settings.table("gold", table)
        columns = [tuple(c.split(" ", 1)) for c in ddl.split(", ")]
        execute(f"CREATE OR REPLACE TABLE {name} ({ddl})")
        rows = [
            "(" + ", ".join(sql_literal(r[col], typ) for col, typ in columns) + ")" for r in tables[source]
        ]
        if rows:
            execute(f"INSERT INTO {name} VALUES " + ",\n".join(rows))


def call(execute, settings: Settings, case: dict) -> list[tuple]:
    params = case["params"]
    if case["procedure"] == "sp_DailyTransaction":
        fn, columns, types = FN_DAILY_TRANSACTION, DAILY_COLUMNS, ("DATE", "DATE")
    else:
        fn, columns, types = FN_BALANCE_PER_CUSTOMER, BALANCE_COLUMNS, ("STRING",)
    names = [f"p{i}" for i in range(len(params))]
    sql = f"SELECT * FROM {function_name(settings, fn)}({', '.join(':' + n for n in names)})"
    parameters = [
        {"name": n, "type": t, **({} if v is None else {"value": v})} for n, t, v in zip(names, types, params)
    ]
    resp = execute(sql, parameters)
    result_columns = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    assert tuple(result_columns) == columns, result_columns
    return [tuple(r) for r in resp.get("result", {}).get("data_array") or []]


def expected_rows(case: dict) -> list[tuple]:
    columns = DAILY_COLUMNS if case["procedure"] == "sp_DailyTransaction" else BALANCE_COLUMNS
    return [tuple(None if r[c] is None else str(r[c]) for c in columns) for r in case["rows"]]


def sort_key(row):
    return tuple((v is not None, v) for v in row)


def check(execute, settings: Settings, cases: list[dict], label: str) -> int:
    failures = 0
    for case in cases:
        got, want = call(execute, settings, case), expected_rows(case)
        if case["procedure"] == "sp_BalancePerCustomer":  # the procedure has no ORDER BY
            got, want = sorted(got, key=sort_key), sorted(want, key=sort_key)
        ok = got == want
        failures += not ok
        print(f"[{'OK' if ok else 'MISMATCH'}] {label} {case['procedure']}{tuple(case['params'])}: {len(got)} rows")
        if not ok:
            print(f"    got:  {got}\n    want: {want}")
    return failures


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema-prefix", required=True, help="Prefix of the isolated parity schemas, e.g. banking_etl_t9_")
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--gold-prefix", help="Also check the dwh cases against <catalog>.<gold-prefix>gold functions")
    ap.add_argument("--drop", action="store_true", help="Drop the isolated parity schemas afterwards")
    a = ap.parse_args()

    def execute(statement: str, parameters: list | None = None) -> dict:
        body = {"warehouse_id": a.warehouse_id, "statement": statement, "wait_timeout": "50s",
                "on_wait_timeout": "CONTINUE", "parameters": parameters or []}
        resp = run_cli(["api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)])
        while resp["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(2)
            resp = run_cli(["api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}"])
        if resp["status"]["state"] != "SUCCEEDED":
            err = resp["status"].get("error", {})
            raise RuntimeError(f"{err.get('error_code')}: {err.get('message')}\n{statement[:300]}")
        return resp

    failures = 0
    for scenario in ("dwh", "dwh_edge"):
        data = json.loads((EXPECTED / f"sqlserver_{scenario}.json").read_text(encoding="utf-8"))
        settings = Settings(catalog=a.catalog, schema_prefix=f"{a.schema_prefix}{scenario}_")
        load_tables(execute, settings, data["tables"])
        for statement in render_functions(settings):
            execute(statement)
        failures += check(execute, settings, data["cases"], settings.schema("gold"))
        if a.drop:
            execute(f"DROP SCHEMA IF EXISTS {settings.schema('gold')} CASCADE")
    if a.gold_prefix is not None:
        data = json.loads((EXPECTED / "sqlserver_dwh.json").read_text(encoding="utf-8"))
        settings = Settings(catalog=a.catalog, schema_prefix=a.gold_prefix)
        failures += check(execute, settings, data["cases"], settings.schema("gold"))
    print(f"{failures} mismatches")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
