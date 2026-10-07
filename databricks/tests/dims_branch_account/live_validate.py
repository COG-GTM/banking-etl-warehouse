"""Live validation of ticket 5 (dim_branch / dim_account) on Databricks.

Not collected by pytest. Seeds a ticket-scoped schema from the local SQL Server
extracts, uploads the package + notebooks, runs them via ``jobs submit`` on
serverless, and checks exact parity against databricks/fixtures/parity/ using the
SQL warehouse statement-execution API.

    python live_validate.py all            # seed + upload + run + parity
    python live_validate.py scd1           # live SCD-1 change/revert round trip

Auth: DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET (OAuth M2M).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.workspace import ImportFormat, Language

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))
from t5_dims_helpers import (  # noqa: E402
    parity_account_rows,
    parity_branch_rows,
    source_account_rows,
    source_branch_rows,
)

DATABRICKS_ROOT = HERE.parents[1]
CATALOG = os.environ.get("T5_CATALOG", "migration_demo")
SCHEMA = os.environ.get("T5_SCHEMA", "banking_mig_t5")
WORKSPACE_ROOT = os.environ.get(
    "T5_WORKSPACE_ROOT", "/Workspace/Shared/banking_etl_migration_v2/ticket_5"
)
FQ = f"{CATALOG}.{SCHEMA}"

w = WorkspaceClient()


def warehouse_id() -> str:
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if wid:
        return wid
    return next(iter(w.warehouses.list())).id


def sql(statement: str):
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id(), wait_timeout="50s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{resp.status.state}: {resp.status.error} :: {statement[:200]}")
    return (resp.result.data_array or []) if resp.result else []


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int,)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def seed() -> None:
    sql(
        f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT "
        "'Ticket 5 scoped schema: bronze seed + silver/gold outputs for dim_branch/dim_account'"
    )
    branch_values = ", ".join(f"({lit(a)}, {lit(b)}, {lit(c)})" for a, b, c in source_branch_rows())
    sql(
        f"CREATE OR REPLACE TABLE {FQ}.sqlserver_branch AS SELECT "
        "CAST(c1 AS INT) branch_id, CAST(c2 AS STRING) branch_name, CAST(c3 AS STRING) branch_location "
        f"FROM VALUES {branch_values} AS v(c1, c2, c3)"
    )
    account_values = ", ".join(
        f"({lit(a)}, {lit(b)}, {lit(c)}, {lit(d)}, {lit(e.strftime('%Y-%m-%d %H:%M:%S'))}, {lit(f)})"
        for a, b, c, d, e, f in source_account_rows()
    )
    sql(
        f"CREATE OR REPLACE TABLE {FQ}.sqlserver_account AS SELECT "
        "CAST(c1 AS INT) account_id, CAST(c2 AS INT) customer_id, CAST(c3 AS STRING) account_type, "
        "CAST(c4 AS INT) balance, CAST(c5 AS TIMESTAMP_NTZ) date_opened, CAST(c6 AS STRING) status "
        f"FROM VALUES {account_values} AS v(c1, c2, c3, c4, c5, c6)"
    )
    print(json.dumps({
        "seeded": FQ,
        "sqlserver_branch": sql(f"SELECT COUNT(*) FROM {FQ}.sqlserver_branch")[0][0],
        "sqlserver_account": sql(f"SELECT COUNT(*) FROM {FQ}.sqlserver_account")[0][0],
    }))


def upload() -> None:
    src = DATABRICKS_ROOT / "src" / "banking_etl"
    targets = [src / "__init__.py", src / "dims" / "__init__.py", src / "dims" / "branch.py", src / "dims" / "account.py"]
    for f in targets:
        dest = f"{WORKSPACE_ROOT}/src/{f.relative_to(DATABRICKS_ROOT / 'src')}"
        w.workspace.mkdirs(os.path.dirname(dest))
        w.workspace.upload(dest, f.read_bytes(), format=ImportFormat.AUTO, overwrite=True)
    w.workspace.mkdirs(f"{WORKSPACE_ROOT}/notebooks")
    for name in ("dim_branch", "dim_account"):
        w.workspace.upload(
            f"{WORKSPACE_ROOT}/notebooks/{name}",
            (DATABRICKS_ROOT / "notebooks" / f"{name}.py").read_bytes(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
    print(json.dumps({"uploaded": WORKSPACE_ROOT}))


def submit(label: str, steps: str = "silver,gold") -> dict:
    params = {
        "catalog": CATALOG,
        "bronze_schema": SCHEMA,
        "silver_schema": SCHEMA,
        "gold_schema": SCHEMA,
        "steps": steps,
        "src_path": f"{WORKSPACE_ROOT}/src",
    }
    tasks = [
        jobs.SubmitTask(
            task_key="dim_branch",
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{WORKSPACE_ROOT}/notebooks/dim_branch", base_parameters=params
            ),
        ),
        jobs.SubmitTask(
            task_key="dim_account",
            depends_on=[jobs.TaskDependency(task_key="dim_branch")],
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{WORKSPACE_ROOT}/notebooks/dim_account", base_parameters=params
            ),
        ),
    ]
    run = w.jobs.submit(run_name=f"ticket-5 dims branch/account ({label})", tasks=tasks).result(
        timeout=__import__("datetime").timedelta(minutes=30)
    )
    out = {"label": label, "run_id": run.run_id, "state": str(run.state.result_state), "url": run.run_page_url, "tasks": {}}
    for t in run.tasks:
        o = w.jobs.get_run_output(t.run_id)
        out["tasks"][t.task_key] = {
            "task_run_id": t.run_id,
            "result": json.loads(o.notebook_output.result) if o.notebook_output and o.notebook_output.result else o.error,
        }
    print(json.dumps(out, indent=2, default=str))
    if run.state.result_state != jobs.RunResultState.SUCCESS:
        raise SystemExit("run failed")
    return out


def _parity_values(rows, types):
    return ", ".join(
        "(" + ", ".join(f"CAST({lit(v if not hasattr(v, 'isoformat') else v.isoformat())} AS {t})" for v, t in zip(r, types)) + ")"
        for r in rows
    )


def parity() -> dict:
    result = {}
    specs = {
        "dim_branch": (parity_branch_rows(), ["INT", "STRING", "STRING"], "branch_id, branch_name, branch_location"),
        "dim_account": (
            parity_account_rows(),
            ["INT", "INT", "STRING", "DECIMAL(19,4)", "DATE", "STRING"],
            "account_id, customer_id, account_type, balance, date_opened, status",
        ),
    }
    for table, (rows, types, cols) in specs.items():
        expected = f"SELECT * FROM VALUES {_parity_values(rows, types)} AS e({cols})"
        gold = f"SELECT {cols} FROM {FQ}.{table}"
        missing = sql(f"SELECT COUNT(*) FROM ({expected} EXCEPT ALL {gold})")[0][0]
        extra = sql(f"SELECT COUNT(*) FROM ({gold} EXCEPT ALL {expected})")[0][0]
        count = sql(f"SELECT COUNT(*) FROM {FQ}.{table}")[0][0]
        schema = [(r[0], r[1]) for r in sql(f"DESCRIBE TABLE {FQ}.{table}") if r[0] and not r[0].startswith("#")]
        result[table] = {"gold_rows": int(count), "expected_rows": len(rows), "missing": int(missing), "extra": int(extra), "schema": schema}
    acct = sql(f"SELECT COUNT(*), SUM(balance), MIN(date_opened), MAX(date_opened) FROM {FQ}.dim_account")[0]
    result["dim_account"]["aggregates"] = acct
    ok = all(v["missing"] == 0 and v["extra"] == 0 and v["gold_rows"] == v["expected_rows"] for v in result.values())
    result["exact_parity"] = ok
    print(json.dumps(result, indent=2))
    if not ok:
        raise SystemExit("parity FAILED")
    return result


def scd1() -> None:
    """Change bronze, rerun, check SCD-1 metrics; then revert and restore parity."""
    sql(f"UPDATE {FQ}.sqlserver_branch SET branch_name = 'KC Jakarta Pusat' WHERE branch_id = 1")
    sql(f"INSERT INTO {FQ}.sqlserver_branch VALUES (6, 'KC Bandung', 'Jl. Asia Afrika No 8')")
    sql(f"UPDATE {FQ}.sqlserver_account SET balance = 1750000, status = 'terminated' WHERE account_id = 1")
    changed = submit("scd1-change")
    b = changed["tasks"]["dim_branch"]["result"]["gold_merge"]
    a = changed["tasks"]["dim_account"]["result"]["gold_merge"]
    assert b["numTargetRowsUpdated"] == 1 and b["numTargetRowsInserted"] == 1 and b["gold_rows"] == 6, b
    assert a["numTargetRowsUpdated"] == 1 and a.get("numTargetRowsInserted", 0) == 0, a
    print(json.dumps({"after_change": sql(
        f"SELECT b.branch_id, b.branch_name, a.balance, a.status FROM {FQ}.dim_branch b "
        f"JOIN {FQ}.dim_account a ON a.account_id = 1 WHERE b.branch_id IN (1, 6) ORDER BY 1")}))
    seed()
    reverted = submit("scd1-revert")
    rb = reverted["tasks"]["dim_branch"]["result"]["gold_merge"]
    assert rb["numTargetRowsUpdated"] == 1 and rb["gold_rows"] == 6, rb
    # SCD-1 never deletes (legacy INSERT-only job); drop the synthetic test row explicitly.
    sql(f"DELETE FROM {FQ}.dim_branch WHERE branch_id = 6")
    parity()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["seed", "upload", "run", "parity", "all", "scd1"])
    args = p.parse_args()
    if args.cmd in ("seed", "all"):
        seed()
    if args.cmd in ("upload", "all"):
        upload()
    if args.cmd in ("run", "all"):
        submit("full")
    if args.cmd in ("parity", "all"):
        parity()
    if args.cmd == "scd1":
        scd1()


if __name__ == "__main__":
    main()
