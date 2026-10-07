"""Seed the ticket-scoped schema ``migration_demo.banking_mig_t7`` for a live run of ticket 7.

Creates bronze-shaped copies of the three transaction streams, the two gold dims the FK check needs,
and ``parity_fact_transaction`` (from databricks/fixtures/parity/fact_transaction.csv) through the
SQL warehouse statement-execution API. It touches only the target schema.

    python databricks/tests/facts/seed_live_fixtures.py [--schema banking_mig_t7] [--warehouse-id ...]
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from conftest import PARITY_CSV, csv_rows, excel_rows

FIXTURES = HERE / "fixtures"


def lit(v, sql_type: str) -> str:
    if v is None or v == "":
        return f"CAST(NULL AS {sql_type})"
    s = str(v).replace("'", "''")
    if sql_type == "STRING":
        return f"'{s}'"
    return f"CAST('{s}' AS {sql_type})"


def values_table(fq: str, columns: list[tuple[str, str]], rows: list[tuple], comment: str, extra: str = "") -> str:
    vals = ",\n".join("(" + ", ".join(lit(v, t) for v, (_, t) in zip(r, columns)) + ")" for r in rows)
    names = ", ".join(c for c, _ in columns)
    return (
        f"CREATE OR REPLACE TABLE {fq} COMMENT '{comment}' AS SELECT {names}{extra} FROM VALUES\n{vals}\nAS v({names})"
    )


def read_csv(path: Path) -> list[tuple]:
    with open(path, newline="") as fh:
        return [tuple(r.values()) for r in csv.DictReader(fh)]


def main() -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="migration_demo")
    ap.add_argument("--schema", default="banking_mig_t7")
    ap.add_argument("--warehouse-id")
    args = ap.parse_args()

    w = WorkspaceClient()
    wh = args.warehouse_id or next(iter(w.warehouses.list())).id
    s = f"{args.catalog}.{args.schema}"
    tag = "ticket 7 live fixture"

    fact_cols = [
        ("transaction_id", "INT"),
        ("account_id", "INT"),
        ("transaction_date", "TIMESTAMP"),
        ("amount", "INT"),
        ("transaction_type", "STRING"),
        ("branch_id", "INT"),
    ]
    excel_cols = [
        ("transaction_id", "BIGINT"),
        ("account_id", "BIGINT"),
        ("transaction_date", "TIMESTAMP"),
        ("amount", "BIGINT"),
        ("transaction_type", "STRING"),
        ("branch_id", "BIGINT"),
    ]
    str_cols = [(c, "STRING") for c, _ in fact_cols]
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {s} COMMENT 'Ticket 7 (Load_FactTransaction) scratch schema for live validation'",
        values_table(
            f"{s}.sqlserver_transaction_db",
            fact_cols,
            read_csv(FIXTURES / "sqlserver_transaction_db.csv"),
            tag,
            ", current_timestamp() AS _ingested_at",
        ),
        values_table(
            f"{s}.file_transaction_excel",
            excel_cols,
            [tuple(v.strftime("%Y-%m-%d %H:%M:%S") if hasattr(v, "strftime") else v for v in r) for r in excel_rows()],
            tag,
        ),
        values_table(f"{s}.file_transaction_csv", str_cols, csv_rows(), tag),
        values_table(
            f"{s}.dim_account",
            [
                ("account_id", "INT"),
                ("customer_id", "INT"),
                ("account_type", "STRING"),
                ("balance", "DECIMAL(19,4)"),
                ("date_opened", "DATE"),
                ("status", "STRING"),
            ],
            read_csv(FIXTURES / "dim_account.csv"),
            tag,
        ),
        values_table(
            f"{s}.dim_branch",
            [("branch_id", "INT"), ("branch_name", "STRING"), ("branch_location", "STRING")],
            read_csv(FIXTURES / "dim_branch.csv"),
            tag,
        ),
        values_table(
            f"{s}.parity_fact_transaction",
            [
                ("transaction_id", "INT"),
                ("account_id", "INT"),
                ("transaction_date", "TIMESTAMP"),
                ("amount", "DECIMAL(19,4)"),
                ("transaction_type", "STRING"),
                ("branch_id", "INT"),
            ],
            read_csv(PARITY_CSV),
            "Legacy SQL Server DWH.dbo.FactTransaction after Talend Load_FactTransaction",
        ),
    ]
    for sql in statements:
        resp = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="50s")
        while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(3)
            resp = w.statement_execution.get_statement(resp.statement_id)
        if resp.status.state != StatementState.SUCCEEDED:
            raise SystemExit(f"FAILED: {sql[:120]}\n{resp.status.error}")
        print("ok:", sql.splitlines()[0][:110])


if __name__ == "__main__":
    main()
