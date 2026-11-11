"""Regenerate ``expected.json`` by running the original T-SQL on a real SQL Server.

Runs the repository's unmodified ``sql_scripts/01_create_tables.sql`` and the verbatim
``sp_DailyTransaction`` definition from ``sql_scripts/02_create_procedures.sql``, loads ``fact_transaction.csv`` into
``FactTransaction`` and records ``EXEC sp_DailyTransaction`` for every case in ``cases.json``.

    docker run -d --name mssql-oracle -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=... \\
        mcr.microsoft.com/mssql/server:2022-latest
    MSSQL_SA_PASSWORD=... python generate_expected.py [--container mssql-oracle]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
MARKER = "##CASE "


def sql_literal(value: str | None, quoted: bool) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + value.replace("'", "''") + "'" if quoted else value


def build_script(rows: list[dict], cases: list[dict]) -> str:
    account_ids = sorted({int(r["account_id"]) for r in rows if r["account_id"]})
    branch_ids = sorted({int(r["branch_id"]) for r in rows if r["branch_id"]})
    out = [
        "USE DWH;",
        "SET NOCOUNT ON;",
        *(f"INSERT INTO DimBranch (BranchID) VALUES ({b});" for b in branch_ids),
        *(f"INSERT INTO DimAccount (AccountID) VALUES ({a});" for a in account_ids),
    ]
    for r in rows:
        out.append(
            "INSERT INTO FactTransaction (TransactionID, AccountID, TransactionDate, Amount, "
            "TransactionType, BranchID) VALUES ("
            + ", ".join(
                [
                    sql_literal(r["transaction_id"], False),
                    sql_literal(r["account_id"], False),
                    sql_literal(r["transaction_date"], True),
                    sql_literal(r["amount"], False),
                    sql_literal(r["transaction_type"], True),
                    sql_literal(r["branch_id"], False),
                ]
            )
            + ");"
        )
    out.append("GO")
    for case in cases:
        out.append(f"PRINT '{MARKER}{case['name']}';")
        out.append(
            "EXEC sp_DailyTransaction "
            f"@start_date = {sql_literal(case['start_date'], True)}, "
            f"@end_date = {sql_literal(case['end_date'], True)};"
        )
        out.append("GO")
    return "\n".join(out) + "\n"


def sqlcmd(container: str, password: str, script: str, database: str = "master") -> str:
    path = "/tmp/sp_daily_transaction_oracle.sql"
    subprocess.run(
        ["docker", "exec", "-i", container, "bash", "-c", f"cat > {path}"],
        input=script,
        text=True,
        check=True,
    )
    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-C",
            "-b",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-P",
            password,
            "-d",
            database,
            "-W",
            "-h",
            "-1",
            "-s",
            "|",
            "-i",
            path,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def parse_results(output: str) -> dict:
    results: dict[str, list] = {}
    current = None
    for line in output.splitlines():
        line = line.strip()
        if line.startswith(MARKER):
            current = line[len(MARKER) :]
            results[current] = []
        elif current is not None and line:
            date, count, amount = line.split("|")
            results[current].append(
                {
                    "date": date,
                    "total_transactions": int(count),
                    "total_amount": None if amount == "NULL" else amount,
                }
            )
    return results


def extract_procedure() -> str:
    """The verbatim ``CREATE PROCEDURE sp_DailyTransaction`` batch from 02_create_procedures.sql.

    The script itself can't be run as-is: each ``CREATE PROCEDURE`` is preceded by a
    ``PRINT`` in the same batch, which SQL Server rejects (Msg 111), so the procedure
    definition is executed as its own batch.
    """
    text = (REPO / "sql_scripts/02_create_procedures.sql").read_text()
    match = re.search(
        r"^CREATE PROCEDURE sp_DailyTransaction\b.*?^END;", text, re.DOTALL | re.MULTILINE
    )
    if not match:
        raise RuntimeError("sp_DailyTransaction definition not found")
    return match.group(0) + "\nGO\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="mssql-oracle")
    args = parser.parse_args()
    password = os.environ["MSSQL_SA_PASSWORD"]

    with open(HERE / "fact_transaction.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    cases = json.loads((HERE / "cases.json").read_text())

    reset = "IF DB_ID('DWH') IS NOT NULL BEGIN ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE DWH; END\nGO\n"
    sqlcmd(args.container, password, reset)
    sqlcmd(args.container, password, (REPO / "sql_scripts/01_create_tables.sql").read_text())
    sqlcmd(args.container, password, extract_procedure(), database="DWH")
    output = sqlcmd(args.container, password, build_script(rows, cases), database="DWH")
    version = sqlcmd(args.container, password, "SET NOCOUNT ON; SELECT @@VERSION;").splitlines()[0]

    results = parse_results(output)
    missing = [c["name"] for c in cases if c["name"] not in results]
    if missing:
        raise RuntimeError(f"No result captured for cases: {missing}\n{output}")
    expected = {"generated_by": version.strip(), "cases": results}
    (HERE / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")
    print(f"Wrote {len(results)} cases to expected.json ({version.strip()})")


if __name__ == "__main__":
    main()
