"""Regenerate ``fixtures/expected.json`` by running the ORIGINAL T-SQL on SQL Server.

The repo's own ``sql_scripts/01_create_tables.sql`` and ``02_create_procedures.sql`` are
executed unchanged (except for the database name and splitting each ``PRINT`` from the
``CREATE PROCEDURE`` that follows it into its own batch, which SQL Server requires),
``fixtures/fixture.json`` is loaded,
and ``EXEC sp_BalancePerCustomer`` is run for every search in the fixture.

    docker run -d --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" \
        -p 14333:1433 mcr.microsoft.com/mssql/server:2022-latest
    pip install pymssql
    MSSQL_SA_PASSWORD=... python generate_expected.py --server localhost --port 14333
"""

from __future__ import annotations

import argparse
import json
import os
import re
from decimal import Decimal
from pathlib import Path

import pymssql

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
FIXTURE = HERE / "fixtures" / "fixture.json"
EXPECTED = HERE / "fixtures" / "expected.json"
DATABASE = "DWH_SP_BALANCE_PER_CUSTOMER_FIXTURE"

TABLES = {
    "dim_branch": ("DimBranch", {"branch_id": "BranchID", "branch_name": "BranchName",
                                 "branch_location": "BranchLocation"}),
    "dim_customer": ("DimCustomer", {"customer_id": "CustomerID", "customer_name": "CustomerName",
                                     "address": "Address", "city_name": "CityName",
                                     "state_name": "StateName", "age": "Age", "gender": "Gender",
                                     "email": "Email"}),
    "dim_account": ("DimAccount", {"account_id": "AccountID", "customer_id": "CustomerID",
                                   "account_type": "AccountType", "balance": "Balance",
                                   "date_opened": "DateOpened", "status": "Status"}),
    "fact_transaction": ("FactTransaction", {"transaction_id": "TransactionID",
                                             "account_id": "AccountID",
                                             "transaction_date": "TransactionDate",
                                             "amount": "Amount",
                                             "transaction_type": "TransactionType",
                                             "branch_id": "BranchID"}),
}


def _batches(path: Path) -> list[str]:
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace("name = 'DWH'", f"name = '{DATABASE}'")
    sql = re.sub(r"\bDATABASE DWH\b", f"DATABASE {DATABASE}", sql)
    sql = re.sub(r"\bUSE DWH\b", f"USE {DATABASE}", sql)
    batches = []
    for batch in re.split(r"^\s*GO\s*$", sql, flags=re.M | re.I):
        # 02_create_procedures.sql puts a PRINT before each CREATE PROCEDURE in the same
        # batch, which SQL Server rejects; run the PRINT as its own batch.
        match = re.search(r"^\s*CREATE\s+PROCEDURE\b", batch, flags=re.M | re.I)
        if match and batch[:match.start()].strip():
            batches.extend([batch[:match.start()], batch[match.start():]])
        else:
            batches.append(batch)
    return [b for b in batches if b.strip()]


def _money(value) -> str | None:
    return None if value is None else str(Decimal(value).quantize(Decimal("0.0001")))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="localhost")
    parser.add_argument("--port", type=int, default=1433)
    parser.add_argument("--user", default="sa")
    args = parser.parse_args()

    conn = pymssql.connect(server=args.server, port=args.port, user=args.user,
                           password=os.environ["MSSQL_SA_PASSWORD"], autocommit=True)
    cur = conn.cursor()
    cur.execute(f"IF DB_ID('{DATABASE}') IS NOT NULL BEGIN "
                f"ALTER DATABASE {DATABASE} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE {DATABASE}; END")
    for script in ("01_create_tables.sql", "02_create_procedures.sql"):
        for batch in _batches(REPO_ROOT / "sql_scripts" / script):
            cur.execute(batch)

    cur.execute(f"USE {DATABASE}")
    cur.execute("SELECT CONVERT(VARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation')), @@VERSION")
    collation, version = cur.fetchone()

    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for key, (table, columns) in TABLES.items():
        cols = list(columns)
        placeholders = ", ".join(["%s"] * len(cols))
        for row in fixture[key]:
            cur.execute(f"INSERT INTO {table} ({', '.join(columns[c] for c in cols)}) "
                        f"VALUES ({placeholders})", tuple(row[c] for c in cols))

    results = {}
    for search in fixture["searches"]:
        cur.execute("EXEC sp_BalancePerCustomer @customer_name = %s", (search["customer_name"],))
        rows = [
            {"customer_name": r[0], "account_type": r[1],
             "initial_balance": _money(r[2]), "current_balance": _money(r[3])}
            for r in cur.fetchall()
        ]
        rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
        results[search["id"]] = {"customer_name": search["customer_name"], "rows": rows}

    EXPECTED.write_text(json.dumps({
        "generated_by": "databricks/tests/sp_balance_per_customer/generate_expected.py",
        "sql_server_version": version.splitlines()[0].strip(),
        "database_collation": collation,
        "results": results,
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {EXPECTED} ({len(results)} searches, collation {collation})")


if __name__ == "__main__":
    main()
