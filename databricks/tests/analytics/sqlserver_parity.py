#!/usr/bin/env python3
"""Build the SQL Server 2022 expectations for ticket 9 (sp_DailyTransaction, sp_BalancePerCustomer).

Against a SQL Server 2022 Docker container (sqlcmd at /opt/mssql-tools18/bin/sqlcmd) this:
  1. restores data_sources/sample.bak as database `sample`;
  2. builds DWH from sql_scripts/01_create_tables.sql + 02_create_procedures.sql (procedure bodies verbatim)
     and loads it like the Talend jobs: dims via INSERT ... SELECT (customer UPPER + LEFT JOIN city/state),
     fact = tUnite(transaction_db, Excel, CSV) -> tUniqRow(transaction_id, keep first), one INSERT per row
     with DIE_ON_ERROR=false semantics (a failing row is skipped);
  3. builds DWH_EDGE from the same scripts with synthetic edge rows (case / trailing spaces, NULLs,
     LIKE wildcards, backslashes, 4-decimal MONEY);
  4. runs the ORIGINAL procedures for every case in CASES and writes the DWH tables plus the procedure
     results as JSON (exact strings, MONEY as decimals) to tests/analytics/expected/sqlserver_<scenario>.json.

  docker run -d --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=... mcr.microsoft.com/mssql/server:2022-latest
  MSSQL_SA_PASSWORD=... python tests/analytics/sqlserver_parity.py --container mssql
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import subprocess
from decimal import Decimal
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[2]  # databricks/
REPO = ROOT.parent
OUT = Path(__file__).resolve().parent / "expected"
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"

TABLES = {
    "DimBranch": "BranchID",
    "DimCustomer": "CustomerID",
    "DimAccount": "AccountID",
    "FactTransaction": "TransactionID",
}

LONG_NAME = "%" * 95 + "SHELL" + "Q"  # 101 chars: VARCHAR(100) truncation drops the Q -> matches SHELLY
CASES = {
    "DWH": [
        ("sp_DailyTransaction", ["2024-01-18", "2024-01-20"]),
        ("sp_DailyTransaction", ["2022-01-01", "2024-12-31"]),
        ("sp_DailyTransaction", ["2024-01-22", "2024-01-22"]),
        ("sp_DailyTransaction", ["2024-01-20", "2024-01-18"]),
        ("sp_DailyTransaction", [None, "2024-01-20"]),
        ("sp_DailyTransaction", ["2023-01-01", "2023-12-31"]),
        ("sp_BalancePerCustomer", ["SHELLY"]),
        ("sp_BalancePerCustomer", ["shelly juwita"]),
        ("sp_BalancePerCustomer", ["ma"]),
        ("sp_BalancePerCustomer", ["Ani"]),
        ("sp_BalancePerCustomer", ["ria"]),
        ("sp_BalancePerCustomer", ["adam"]),
        ("sp_BalancePerCustomer", ["agung"]),
        ("sp_BalancePerCustomer", [""]),
        ("sp_BalancePerCustomer", [None]),
        ("sp_BalancePerCustomer", ["zzz"]),
        ("sp_BalancePerCustomer", ["_A_"]),
        ("sp_BalancePerCustomer", ["%"]),
        ("sp_BalancePerCustomer", ["R%O"]),
        ("sp_BalancePerCustomer", [LONG_NAME]),
    ],
    "DWH_EDGE": [
        ("sp_DailyTransaction", ["2024-01-01", "2024-01-31"]),
        ("sp_DailyTransaction", ["2024-01-02", "2024-01-02"]),
        ("sp_DailyTransaction", ["2024-01-01", None]),
        ("sp_DailyTransaction", ["2023-12-31", "2024-02-01"]),
        ("sp_BalancePerCustomer", [""]),
        ("sp_BalancePerCustomer", [None]),
        ("sp_BalancePerCustomer", ["alice"]),
        ("sp_BalancePerCustomer", ["O'NEIL"]),
        ("sp_BalancePerCustomer", ["lower"]),
        ("sp_BalancePerCustomer", ["LOWER"]),
        ("sp_BalancePerCustomer", ["50%"]),
        ("sp_BalancePerCustomer", ["0%_"]),
        ("sp_BalancePerCustomer", ["K\\S"]),
        ("sp_BalancePerCustomer", ["\\"]),
        ("sp_BalancePerCustomer", ["  "]),
        ("sp_BalancePerCustomer", ["nobody"]),
    ],
}

EDGE_ROWS = """
INSERT INTO DimBranch VALUES (1, 'KC TEST', 'Jakarta');
INSERT INTO DimCustomer VALUES
 (1, 'ALICE O''NEIL', NULL, NULL, NULL, 30, 'FEMALE', NULL),
 (2, NULL, NULL, NULL, NULL, 31, 'MALE', NULL),
 (3, 'bob lower', NULL, NULL, NULL, 32, 'male', NULL),
 (4, '50% OFF_GUY', NULL, NULL, NULL, 33, 'MALE', NULL),
 (5, 'BACK\\SLASH', NULL, NULL, NULL, 34, 'MALE', NULL),
 (6, 'TWO  SPACES', NULL, NULL, NULL, 35, 'MALE', NULL);
INSERT INTO DimAccount VALUES
 (1, 1, 'saving', 100.1234, '2020-01-01', 'ACTIVE '),
 (2, 1, 'checking', 200.0000, '2020-01-01', 'Active'),
 (3, 1, 'saving', 5.0000, '2020-01-01', 'terminated'),
 (4, 2, 'saving', 300.0000, '2020-01-01', 'active'),
 (5, 3, 'saving', 400.0000, '2020-01-01', 'active'),
 (6, 3, 'loan', NULL, '2020-01-01', 'active'),
 (7, 4, 'saving', 10.0000, '2020-01-01', 'active'),
 (8, 5, 'saving', 10.0000, '2020-01-01', 'active'),
 (9, NULL, 'orphan', 1.0000, '2020-01-01', 'active'),
 (10, 6, 'saving', 0.0001, '2020-01-01', NULL),
 (11, 6, 'checking', 7.5000, '2020-01-01', ' active');
INSERT INTO FactTransaction VALUES
 (1, 1, '2024-01-01 00:00:00', 10.0001, 'Deposit', 1),
 (2, 1, '2024-01-01 23:59:59', 0.0002, 'deposit', 1),
 (3, 1, '2024-01-02 08:00:00', 1.5000, 'DEPOSIT  ', 1),
 (4, 1, '2024-01-02 09:00:00', 3.2500, 'Withdrawal', 1),
 (5, 2, '2024-01-02 10:00:00', 20.0000, NULL, 1),
 (6, 2, '2024-01-03 10:00:00', NULL, 'Deposit', 1),
 (7, 5, NULL, 50.0000, 'Deposit', 1),
 (8, 5, '2024-02-01 00:00:00', 0.1234, ' Deposit', 1),
 (9, 6, '2024-01-31 23:59:59', 9.9999, 'Deposit', NULL),
 (10, NULL, '2024-01-15 12:00:00', 100.0000, 'Deposit', 1),
 (11, 8, '2023-12-31 23:59:59', 1.0000, 'Payment', 1),
 (12, 11, '2024-01-15 12:00:00', 2.0000, 'Transfer', 1);
"""


class SqlServer:
    def __init__(self, container: str, password: str):
        self.container = container
        self.password = password

    def run(self, sql: str, database: str = "master", *, check: bool = True) -> str:
        cmd = ["docker", "exec", "-i", self.container, SQLCMD, "-C", "-S", "localhost", "-U", "sa",
               "-P", self.password, "-d", database, "-y", "0"]
        if check:
            cmd.append("-b")
        res = subprocess.run(cmd, input=sql, capture_output=True, text=True)
        if check and res.returncode:
            raise RuntimeError(f"sqlcmd failed ({res.returncode}): {res.stdout}\n{res.stderr}")
        return res.stdout

    def json_rows(self, select: str, database: str) -> list[dict]:
        """Run ``select`` FOR JSON (exact strings / MONEY) and parse the rows."""
        out = self.run(f"SET NOCOUNT ON; {select} FOR JSON PATH, INCLUDE_NULL_VALUES;", database)
        # Drop informational messages (e.g. "Warning: Null value is eliminated by an aggregate").
        text = "".join(line.strip("\r\n") for line in out.splitlines() if not line.startswith("Warning:")).strip()
        return json.loads(text, parse_float=Decimal) if text else []


def literal(value: str | None) -> str:
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


def talend_fact_rows(db: SqlServer) -> list[dict]:
    """tUnite(transaction_db, Excel, CSV) -> tUniqRow(transaction_id, keep first)."""
    rows: list[dict] = []
    for r in db.json_rows("SELECT * FROM dbo.transaction_db ORDER BY transaction_id", "sample"):
        r["transaction_date"] = r["transaction_date"].replace("T", " ")[:19]
        rows.append({k: str(v) for k, v in r.items()})
    sheet = openpyxl.load_workbook(REPO / "data_sources" / "transaction_excel.xlsx", read_only=True)["Sheet1"]
    values = sheet.iter_rows(values_only=True)
    header = next(values)
    for v in values:
        row = dict(zip(header, v))
        row["transaction_date"] = row["transaction_date"].strftime("%Y-%m-%d %H:%M:%S")
        rows.append({k: str(x) for k, x in row.items()})
    with (REPO / "data_sources" / "transaction_csv.csv").open(newline="", encoding="latin-1") as fh:
        for row in csv.DictReader(fh):
            parsed = dt.datetime.strptime(row["transaction_date"], "%d-%m-%Y %H:%M:%S")
            row["transaction_date"] = parsed.strftime("%Y-%m-%d %H:%M:%S")
            rows.append(row)
    seen: set[str] = set()
    unique = []
    for row in rows:
        if row["transaction_id"] not in seen:
            seen.add(row["transaction_id"])
            unique.append(row)
    return unique


def create_dwh(db: SqlServer, name: str) -> None:
    db.run(f"IF DB_ID('{name}') IS NOT NULL BEGIN ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
           f"DROP DATABASE [{name}]; END")
    for script in ("01_create_tables.sql", "02_create_procedures.sql"):
        sql = (REPO / "sql_scripts" / script).read_text(encoding="utf-8")
        # 02_create_procedures.sql puts a PRINT in the same batch as each CREATE PROCEDURE (Msg 111);
        # split the batch so the procedure bodies run verbatim.
        sql = re.sub(r"^(PRINT [^\n]*;)\s*\n(CREATE PROCEDURE)", r"\1\nGO\n\2", sql, flags=re.MULTILINE)
        db.run(re.sub(r"\bDWH\b", name, sql))


def load_dwh(db: SqlServer) -> None:
    # Talend-style queries run in the `sample` context (a `sample.dbo.x` 3-part column prefix gives Msg 4104).
    db.run("""
INSERT INTO DWH.dbo.DimBranch (BranchID, BranchName, BranchLocation)
  SELECT branch_id, branch_name, branch_location FROM dbo.branch;
INSERT INTO DWH.dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
  SELECT account_id, customer_id, account_type, balance, date_opened, status FROM dbo.account;
INSERT INTO DWH.dbo.DimCustomer (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email)
  SELECT dbo.customer.customer_id, UPPER(dbo.customer.customer_name), UPPER(dbo.customer.address),
         dbo.city.city_name, dbo.state.state_name, dbo.customer.age, UPPER(dbo.customer.gender), dbo.customer.email
  FROM dbo.customer
  LEFT JOIN dbo.city ON dbo.customer.city_id = dbo.city.city_id
  LEFT JOIN dbo.state ON dbo.city.state_id = dbo.state.state_id;
""", "sample")
    batches = [
        "INSERT INTO FactTransaction VALUES ({transaction_id}, {account_id}, '{transaction_date}', {amount}, "
        "'{transaction_type}', {branch_id});\nGO".format(**r)
        for r in talend_fact_rows(db)
    ]
    db.run("\n".join(batches), "DWH", check=False)  # DIE_ON_ERROR=false: failing rows are skipped


def run_case(db: SqlServer, database: str, proc: str, params: list) -> list[dict]:
    if proc == "sp_DailyTransaction":
        temp = "CREATE TABLE #r ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);"
        order = " ORDER BY [Date]"
    else:
        temp = ("CREATE TABLE #r (CustomerName VARCHAR(100), AccountType VARCHAR(50), "
                "InitialBalance MONEY, CurrentBalance MONEY);")
        order = " ORDER BY CustomerName, AccountType, InitialBalance"
    args = ", ".join(literal(p) for p in params)
    return db.json_rows(f"{temp} INSERT INTO #r EXEC {proc} {args}; SELECT * FROM #r{order}", database)


def dump(db: SqlServer, database: str) -> dict:
    return {
        "tables": {t: db.json_rows(f"SELECT * FROM {t} ORDER BY {k}", database) for t, k in TABLES.items()},
        "cases": [
            {"procedure": proc, "params": params, "rows": run_case(db, database, proc, params)}
            for proc, params in CASES[database]
        ],
    }


def default(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--container", required=True)
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()
    db = SqlServer(a.container, os.environ["MSSQL_SA_PASSWORD"])

    subprocess.run(["docker", "cp", str(REPO / "data_sources" / "sample.bak"), f"{a.container}:/tmp/sample.bak"],
                   check=True)
    db.run("RESTORE DATABASE sample FROM DISK='/tmp/sample.bak' WITH MOVE 'sample' TO "
           "'/var/opt/mssql/data/sample.mdf', MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf', REPLACE")
    create_dwh(db, "DWH")
    load_dwh(db)
    create_dwh(db, "DWH_EDGE")
    db.run(EDGE_ROWS, "DWH_EDGE")

    a.out.mkdir(parents=True, exist_ok=True)
    version = db.run("SET NOCOUNT ON; SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(30)) + ' ' + "
                     "CAST(DATABASEPROPERTYEX('DWH', 'Collation') AS VARCHAR(60));").split()
    for database, scenario in (("DWH", "dwh"), ("DWH_EDGE", "dwh_edge")):
        payload = {"source": f"SQL Server {version[0]} ({version[1]}) database {database}", **dump(db, database)}
        path = a.out / f"sqlserver_{scenario}.json"
        path.write_text(json.dumps(payload, indent=1, default=default) + "\n", encoding="utf-8")
        print(f"wrote {path}: {len(payload['cases'])} cases, "
              + ", ".join(f"{t}={len(r)}" for t, r in payload["tables"].items()))


if __name__ == "__main__":
    main()
