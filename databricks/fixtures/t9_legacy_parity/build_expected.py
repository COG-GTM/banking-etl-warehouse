"""Rebuild the Ticket 9 legacy fixtures by running the ORIGINAL stored procedures on SQL Server.

Needs a SQL Server 2022 container with ``data_sources/sample.bak`` restorable, e.g.::

    docker run -d --name t9mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=... mcr.microsoft.com/mssql/server:2022-latest
    MSSQL_SA_PASSWORD=... python databricks/fixtures/t9_legacy_parity/build_expected.py

Steps (all inside the container, via sqlcmd):

1. Restore ``sample`` from ``sample.bak``.
2. ``legacy`` dataset: run ``sql_scripts/01_create_tables.sql`` (database ``DWH``), then the
   Talend-equivalent loads (Load_DimBranch / Load_DimAccount straight copies, Load_DimCustomer with
   LEFT OUTER city/state lookups and UPCASE on name/address/gender, Load_FactTransaction:
   transaction_db + Excel + CSV, tUnite in that order, tUniqRow on transaction_id keeping the first
   row, row-by-row INSERT with DIE_ON_ERROR=false so FK violations are rejected one row at a time).
3. ``edge`` dataset: the same DDL in database ``DWH_EDGE`` with hand-written rows that exercise the
   collation, NULL, wildcard, truncation and boundary semantics of the procedures.
4. ``sql_scripts/02_create_procedures.sql`` in both databases (with the ``GO`` the script is missing
   before each CREATE PROCEDURE), then every case in ``CASES`` is executed with
   ``INSERT INTO #r EXEC ...`` and captured with ``FOR JSON``.

Outputs, next to this script:
* ``<dataset>/<DimTable>.json``: the legacy DWH tables (legacy column names), used as Silver fixtures.
* ``expected.json``: result set of every procedure call.
"""

from __future__ import annotations

import csv
import json
import os
import re
import shlex
import subprocess
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CONTAINER = os.environ.get("T9_MSSQL_CONTAINER", "t9mssql")
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"
TABLES = ("DimBranch", "DimAccount", "DimCustomer", "FactTransaction")


def sqlcmd(script: str, database: str = "master") -> str:
    """Run a T-SQL script (GO batches allowed) in the container and return stdout."""
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8") as fh:
        fh.write(script)
        local = fh.name
    remote = f"/tmp/{Path(local).name}"
    os.chmod(local, 0o644)
    subprocess.run(["docker", "cp", local, f"{CONTAINER}:{remote}"], check=True)
    os.unlink(local)
    cmd = [
        "docker", "exec", CONTAINER, SQLCMD, "-C", "-S", "localhost", "-U", "sa",
        "-P", os.environ["MSSQL_SA_PASSWORD"], "-d", database, "-f", "65001",
        "-y", "0", "-i", remote,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if res.returncode != 0:
        raise RuntimeError(f"sqlcmd failed ({res.returncode}): {res.stdout}\n{res.stderr}")
    return res.stdout


def query_json(sql: str, database: str) -> list[dict]:
    """Run a SELECT ... FOR JSON PATH, INCLUDE_NULL_VALUES and parse it (exact decimals)."""
    out = sqlcmd(f"SET NOCOUNT ON;\n{sql}\n", database)
    lines = out.splitlines()
    if any(line.startswith("Msg ") for line in lines):
        raise RuntimeError(out)
    # -y 0 prints the FOR JSON column header and a dashed underline, then the JSON in chunks.
    # "Warning: Null value is eliminated by an aggregate" is informational (SUM over NULL amounts).
    text = "".join(line for line in lines if not line.startswith(("JSON_", "Warning: Null value"))
                   and not re.fullmatch(r"-+", line))
    return json.loads(text, parse_float=Decimal) if text else []


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, Decimal)):
        return str(v)
    if isinstance(v, datetime):
        return f"'{v.isoformat(sep=' ', timespec='milliseconds')}'"
    return "N'" + str(v).replace("'", "''") + "'"


# ---------------------------------------------------------------------------------------------
# Legacy dataset: Talend-equivalent load of the committed sources
# ---------------------------------------------------------------------------------------------
def legacy_fact_rows() -> list[tuple]:
    import openpyxl

    sql_rows = query_json(
        "SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id "
        "FROM dbo.transaction_db FOR JSON PATH, INCLUDE_NULL_VALUES", "sample")
    rows = [(r["transaction_id"], r["account_id"], datetime.fromisoformat(r["transaction_date"]),
             r["amount"], r["transaction_type"], r["branch_id"]) for r in sql_rows]
    ws = openpyxl.load_workbook(REPO / "data_sources" / "transaction_excel.xlsx").active
    for r in list(ws.iter_rows(values_only=True))[1:]:
        rows.append(tuple(r))
    with open(REPO / "data_sources" / "transaction_csv.csv", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            rows.append((int(r["transaction_id"]), int(r["account_id"]),
                         datetime.strptime(r["transaction_date"], "%d-%m-%Y %H:%M:%S"),
                         int(r["amount"]), r["transaction_type"], int(r["branch_id"])))
    seen, unique = set(), []
    for r in rows:  # tUniqRow: first occurrence of transaction_id wins (stream order SQL, Excel, CSV)
        if r[0] not in seen:
            seen.add(r[0])
            unique.append(r)
    return unique


def load_legacy() -> None:
    ddl = (REPO / "sql_scripts" / "01_create_tables.sql").read_text(encoding="utf-8")
    sqlcmd(ddl)
    sqlcmd("""
INSERT INTO DWH.dbo.DimBranch (BranchID, BranchName, BranchLocation)
  SELECT branch_id, branch_name, branch_location FROM sample.dbo.branch;
INSERT INTO DWH.dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
  SELECT account_id, customer_id, account_type, balance, date_opened, status FROM sample.dbo.account;
INSERT INTO DWH.dbo.DimCustomer (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email)
  SELECT c.customer_id, UPPER(c.customer_name), UPPER(c.address), ci.city_name, s.state_name,
         c.age, UPPER(c.gender), c.email
  FROM sample.dbo.customer c
  LEFT JOIN sample.dbo.city ci ON c.city_id = ci.city_id
  LEFT JOIN sample.dbo.state s ON ci.state_id = s.state_id;
""", "sample")
    # One statement per batch: a rejected row (FK violation) must not abort the others.
    stmts = "\nGO\n".join(
        "INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, "
        f"TransactionType, BranchID) VALUES ({', '.join(lit(v) for v in r)});"
        for r in legacy_fact_rows())
    sqlcmd("TRUNCATE TABLE dbo.FactTransaction;\nGO\n" + stmts + "\nGO\n", "DWH")


# ---------------------------------------------------------------------------------------------
# Edge dataset: semantics the procedures depend on
# ---------------------------------------------------------------------------------------------
EDGE_BRANCH = [(1, "KC EDGE", "JL. EDGE 1")]
EDGE_CUSTOMER = [  # CustomerID, CustomerName
    (1, "SHELLY JUWITA"),
    (2, "Ana Maria"),            # mixed case: CI collation must match 'ana', 'ANA', 'aNa'
    (3, "ANA_LIA"),              # literal underscore
    (4, "ANAXLIA"),              # matched by 'ana_lia' because '_' is a wildcard in T-SQL
    (5, "100% SAVER"),           # literal percent
    (6, "O'BRIEN"),              # quote
    (7, "MULLER\\TEST"),         # backslash: literal in T-SQL, escape char in Spark LIKE
    (8, "JOSÉ RIZAL"),           # accent-sensitive: 'jose' must not match
    (9, "BUDI "),                # trailing space in the stored name
    (10, None),                  # NULL name never matches
    (11, "NO ACCOUNTS"),         # inner join on DimAccount drops it
    (12, "[VIP] TONO"),          # T-SQL bracket class vs Spark literal (documented divergence)
]
EDGE_ACCOUNT = [  # AccountID, CustomerID, AccountType, Balance, DateOpened, Status
    (1, 1, "saving", Decimal("1500000.0000"), "2020-05-01", "active"),
    (2, 1, "checking", Decimal("100.5000"), "2020-05-02", "Active"),      # CI equality
    (3, 2, "saving", Decimal("2000.0000"), "2020-05-03", "ACTIVE  "),     # CI + trailing spaces
    (4, 2, "checking", Decimal("999.0000"), "2020-05-04", "terminated"),
    (5, 3, "saving", Decimal("10.0000"), "2020-05-05", "active"),
    (6, 4, "saving", Decimal("20.0000"), "2020-05-06", "active"),
    (7, 5, "saving", Decimal("30.0000"), "2020-05-07", "active"),
    (8, 6, "saving", Decimal("40.0000"), "2020-05-08", "active"),
    (9, 7, "saving", Decimal("50.0000"), "2020-05-09", "active"),
    (10, 8, "saving", Decimal("60.0000"), "2020-05-10", "active"),
    (11, 9, "saving", Decimal("70.0000"), "2020-05-11", "active"),
    (12, 10, "saving", Decimal("80.0000"), "2020-05-12", "active"),
    (13, 12, "saving", Decimal("90.0000"), "2020-05-13", "active"),
    (14, 1, "saving", None, "2020-05-14", "active"),                       # NULL balance
    (15, 2, "saving", Decimal("5.0000"), "2020-05-15", None),              # NULL status
    (16, 99, "saving", Decimal("7.0000"), "2020-05-16", "active"),         # orphan customer
    (17, 3, "checking", Decimal("0.0000"), "2020-05-17", " active"),       # leading space: no match
]
EDGE_FACT = [  # TransactionID, AccountID, TransactionDate, Amount, TransactionType
    (1, 1, "2024-01-01 00:00:00.000", Decimal("100.0000"), "Deposit"),
    (2, 1, "2024-01-01 23:59:59.997", Decimal("40.2500"), "Withdrawal"),
    (3, 2, "2024-01-02 12:00:00.000", Decimal("10.0000"), "deposit"),      # CI equality
    (4, 2, "2024-01-02 13:00:00.000", Decimal("1.1111"), "DEPOSIT   "),    # CI + trailing spaces
    (5, 3, "2024-01-03 08:00:00.000", Decimal("500.0000"), None),          # NULL type -> -Amount
    (6, 3, "2024-01-03 09:00:00.000", None, "Deposit"),                    # NULL amount ignored by SUM
    (7, 5, "2024-01-31 23:59:59.997", Decimal("3.0000"), "Transfer"),
    (8, 5, "2024-02-01 00:00:00.000", Decimal("4.0000"), "Payment"),
    (9, 14, "2024-02-01 10:00:00.000", Decimal("9.0000"), "Deposit"),      # NULL balance stays NULL
    (10, 16, "2023-12-31 23:59:59.997", Decimal("1.0000"), " Deposit"),    # leading space: not Deposit
    (11, 4, "2024-02-29 12:00:00.000", Decimal("12.3400"), "Deposit"),
    (12, 6, "2024-03-01 00:00:00.000", None, None),                        # day with only NULL amounts
]


def load_edge() -> None:
    ddl = (REPO / "sql_scripts" / "01_create_tables.sql").read_text(encoding="utf-8")
    sqlcmd(ddl.replace("'DWH'", "'DWH_EDGE'").replace("DATABASE DWH", "DATABASE DWH_EDGE")
           .replace("USE DWH", "USE DWH_EDGE"))
    rows = []
    rows += [f"INSERT INTO dbo.DimBranch VALUES ({lit(b)}, {lit(n)}, {lit(l)});" for b, n, l in EDGE_BRANCH]
    rows += [f"INSERT INTO dbo.DimCustomer (CustomerID, CustomerName) VALUES ({c}, {lit(n)});"
             for c, n in EDGE_CUSTOMER]
    rows += [f"INSERT INTO dbo.DimAccount VALUES ({a}, {c}, {lit(t)}, {lit(b)}, '{d}', {lit(s)});"
             for a, c, t, b, d, s in EDGE_ACCOUNT]
    rows += [f"INSERT INTO dbo.FactTransaction VALUES ({t}, {a}, '{d}', {lit(m)}, {lit(ty)}, 1);"
             for t, a, d, m, ty in EDGE_FACT]
    sqlcmd("SET NOCOUNT ON;\n" + "\n".join(rows) + "\nGO\n", "DWH_EDGE")


def create_procs(database: str) -> None:
    procs = (REPO / "sql_scripts" / "02_create_procedures.sql").read_text(encoding="utf-8")
    procs = procs.replace("USE DWH;", f"USE {database};")
    procs = re.sub(r"^CREATE PROCEDURE", "GO\nCREATE PROCEDURE", procs, flags=re.M)
    sqlcmd(procs, database)


# ---------------------------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------------------------
LONG_TRUNCATED = "%" * 100 + "ZZZ_NOT_A_NAME"  # VARCHAR(100) truncation leaves only '%' -> all rows
PATHOLOGICAL = "%A" * 40 + "%Q"               # many '%' segments: must not hang
CASES = [
    ("legacy", "sp_DailyTransaction", {"start_date": "2024-01-01", "end_date": "2024-01-31"}),
    ("legacy", "sp_DailyTransaction", {"start_date": "2022-01-01", "end_date": "2022-12-31"}),
    ("legacy", "sp_DailyTransaction", {"start_date": "1900-01-01", "end_date": "9999-12-31"}),
    ("legacy", "sp_DailyTransaction", {"start_date": "2024-01-22", "end_date": "2024-01-22"}),
    ("legacy", "sp_DailyTransaction", {"start_date": "2024-01-31", "end_date": "2024-01-01"}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": "shelly"}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": "SHELLY"}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": "Ma"}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": "an"}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": "cintya"}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": ""}),
    ("legacy", "sp_BalancePerCustomer", {"customer_name": "nobody"}),
    ("edge", "sp_DailyTransaction", {"start_date": "2024-01-01", "end_date": "2024-01-31"}),
    ("edge", "sp_DailyTransaction", {"start_date": "2023-01-01", "end_date": "2024-12-31"}),
    ("edge", "sp_DailyTransaction", {"start_date": "2024-02-29", "end_date": "2024-03-01"}),
    ("edge", "sp_DailyTransaction", {"start_date": None, "end_date": "2024-12-31"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "ana"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "aNa MaRiA"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "shelly"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "ana_lia"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "_"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "%"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "100%"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "0% s"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "o'brien"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "muller\\test"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "\\"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "jose"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "josé"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "budi "}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "budi  "}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": ""}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": None}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "no accounts"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "%%%%%%%%ana%%%%%%%%"}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": LONG_TRUNCATED}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": PATHOLOGICAL}),
    ("edge", "sp_BalancePerCustomer", {"customer_name": "VIP"}),
]
DATABASES = {"legacy": "DWH", "edge": "DWH_EDGE"}
RESULT_DDL = {
    "sp_DailyTransaction": "[Date] DATE, TotalTransactions INT, TotalAmount MONEY",
    "sp_BalancePerCustomer": "CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY",
}


def run_case(dataset: str, proc: str, params: dict) -> list[dict]:
    args = ", ".join(f"@{k} = {lit(v)}" for k, v in params.items())
    sql = (f"CREATE TABLE #r ({RESULT_DDL[proc]});\nINSERT INTO #r EXEC dbo.{proc} {args};\n"
           "SELECT * FROM #r FOR JSON PATH, INCLUDE_NULL_VALUES;")
    return query_json(sql, DATABASES[dataset])


def _jsonable(v):
    return str(v) if isinstance(v, Decimal) else v


def main() -> None:
    sqlcmd("IF DB_ID('sample') IS NULL RESTORE DATABASE sample FROM DISK='/tmp/sample.bak' WITH "
           "MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', "
           "MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf';\n"
           "IF DB_ID('DWH') IS NOT NULL DROP DATABASE DWH;\n"
           "IF DB_ID('DWH_EDGE') IS NOT NULL DROP DATABASE DWH_EDGE;\n")
    load_legacy()
    load_edge()
    for db in DATABASES.values():
        create_procs(db)
    for dataset, db in DATABASES.items():
        out = HERE / dataset
        out.mkdir(exist_ok=True)
        for t in TABLES:
            rows = query_json(f"SELECT * FROM dbo.{t} ORDER BY 1 FOR JSON PATH, INCLUDE_NULL_VALUES", db)
            (out / f"{t}.json").write_text(
                json.dumps([{k: _jsonable(v) for k, v in r.items()} for r in rows], indent=1,
                           ensure_ascii=False) + "\n", encoding="utf-8")
            print(f"{dataset}.{t}: {len(rows)} rows")
    version = sqlcmd("SET NOCOUNT ON; SELECT @@VERSION;").strip().splitlines()[0].strip()
    cases = []
    for dataset, proc, params in CASES:
        rows = run_case(dataset, proc, params)
        cases.append({"dataset": dataset, "proc": proc, "params": params,
                      "rows": [{k: _jsonable(v) for k, v in r.items()} for r in rows]})
        print(f"{dataset} {proc} {params}: {len(rows)} rows")
    (HERE / "expected.json").write_text(json.dumps(
        {"generated_by": "databricks/fixtures/t9_legacy_parity/build_expected.py",
         "sql_server": version, "collation": "SQL_Latin1_General_CP1_CI_AS", "cases": cases},
        indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
