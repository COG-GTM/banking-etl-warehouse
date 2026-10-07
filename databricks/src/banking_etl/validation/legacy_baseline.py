"""Build the legacy SQL Server DWH and export parity snapshots to ``databricks/fixtures/legacy_dwh``.

Replays the four Talend jobs (``talend_jobs/*.zip``) as Talend-equivalent row-by-row INSERTs
against a SQL Server 2022 instance that has ``data_sources/sample.bak`` restored:

* Load_DimBranch / Load_DimAccount: ``SELECT dbo.<t>.*`` -> tMap pass-through -> INSERT.
* Load_DimCustomer: customer LEFT JOIN city LEFT JOIN state (tMap UNIQUE_MATCH lookups),
  ``StringHandling.UPCASE`` on customer_name/address/gender.
* Load_FactTransaction: TRUNCATE, tUnite(transaction_db, Excel Sheet1, CSV) in merge order,
  tUniqRow on transaction_id (first wins), INSERT with DIE_ON_ERROR=false (FK/PK violations
  are rejected per row and logged to ``talend_rejects.csv``).

Requires ``pymssql`` and ``openpyxl`` (dev-only; not needed on Databricks)::

    docker run -d --name mssql2022 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=... -p 1433:1433 \
        -v $PWD/data_sources:/data_sources:ro mcr.microsoft.com/mssql/server:2022-latest
    python -m banking_etl.validation.legacy_baseline --password ... --restore
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import decimal
import hashlib
import json
import os
import re
from pathlib import Path

from banking_etl.validation import specs

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_OUT = REPO_ROOT / "databricks" / "fixtures" / "legacy_dwh"

SOURCE_TABLES = ("branch", "account", "customer", "city", "state", "transaction_db")


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="milliseconds" if value.microsecond else "seconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return format(value.quantize(decimal.Decimal("0.0001")), "f")
    return str(value)


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _write_csv(path: Path, header: list[str], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        for row in rows:
            w.writerow([_fmt(v) for v in row])
            n += 1
    return n


def split_batches(script: str) -> list[str]:
    """Split a T-SQL script on ``GO`` separators.

    ``02_create_procedures.sql`` puts a PRINT before each CREATE PROCEDURE in the same batch,
    which SQL Server rejects (Msg 111), so a batch break is inserted before CREATE PROCEDURE.
    """
    script = re.sub(r"(?im)^(\s*CREATE\s+PROCEDURE\b)", r"GO\n\1", script)
    return [b.strip() for b in re.split(r"(?im)^\s*GO\s*$", script) if b.strip()]


def _exec_script(conn, path: Path) -> None:
    cur = conn.cursor()
    for batch in split_batches(path.read_text(encoding="utf-8")):
        cur.execute(batch)
    conn.commit()


def read_excel_rows(path: Path) -> list[tuple]:
    import openpyxl

    ws = openpyxl.load_workbook(path, data_only=True)["Sheet1"]
    rows = list(ws.iter_rows(values_only=True))[1:]  # HEADER=1
    return [tuple(r[:6]) for r in rows if any(v is not None for v in r)]


def read_csv_rows(path: Path) -> list[tuple]:
    with path.open(encoding="utf-8") as fh:
        rows = list(csv.reader(fh))[1:]  # HEADER=1
    out = []
    for tid, acc, tdate, amount, ttype, branch in rows:
        out.append((int(tid), int(acc), dt.datetime.strptime(tdate, "%d-%m-%Y %H:%M:%S"),
                    int(amount), ttype, int(branch), tdate))
    return out


def talend_fact_stream(sql_rows, excel_rows, csv_rows) -> list[tuple[str, tuple]]:
    """tUnite (merge order sql, excel, csv) + tUniqRow(transaction_id) keeping first occurrence."""
    seen: set[int] = set()
    out = []
    for source, rows in (("sqlserver", sql_rows), ("excel", excel_rows), ("csv", csv_rows)):
        for r in rows:
            if r[0] in seen:
                continue
            seen.add(r[0])
            out.append((source, tuple(r[:6])))
    return out


def run(args) -> dict:
    import pymssql

    out = Path(args.out)
    conn = pymssql.connect(server=args.server, port=args.port, user=args.user,
                           password=args.password, autocommit=True)
    cur = conn.cursor()
    if args.restore:
        cur.execute(
            f"RESTORE DATABASE sample FROM DISK='{args.bak}' WITH "
            "MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', "
            "MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf', REPLACE"
        )
        while cur.nextset():
            pass
    cur.execute("IF DB_ID('DWH') IS NOT NULL BEGIN ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE DWH; END")
    _exec_script(conn, REPO_ROOT / "sql_scripts" / "01_create_tables.sql")
    _exec_script(conn, REPO_ROOT / "sql_scripts" / "02_create_procedures.sql")

    # ---- extract sources (queries copied from the Talend tMSSqlInput components) ----
    cur.execute("USE sample")
    src: dict[str, tuple[list[str], list[tuple]]] = {}
    for t in SOURCE_TABLES:
        cols = [f.name for f in specs.SOURCE_SCHEMAS[f"sqlserver_{t}"].fields]
        cur.execute(f"SELECT {', '.join(f'dbo.{t}.{c}' for c in cols)} FROM dbo.{t} ORDER BY 1")
        src[t] = (cols, list(cur.fetchall()))
    excel_rows = read_excel_rows(REPO_ROOT / "data_sources" / "transaction_excel.xlsx")
    csv_rows = read_csv_rows(REPO_ROOT / "data_sources" / "transaction_csv.csv")

    counts: dict[str, int] = {}
    for t, (cols, rows) in src.items():
        counts[f"source/sqlserver_{t}"] = _write_csv(out / "source" / f"sqlserver_{t}.csv", cols, rows)
    txn_cols = [f.name for f in specs.SOURCE_SCHEMAS["file_transaction_excel"].fields]
    counts["source/file_transaction_excel"] = _write_csv(
        out / "source" / "file_transaction_excel.csv", txn_cols, excel_rows)
    counts["source/file_transaction_csv"] = _write_csv(
        out / "source" / "file_transaction_csv.csv", txn_cols,
        [(r[0], r[1], r[6], r[3], r[4], r[5]) for r in csv_rows])

    # ---- Talend-equivalent loads into DWH ----
    cur.execute("USE DWH")
    rejects: list[tuple] = []

    def insert(job: str, table: str, cols: list[str], row: tuple, source: str = "sqlserver"):
        try:
            cur.execute(f"INSERT INTO dbo.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})", row)
        except pymssql.Error as exc:  # DIE_ON_ERROR=false -> reject and continue
            msg = str(exc.args[1] if len(exc.args) > 1 else exc)
            msg = msg.decode() if isinstance(msg, bytes) else msg
            reason = "FK_VIOLATION" if "FOREIGN KEY" in msg else "PK_VIOLATION" if "PRIMARY KEY" in msg else "ERROR"
            rejects.append((job, row[0], row[1] if table == "FactTransaction" else None,
                            row[5] if table == "FactTransaction" else None, source, reason))

    for r in src["branch"][1]:
        insert("Load_DimBranch", "DimBranch", ["BranchID", "BranchName", "BranchLocation"], tuple(r))
    for r in src["account"][1]:
        insert("Load_DimAccount", "DimAccount",
               ["AccountID", "CustomerID", "AccountType", "Balance", "DateOpened", "Status"], tuple(r))
    city = {c[0]: c for c in src["city"][1]}
    state = {s[0]: s for s in src["state"][1]}
    for cid, name, addr, city_id, age, gender, email in src["customer"][1]:
        c = city.get(city_id)
        s = state.get(c[2]) if c else None
        up = lambda v: v.upper() if v is not None else None  # noqa: E731
        insert("Load_DimCustomer", "DimCustomer",
               ["CustomerID", "CustomerName", "Address", "CityName", "StateName", "Age", "Gender", "Email"],
               (cid, up(name), up(addr), c[1] if c else None, s[1] if s else None, age, up(gender), email))
    cur.execute("TRUNCATE TABLE dbo.FactTransaction")
    stream = talend_fact_stream(src["transaction_db"][1], excel_rows, csv_rows)
    for source, r in stream:
        insert("Load_FactTransaction", "FactTransaction",
               ["TransactionID", "AccountID", "TransactionDate", "Amount", "TransactionType", "BranchID"],
               r, source)

    # ---- export DWH snapshots ----
    for spec in specs.GOLD_TABLES:
        cols = [c.legacy for c in spec.columns]
        cur.execute(f"SELECT {', '.join(cols)} FROM dbo.{spec.legacy} ORDER BY 1")
        counts[f"dwh/{spec.legacy}"] = _write_csv(out / "dwh" / f"{spec.legacy}.csv", cols, cur.fetchall())
    counts["talend_rejects"] = _write_csv(
        out / "talend_rejects.csv", [f.name for f in specs.TALEND_REJECTS_SCHEMA.fields], rejects)

    # ---- stored procedure outputs ----
    daily = []
    for start, end in specs.DAILY_TRANSACTION_PARAMS:
        cur.execute("EXEC dbo.sp_DailyTransaction @start_date=%s, @end_date=%s", (start, end))
        daily += [(start, end, *r) for r in cur.fetchall()]
    counts["procs/sp_DailyTransaction"] = _write_csv(
        out / "procs" / "sp_DailyTransaction.csv",
        [f.name for f in specs.DAILY_TRANSACTION_SCHEMA.fields], daily)
    bal = []
    for name in specs.BALANCE_PER_CUSTOMER_PARAMS:
        cur.execute("EXEC dbo.sp_BalancePerCustomer @customer_name=%s", (name,))
        bal += [(name, *r) for r in cur.fetchall()]
    counts["procs/sp_BalancePerCustomer"] = _write_csv(
        out / "procs" / "sp_BalancePerCustomer.csv",
        [f.name for f in specs.BALANCE_PER_CUSTOMER_SCHEMA.fields], bal)

    cur.execute("SELECT @@VERSION")
    version = _text(cur.fetchone()[0]).splitlines()[0]
    cur.execute("SELECT CAST(DATABASEPROPERTYEX('DWH', 'Collation') AS NVARCHAR(128))")
    collation = _text(cur.fetchone()[0])
    files = sorted(p for p in out.rglob("*.csv"))
    manifest = {
        "generated_by": "banking_etl.validation.legacy_baseline",
        "sql_server_version": version,
        "dwh_collation": collation,
        "row_counts": counts,
        "sha256": {str(p.relative_to(out)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    conn.close()
    return manifest


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--server", default=os.environ.get("MSSQL_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("MSSQL_PORT", "1433")))
    p.add_argument("--user", default=os.environ.get("MSSQL_USER", "sa"))
    p.add_argument("--password", default=os.environ.get("MSSQL_SA_PASSWORD"))
    p.add_argument("--restore", action="store_true", help="restore sample.bak first")
    p.add_argument("--bak", default="/data_sources/sample.bak", help="path to sample.bak inside the container")
    p.add_argument("--out", default=str(DEFAULT_OUT))
    manifest = run(p.parse_args(argv))
    print(json.dumps(manifest["row_counts"], indent=2))


if __name__ == "__main__":
    main()
