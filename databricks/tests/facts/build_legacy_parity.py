"""Rebuild databricks/fixtures/parity/fact_transaction.csv from the legacy stack.

Replays Talend ``Load_FactTransaction`` against SQL Server 2022 running in Docker:

1. ``sample.bak`` restored as ``sample`` and ``sql_scripts/01_create_tables.sql`` applied (DWH).
2. ``DimBranch`` / ``DimAccount`` loaded with the Talend tDBInput queries (needed for the FKs).
3. tUnite: SQL Server ``transaction_db`` (mergeOrder 1), ``transaction_excel.xlsx`` (2),
   ``transaction_csv.csv`` (3, ``dd-MM-yyyy HH:mm:ss``).
4. tUniqRow on ``transaction_id``: the first row seen in merge order wins.
5. tDBOutput: TRUNCATE + row INSERT with DIE_ON_ERROR=false, so FK violations are skipped.

Usage (container must already exist with the DB restored and DWH created):
    python databricks/tests/facts/build_legacy_parity.py --container mssql7 --password '...'
"""

from __future__ import annotations

import argparse
import csv
import subprocess
from datetime import datetime
from pathlib import Path

import openpyxl

REPO = Path(__file__).resolve().parents[3]
DATA = REPO / "data_sources"
OUT = REPO / "databricks" / "fixtures" / "parity" / "fact_transaction.csv"


def _q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, datetime):
        return "'" + v.strftime("%Y-%m-%dT%H:%M:%S") + "'"
    if isinstance(v, (int, float)):
        return str(int(v))
    return "'" + str(v).replace("'", "''") + "'"


def file_rows() -> list[tuple]:
    rows: list[tuple] = []
    ws = openpyxl.load_workbook(DATA / "transaction_excel.xlsx", data_only=True)["Sheet1"]
    for i, r in enumerate(ws.iter_rows(min_row=2, values_only=True), start=1):
        if all(c is None for c in r):
            continue
        rows.append((2, i, *r[:6]))
    with open(DATA / "transaction_csv.csv", newline="", encoding="iso-8859-1") as fh:
        for i, r in enumerate(csv.DictReader(fh), start=1):
            ts = datetime.strptime(r["transaction_date"], "%d-%m-%Y %H:%M:%S")
            rows.append(
                (
                    3,
                    i,
                    int(r["transaction_id"]),
                    int(r["account_id"]),
                    ts,
                    int(r["amount"]),
                    r["transaction_type"],
                    int(r["branch_id"]),
                )
            )
    return rows


def build_sql() -> str:
    values = ",\n".join("(" + ", ".join(_q(v) for v in row) + ")" for row in file_rows())
    return f"""
SET NOCOUNT ON;
USE sample;
DELETE FROM DWH.dbo.FactTransaction; DELETE FROM DWH.dbo.DimAccount; DELETE FROM DWH.dbo.DimBranch;
INSERT INTO DWH.dbo.DimBranch (BranchID, BranchName, BranchLocation)
SELECT dbo.branch.branch_id, dbo.branch.branch_name, dbo.branch.branch_location FROM dbo.branch;
INSERT INTO DWH.dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
SELECT dbo.account.account_id, dbo.account.customer_id, dbo.account.account_type,
       dbo.account.balance, dbo.account.date_opened, dbo.account.status FROM dbo.account;

CREATE TABLE #src (merge_order INT, ordinal INT, transaction_id INT, account_id INT,
                   transaction_date DATETIME2(0), amount INT, transaction_type VARCHAR(50), branch_id INT);
INSERT INTO #src
SELECT 1, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), dbo.transaction_db.transaction_id,
       dbo.transaction_db.account_id, dbo.transaction_db.transaction_date, dbo.transaction_db.amount,
       dbo.transaction_db.transaction_type, dbo.transaction_db.branch_id
FROM dbo.transaction_db;
INSERT INTO #src VALUES
{values};

DECLARE @id INT, @acc INT, @dt DATETIME2(0), @amt INT, @typ VARCHAR(50), @br INT;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR
  SELECT transaction_id, account_id, transaction_date, amount, transaction_type, branch_id
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY merge_order, ordinal) rn FROM #src) s
  WHERE rn = 1 ORDER BY merge_order, ordinal;
OPEN c; FETCH NEXT FROM c INTO @id, @acc, @dt, @amt, @typ, @br;
WHILE @@FETCH_STATUS = 0
BEGIN
  BEGIN TRY
    INSERT INTO DWH.dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES (@id, @acc, @dt, @amt, @typ, @br);
  END TRY
  BEGIN CATCH
    PRINT CONCAT('REJECT ', @id, ': ', ERROR_MESSAGE());
  END CATCH
  FETCH NEXT FROM c INTO @id, @acc, @dt, @amt, @typ, @br;
END
CLOSE c; DEALLOCATE c;
SELECT CONCAT(TransactionID, ',', AccountID, ',', CONVERT(VARCHAR(23), TransactionDate, 121), ',',
              CAST(Amount AS DECIMAL(19,4)), ',', TransactionType, ',', BranchID)
FROM DWH.dbo.FactTransaction ORDER BY TransactionID;
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", default="mssql7")
    ap.add_argument("--password", required=True)
    args = ap.parse_args()
    out = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            args.container,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-C",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-P",
            args.password,
            "-h",
            "-1",
            "-W",
            "-b",
        ],
        input=build_sql(),
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    for ln in lines:
        if ln.startswith("REJECT"):
            print(ln)
    data = [ln for ln in lines if not ln.startswith("REJECT") and "," in ln]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        "transaction_id,account_id,transaction_date,amount,transaction_type,branch_id\n" + "\n".join(data) + "\n"
    )
    print(f"wrote {len(data)} rows to {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
