"""Regenerate the Ticket 8 fixtures from the committed sources.

File streams (run from ``databricks/``): ``python fixtures/t8_build_fixtures.py`` rewrites
``t8_bronze_file_transaction_{csv,excel}.csv`` from ``data_sources/transaction_csv.csv`` (12 rows,
``dd-MM-yyyy HH:mm:ss``) and ``data_sources/transaction_excel.xlsx`` (Sheet1, 7 rows), typed the way the
Bronze file contracts store them (ISO timestamps, amount as DECIMAL).

SQL Server rows (``t8_bronze_sqlserver_transaction_db.csv``, ``t8_silver_dim_account.csv``,
``t8_silver_dim_branch.csv``) were exported from a SQL Server 2022 restore of ``data_sources/sample.bak``::

    RESTORE DATABASE sample FROM DISK='/backup/sample.bak'
      WITH MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf'
    sqlcmd -C -d sample -W -s, -h-1 -Q "SET NOCOUNT ON; SELECT transaction_id, account_id,
      CONVERT(varchar(19), transaction_date, 120), amount, transaction_type, branch_id
      FROM dbo.transaction_db ORDER BY transaction_id"
    (likewise dbo.account with CONVERT(varchar(10), date_opened, 23), and dbo.branch)
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
HEADER = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id"]


def _write(path: Path, rows: list[list]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(HEADER)
        w.writerows(rows)


def build_csv() -> list[list]:
    with open(REPO / "data_sources" / "transaction_csv.csv", encoding="latin-1", newline="") as fh:
        rows = list(csv.DictReader(fh))
    return [[r["transaction_id"], r["account_id"],
             datetime.strptime(r["transaction_date"], "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
             r["amount"], r["transaction_type"], r["branch_id"]] for r in rows]


def build_excel() -> list[list]:
    import openpyxl

    ws = openpyxl.load_workbook(REPO / "data_sources" / "transaction_excel.xlsx", read_only=True)["Sheet1"]
    it = ws.iter_rows(values_only=True)
    assert [str(h) for h in next(it)] == HEADER
    return [[r[0], r[1], r[2].strftime("%Y-%m-%d %H:%M:%S"), r[3], r[4], r[5]] for r in it if r[0] is not None]


if __name__ == "__main__":
    _write(HERE / "t8_bronze_file_transaction_csv.csv", build_csv())
    _write(HERE / "t8_bronze_file_transaction_excel.csv", build_excel())
    print("wrote t8_bronze_file_transaction_{csv,excel}.csv")
