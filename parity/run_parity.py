#!/usr/bin/env python3
"""Legacy (T-SQL + Talend) vs dbt parity harness, executed on DuckDB.

Runs entirely locally: no SQL Server, no Talend and no Databricks workspace.

    python3 parity/run_parity.py              # run the checks
    python3 parity/run_parity.py --update     # rewrite parity/expected/*.csv

What it does, per scenario:
  1. builds the legacy DWH in DuckDB by replaying the four Talend jobs
     (parity/sql/01_legacy_dwh.sql),
  2. builds the dbt DAG (staging -> intermediate -> marts) over the same inputs
     (parity/sql/04_dbt_models.sql),
  3. runs the DuckDB translation of the legacy stored procedure and the dbt
     reporting model with the same parameters and asserts the result sets are
     identical (values compared positionally: the columns are deliberately
     renamed to snake_case in the dbt version),
  4. asserts both match the committed expected output in parity/expected/,
  5. runs the DuckDB equivalents of the dbt schema tests that replace the
     legacy PK/FK constraints and reports the failures.

Inputs are the real data in the repository:
  * data_sources/transaction_csv.csv and data_sources/transaction_excel.xlsx
    are read directly (the .xlsx with the standard library only),
  * parity/fixtures/source/*.csv are the six `sample` OLTP tables, extracted
    from data_sources/sample.bak with SQL Server (see parity/README.md).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import xml.etree.ElementTree as ET
import zipfile
from decimal import Decimal
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
PARITY_DIR = REPO_ROOT / "parity"
SQL_DIR = PARITY_DIR / "sql"
FIXTURE_DIR = PARITY_DIR / "fixtures" / "source"
EXPECTED_DIR = PARITY_DIR / "expected"

TRANSACTION_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
]

XLSX_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
EXCEL_EPOCH = dt.datetime(1899, 12, 30)

# scenario -> (legacy sql file, dbt sql file, parameters, expected csv)
SCENARIOS = [
    (
        "daily_transaction__all_time",
        "02_legacy_sp_daily_transaction.sql",
        "05_dbt_rpt_daily_transaction.sql",
        {"start_date": "2000-01-01", "end_date": "2099-12-31"},
    ),
    (
        "daily_transaction__default_vars_2024",
        "02_legacy_sp_daily_transaction.sql",
        "05_dbt_rpt_daily_transaction.sql",
        {"start_date": "2024-01-01", "end_date": "2024-12-31"},
    ),
    (
        "daily_transaction__readme_example",
        "02_legacy_sp_daily_transaction.sql",
        "05_dbt_rpt_daily_transaction.sql",
        {"start_date": "2024-01-18", "end_date": "2024-01-20"},
    ),
    (
        "balance_per_customer__unfiltered",
        "03_legacy_sp_balance_per_customer.sql",
        "06_dbt_rpt_balance_per_customer.sql",
        {"customer_name": ""},
    ),
    (
        "balance_per_customer__shelly",
        "03_legacy_sp_balance_per_customer.sql",
        "06_dbt_rpt_balance_per_customer.sql",
        {"customer_name": "Shelly"},
    ),
    (
        "balance_per_customer__lowercase_input",
        "03_legacy_sp_balance_per_customer.sql",
        "06_dbt_rpt_balance_per_customer.sql",
        {"customer_name": "shelly juwita"},
    ),
]


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


def read_xlsx(path: Path) -> list[list]:
    """Minimal .xlsx reader (stdlib only) for the single-sheet transaction file."""
    with zipfile.ZipFile(path) as archive:
        shared = [
            "".join(node.itertext())
            for node in ET.fromstring(archive.read("xl/sharedStrings.xml"))
        ]
        styles = ET.fromstring(archive.read("xl/styles.xml"))
        cell_xfs = styles.find(f"{XLSX_NS}cellXfs")
        date_styles = {
            index
            for index, xf in enumerate(cell_xfs)
            # 14-22 and 45-47 are the built-in date/time number formats
            if int(xf.get("numFmtId", "0")) in set(range(14, 23)) | {45, 46, 47}
        }
        sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))

    rows = []
    for row in sheet.iter(f"{XLSX_NS}row"):
        values = []
        for cell in row.iter(f"{XLSX_NS}c"):
            node = cell.find(f"{XLSX_NS}v")
            raw = "" if node is None else (node.text or "")
            if cell.get("t") == "s":
                values.append(shared[int(raw)])
            elif raw == "":
                values.append(None)
            elif int(cell.get("s", "0")) in date_styles:
                values.append(EXCEL_EPOCH + dt.timedelta(days=float(raw)))
            elif "." in raw:
                values.append(float(raw))
            else:
                values.append(int(raw))
        rows.append(values)
    return rows


def load_sources(con: duckdb.DuckDBPyConnection) -> None:
    """Register the raw inputs as src_* relations."""
    for fixture in sorted(FIXTURE_DIR.glob("*.csv")):
        con.execute(
            f"CREATE OR REPLACE TABLE src_{fixture.stem} AS "
            "SELECT * FROM read_csv(?, header = true, all_varchar = false)",
            [str(fixture)],
        )

    con.execute(
        """
        CREATE OR REPLACE TABLE src_transaction_csv AS
        SELECT
            CAST(transaction_id AS INT)   AS transaction_id,
            CAST(account_id AS INT)       AS account_id,
            strptime(transaction_date, '%d-%m-%Y %H:%M:%S') AS transaction_date,
            CAST(amount AS DECIMAL(18,2)) AS amount,
            transaction_type,
            CAST(branch_id AS INT)        AS branch_id
        FROM read_csv(?, header = true, all_varchar = true)
        """,
        [str(REPO_ROOT / "data_sources" / "transaction_csv.csv")],
    )

    header, *body = read_xlsx(REPO_ROOT / "data_sources" / "transaction_excel.xlsx")
    if [str(column) for column in header] != TRANSACTION_COLUMNS:
        raise SystemExit(f"unexpected transaction_excel.xlsx header: {header}")
    con.execute(
        """
        CREATE OR REPLACE TABLE src_transaction_excel (
            transaction_id INT,
            account_id INT,
            transaction_date TIMESTAMP,
            amount DECIMAL(18,2),
            transaction_type VARCHAR,
            branch_id INT
        )
        """
    )
    con.executemany(
        "INSERT INTO src_transaction_excel VALUES (?, ?, ?, ?, ?, ?)",
        [
            [row[0], row[1], row[2], Decimal(str(row[3])), row[4], row[5]]
            for row in body
        ],
    )


def normalise(rows: list[tuple]) -> list[tuple]:
    """Make values comparable across the two SQL dialects/renamings."""
    out = []
    for row in rows:
        out.append(
            tuple(
                f"{value:.2f}" if isinstance(value, Decimal) else str(value)
                for value in row
            )
        )
    return out


def write_rows(path: Path, columns: list[str], rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def read_expected(path: Path) -> tuple[list[str], list[tuple]]:
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        columns = next(reader)
        return columns, [tuple(row) for row in reader]


def diff(left: list[tuple], right: list[tuple]) -> list[str]:
    problems = []
    if len(left) != len(right):
        problems.append(f"row count {len(left)} != {len(right)}")
    for index, (a, b) in enumerate(zip(left, right)):
        if a != b:
            problems.append(f"row {index}: {a} != {b}")
    return problems[:10]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update",
        action="store_true",
        help="rewrite parity/expected/*.csv from the current run",
    )
    args = parser.parse_args()

    con = duckdb.connect()
    load_sources(con)
    con.execute(read_sql("01_legacy_dwh.sql"))
    con.execute(read_sql("04_dbt_models.sql"))

    failures = 0
    for name, legacy_file, dbt_file, params in SCENARIOS:
        legacy = con.execute(read_sql(legacy_file), params)
        legacy_columns = [d[0] for d in legacy.description]
        legacy_rows = normalise(legacy.fetchall())

        target = con.execute(read_sql(dbt_file), params)
        target_columns = [d[0] for d in target.description]
        target_rows = normalise(target.fetchall())

        problems = diff(legacy_rows, target_rows)
        expected_path = EXPECTED_DIR / f"{name}.csv"
        if args.update:
            write_rows(expected_path, target_columns, target_rows)
        elif not expected_path.exists():
            problems.append(f"missing expected output {expected_path}")
        else:
            expected_columns, expected_rows = read_expected(expected_path)
            if expected_columns != target_columns:
                problems.append(
                    f"columns {target_columns} != expected {expected_columns}"
                )
            problems.extend(diff(target_rows, expected_rows))

        status = "PASS" if not problems else "FAIL"
        failures += bool(problems)
        param_text = ", ".join(f"{k}={v!r}" for k, v in params.items())
        print(f"[{status}] {name} ({param_text})")
        print(f"         legacy {legacy_columns} -> dbt {target_columns}")
        print(f"         {len(legacy_rows)} rows")
        for problem in problems:
            print(f"         ! {problem}")

    print("\ndbt schema tests (PK/FK replacements):")
    test_rows = normalise(con.execute(read_sql("07_dbt_tests.sql")).fetchall())
    for test_name, count in test_rows:
        print(f"  [{'PASS' if count == '0' else 'FAIL'}] {test_name}: {count} failing rows")
    tests_path = EXPECTED_DIR / "dbt_tests.csv"
    if args.update:
        write_rows(tests_path, ["test_name", "failures"], test_rows)
    else:
        _, expected_tests = read_expected(tests_path)
        drift = diff(test_rows, expected_tests)
        failures += bool(drift)
        for problem in drift:
            print(f"  ! drift from expected: {problem}")

    if args.update:
        print(f"\nexpected outputs rewritten in {EXPECTED_DIR}")
        return 0

    print(f"\n{'PARITY OK' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
