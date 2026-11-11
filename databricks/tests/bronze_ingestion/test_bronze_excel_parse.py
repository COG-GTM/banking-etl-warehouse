"""Pure-Python tests of the openpyxl-based workbook parser (no Spark)."""

import datetime as dt
import io
import json

import pytest

openpyxl = pytest.importorskip("openpyxl")

from banking_etl.ingestion.excel import cell_to_string, parse_xlsx
from banking_etl.ingestion.metadata import CORRUPT_RECORD_COL, UNEXPECTED_COLUMNS_COL
from banking_etl.ingestion.sources import TRANSACTION_EXCEL

COLS = TRANSACTION_EXCEL.column_names


def _xlsx(rows, sheet="Sheet1"):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet
    for r in rows:
        ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_real_workbook_parses_all_rows(data_sources):
    rows = parse_xlsx((data_sources / "transaction_excel.xlsx").read_bytes(), COLS)
    assert [r["transaction_id"] for r in rows] == ["6", "7", "11", "12", "13", "14", "15"]
    first = rows[0]
    assert first["account_id"] == "6"
    assert first["transaction_date"] == "2024-01-18 13:10:00"
    assert first["amount"] == "50000"
    assert first["transaction_type"] == "Withdrawal"
    assert first["branch_id"] == "1"
    assert first["_excel_sheet"] == "Sheet1"
    assert first["_excel_row"] == 2
    assert all(r[CORRUPT_RECORD_COL] is None and r[UNEXPECTED_COLUMNS_COL] is None for r in rows)


def test_bad_cells_are_kept_as_text_not_fatal():
    content = _xlsx(
        [
            COLS,
            [1, 1, dt.datetime(2024, 1, 1, 9, 0), 100, "Deposit", 1],
            [2, "abc", "not-a-date", "1,5", "Deposit", 1],
            [3, 2, "31-02-2024 10:00:00", 12.5, "Payment", "#N/A"],
        ]
    )
    rows = parse_xlsx(content, COLS)
    assert len(rows) == 3
    assert rows[1]["account_id"] == "abc"
    assert rows[1]["transaction_date"] == "not-a-date"
    assert rows[2]["transaction_date"] == "31-02-2024 10:00:00"
    assert rows[2]["amount"] == "12.5"
    assert rows[2]["branch_id"] == "#N/A"


def test_header_matching_blank_rows_and_unexpected_columns():
    content = _xlsx(
        [
            [None] * 3,
            [
                "Transaction_ID",
                "account_id",
                "TRANSACTION_DATE",
                "amount",
                "transaction_type",
                "branch_id",
                "note",
            ],
            [None, None, None],
            [9, 9, "21-01-2024 14:00:00", 5, "Deposit", 4, "manual"],
        ]
    )
    rows = parse_xlsx(content, COLS)
    assert len(rows) == 1
    assert rows[0]["transaction_id"] == "9"
    assert rows[0]["transaction_date"] == "21-01-2024 14:00:00"
    assert rows[0]["_excel_row"] == 4
    assert json.loads(rows[0][UNEXPECTED_COLUMNS_COL]) == {"note": "manual"}


def test_missing_columns_flagged_on_each_row():
    rows = parse_xlsx(_xlsx([["transaction_id", "amount"], [1, 10]]), COLS)
    assert rows[0]["transaction_id"] == "1"
    assert rows[0]["branch_id"] is None
    assert rows[0][CORRUPT_RECORD_COL].startswith("missing columns: account_id")


def test_unreadable_workbook_becomes_one_rescued_row():
    rows = parse_xlsx(b"this is not a zip", COLS)
    assert len(rows) == 1
    assert rows[0]["transaction_id"] is None
    assert rows[0][CORRUPT_RECORD_COL].startswith("unreadable workbook")


def test_unknown_sheet_is_rescued():
    rows = parse_xlsx(_xlsx([COLS, [1, 1, None, 1, "x", 1]]), COLS, sheet_name="Nope")
    assert rows[0][CORRUPT_RECORD_COL].startswith("sheet not found")


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        (5, "5"),
        (5.0, "5"),
        (5.25, "5.25"),
        (True, "true"),
        (dt.datetime(2024, 1, 2, 3, 4, 5), "2024-01-02 03:04:05"),
        (dt.datetime(2024, 1, 2, 3, 4, 5, 500000), "2024-01-02 03:04:05.500000"),
        (dt.date(2024, 1, 2), "2024-01-02"),
        (" x ", " x "),
    ],
)
def test_cell_to_string(value, expected):
    assert cell_to_string(value) == expected
