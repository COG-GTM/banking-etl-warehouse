"""Excel (.xlsx) parsing that never fails a whole file.

Auto Loader's native Excel reader rejects an entire file when one typed cell
is bad, so workbooks are ingested as ``binaryFile`` and parsed here with
openpyxl into STRING values; typing and rescue happen afterwards in
``metadata.apply_schema_with_rescue``.
"""

from __future__ import annotations

import datetime as _dt
import io
import json
from collections.abc import Iterator, Sequence
from typing import Any

from .metadata import CORRUPT_RECORD_COL, UNEXPECTED_COLUMNS_COL

EXCEL_SHEET_COL = "_excel_sheet"
EXCEL_ROW_COL = "_excel_row"


def cell_to_string(value: Any) -> str | None:
    """Render an openpyxl cell value as text without losing information."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (_dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, _dt.timedelta):
        return str(value)
    return str(value)


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def parse_xlsx(
    content: bytes,
    expected_columns: Sequence[str],
    sheet_name: str | None = None,
) -> list[dict[str, Any]]:
    """Parse one workbook into rows of ``{column: str | None}``.

    * The first non-blank row of the sheet is the header; headers are matched
      to ``expected_columns`` case-insensitively.
    * Blank rows are skipped.
    * Values under unexpected headers go to ``_unexpected_columns`` (JSON).
    * Missing expected headers, or a workbook/sheet that cannot be read, are
      reported in ``_corrupt_record`` instead of raising.
    """
    expected = list(expected_columns)

    def empty_row(**extra: Any) -> dict[str, Any]:
        row: dict[str, Any] = {c: None for c in expected}
        row.update(
            {
                CORRUPT_RECORD_COL: None,
                UNEXPECTED_COLUMNS_COL: None,
                EXCEL_SHEET_COL: sheet_name,
                EXCEL_ROW_COL: None,
            }
        )
        row.update(extra)
        return row

    try:
        import openpyxl

        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:  # noqa: BLE001 - any unreadable file is rescued, not raised
        return [empty_row(**{CORRUPT_RECORD_COL: f"unreadable workbook: {type(exc).__name__}: {exc}"})]

    try:
        if sheet_name is None:
            ws = wb.worksheets[0]
        elif sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
        else:
            return [
                empty_row(**{CORRUPT_RECORD_COL: f"sheet not found: {sheet_name!r}; sheets={wb.sheetnames}"})
            ]
        title = ws.title

        rows: list[dict[str, Any]] = []
        header: list[str | None] | None = None
        missing: list[str] = []
        for row_number, values in enumerate(ws.iter_rows(values_only=True), start=1):
            if all(_blank(v) for v in values):
                continue
            if header is None:
                header = [None if _blank(v) else str(v).strip() for v in values]
                present = {h.lower() for h in header if h}
                missing = [c for c in expected if c.lower() not in present]
                continue
            out = empty_row(**{EXCEL_SHEET_COL: title, EXCEL_ROW_COL: row_number})
            unexpected: dict[str, str | None] = {}
            for idx, value in enumerate(values):
                text = cell_to_string(value)
                name = header[idx] if idx < len(header) else None
                match = next((c for c in expected if name and c.lower() == name.lower()), None)
                if match is not None:
                    out[match] = text
                elif text is not None:
                    unexpected[name or f"_col{idx + 1}"] = text
            if unexpected:
                out[UNEXPECTED_COLUMNS_COL] = json.dumps(unexpected, sort_keys=True)
            if missing:
                out[CORRUPT_RECORD_COL] = f"missing columns: {', '.join(missing)}"
            rows.append(out)
        return rows
    except Exception as exc:  # noqa: BLE001
        return [empty_row(**{CORRUPT_RECORD_COL: f"unreadable sheet: {type(exc).__name__}: {exc}"})]
    finally:
        wb.close()


def xlsx_rows_from_files(
    files: Iterator[tuple[str, Any, bytes]],
    expected_columns: Sequence[str],
    sheet_name: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Parse ``(path, modification_time, content)`` tuples, tagging each row with its file."""
    for path, mtime, content in files:
        for row in parse_xlsx(content, expected_columns, sheet_name):
            row["path"] = path
            row["modificationTime"] = mtime
            yield row
