import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from banking_dwh.bronze import excel
from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.tables import BRONZE_FILE_TRANSACTION_EXCEL as CONTRACT

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT.parent / "data_sources" / "transaction_excel.xlsx"
NOTEBOOK = ROOT / "notebooks" / "bronze" / "03_ingest_excel.py"
HEADER = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id"]
EXPECTED = [
    (6, 6, datetime(2024, 1, 18, 13, 10), Decimal("50000.0000"), "Withdrawal", 1),
    (7, 6, datetime(2024, 1, 19, 14, 0), Decimal("100000.0000"), "Payment", 1),
    (11, 10, datetime(2024, 1, 20, 15, 0), Decimal("1000000.0000"), "Transfer", 1),
    (12, 11, datetime(2024, 1, 20, 10, 0), Decimal("500000.0000"), "Deposit", 1),
    (13, 12, datetime(2024, 1, 20, 12, 10), Decimal("500000.0000"), "Withdrawal", 5),
    (14, 13, datetime(2024, 1, 21, 14, 0), Decimal("1500000.0000"), "Deposit", 4),
    (15, 14, datetime(2024, 1, 21, 8, 0), Decimal("500000.0000"), "Transfer", 3),
]


def _write_xlsx(path: Path, rows, header=HEADER, sheet="Sheet1"):
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet
    ws.append(header)
    for r in rows:
        ws.append(list(r))
    wb.save(path)
    return path


@pytest.fixture()
def t5_cfg(spark, tmp_path):
    """Fresh bronze schema per test so idempotency counts are isolated."""
    prefix = f"t5_{abs(hash(tmp_path)) % 10**8}_"
    cfg = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix=prefix)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('bronze')}")
    yield cfg
    spark.sql(f"DROP SCHEMA IF EXISTS {cfg.schema('bronze')} CASCADE")


@pytest.fixture()
def landing(tmp_path):
    d = tmp_path / "landing" / "excel" / "transaction"
    d.mkdir(parents=True)
    return d


def _copy_committed(landing: Path, name="transaction_excel.xlsx") -> Path:
    dst = landing / name
    dst.write_bytes(XLSX.read_bytes())
    return dst


def _business(rows):
    return [tuple(r[c] for c in HEADER) for r in rows]


# ---------------------------------------------------------------- openpyxl reader
def test_committed_workbook_shape():
    rows = excel.read_workbook_rows(str(XLSX))
    assert len(rows) == 7
    assert list(rows[0]) == HEADER
    assert rows[0]["transaction_date"] == "2024-01-18 13:10:00"
    assert rows[0]["amount"] == "50000"


@pytest.mark.parametrize("value,expected", [
    (None, None), (6, "6"), (6.0, "6"), (6.5, "6.5"), (0.1, "0.1"), ("  ", None), ("abc", "abc"),
    (datetime(2024, 1, 2, 3, 4, 5), "2024-01-02 03:04:05"), (True, "true"), (Decimal("1.25"), "1.25"),
])
def test_cell_to_text(value, expected):
    assert excel.cell_to_text(value) == expected


def test_missing_sheet_raises(tmp_path):
    p = _write_xlsx(tmp_path / "x.xlsx", [], sheet="Other")
    with pytest.raises(ValueError, match="Sheet1"):
        excel.read_workbook_rows(str(p))


def test_list_excel_files_filters_and_sorts(landing):
    for name in ("b.xlsx", "a.xlsx", "~$a.xlsx", "notes.csv", ".hidden.xlsx"):
        (landing / name).write_bytes(b"")
    assert [Path(p).name for p in excel.list_excel_files(str(landing))] == ["a.xlsx", "b.xlsx"]
    assert excel.list_excel_files("dbfs:" + str(landing / "a.xlsx")) == [str(landing / "a.xlsx")]
    with pytest.raises(FileNotFoundError):
        excel.list_excel_files(str(landing / "missing"))


def test_native_reader_options_use_bare_sheet_name():
    opts = excel.native_reader_options()
    assert opts["dataAddress"] == "Sheet1" and "!" not in opts["dataAddress"]
    assert opts["inferColumnTypes"] == "false" and opts["headerRows"] == "1"


# ---------------------------------------------------------------- ingestion
def test_ingest_committed_file_matches_contract(spark, t5_cfg, landing):
    src = _copy_committed(landing)
    summary = excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    table = t5_cfg.table("bronze", "file_transaction_excel")
    assert summary["table"] == table
    assert summary["rows_written"] == 7 and summary["rescued_rows"] == 0 and summary["table_rows"] == 7

    df = spark.table(table)
    assert df.columns == CONTRACT.column_names
    types = {f.name: f.dataType.simpleString() for f in df.schema}
    assert types["transaction_date"] == "timestamp"
    assert types["amount"] == "decimal(19,4)"
    assert types["transaction_id"] == types["account_id"] == types["branch_id"] == "int"

    rows = df.orderBy("transaction_id").collect()
    assert _business(rows) == EXPECTED
    assert {r["_source_file"] for r in rows} == {str(src)}
    assert all(r["_ingested_at"] is not None and r["_rescued_data"] is None for r in rows)


def test_rerun_same_file_is_idempotent(spark, t5_cfg, landing):
    _copy_committed(landing)
    excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False,
                       ingested_at=datetime(2024, 1, 1))
    second = excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False,
                                ingested_at=datetime(2024, 2, 1))
    table = spark.table(t5_cfg.table("bronze", "file_transaction_excel"))
    assert second["table_rows"] == 7 and table.count() == 7
    assert {r[0] for r in table.select("_ingested_at").distinct().collect()} == {datetime(2024, 2, 1)}

    # A second file appends; rerunning both still doesn't duplicate.
    _write_xlsx(landing / "transaction_excel_2.xlsx", [(99, 1, datetime(2024, 3, 1, 9, 0), 10, "Deposit", 2)])
    excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    third = excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    assert third["table_rows"] == 8 and len(third["files"]) == 2


def test_single_file_rerun_leaves_other_files_untouched(spark, t5_cfg, landing):
    _copy_committed(landing)
    other = _write_xlsx(landing / "b.xlsx", [(99, 1, datetime(2024, 3, 1), 10, "Deposit", 2)])
    excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    s = excel.ingest_excel(spark, t5_cfg, str(other), unity_catalog=False)
    assert s["rows_written"] == 1 and s["table_rows"] == 8


def test_empty_landing_folder_creates_table_and_writes_nothing(spark, t5_cfg, landing):
    s = excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    assert s["files"] == [] and s["rows_written"] == 0
    assert spark.table(t5_cfg.table("bronze", "file_transaction_excel")).count() == 0


def test_unparseable_cells_are_rescued_not_failed(spark, t5_cfg, landing):
    rows = [
        (1, 2, "20-01-2024 15:00:00", 100, "Deposit", 1, None),        # Talend text date pattern
        ("x1", 2, "not a date", "abc", "Payment", 1.5, "extra!"),      # every typed cell bad
        (3, "", datetime(2024, 1, 3), 12.34567, "Transfer", 2, None),  # blank cell, rounding
        (None, None, None, None, None, None, None),                    # empty row skipped
        (4, 2, 45309.5486111111, 7, "Deposit", 3, None),               # date cell lost its format
        (5, 2, "1/18/24 13:10", 99999999999999999999, "Deposit", 3, None),  # amount overflow
    ]
    _write_xlsx(landing / "bad.xlsx", rows, header=HEADER + ["Comment"])
    s = excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    assert s["rows_written"] == 5 and s["rescued_rows"] == 2

    rows_out = spark.table(t5_cfg.table("bronze", "file_transaction_excel")).collect()
    by_id = {r["transaction_id"]: r for r in rows_out if r["transaction_id"] is not None}
    by_type = {r["transaction_type"]: r for r in rows_out if r["transaction_type"] in ("Payment", "Transfer")}

    assert by_id[1]["transaction_date"] == datetime(2024, 1, 20, 15, 0) and by_id[1]["_rescued_data"] is None

    bad = by_type["Payment"]
    assert (bad["transaction_id"], bad["transaction_date"], bad["amount"], bad["branch_id"]) == (None,) * 4
    assert bad["account_id"] == 2
    assert json.loads(bad["_rescued_data"]) == {
        "transaction_id": "x1", "transaction_date": "not a date", "amount": "abc",
        "branch_id": "1.5", "comment": "extra!",
    }

    tr = by_type["Transfer"]
    assert tr["account_id"] is None and tr["_rescued_data"] is None
    assert tr["amount"] == Decimal("12.3457") and tr["transaction_date"] == datetime(2024, 1, 3)

    assert by_id[4]["transaction_date"] == datetime(2024, 1, 18, 13, 10) and by_id[4]["_rescued_data"] is None
    assert by_id[5]["transaction_date"] == datetime(2024, 1, 18, 13, 10)
    assert by_id[5]["amount"] is None and list(json.loads(by_id[5]["_rescued_data"])) == ["amount"]


def test_header_case_and_column_order_are_tolerated(spark, t5_cfg, landing):
    header = ["Branch_ID", " Amount ", "TRANSACTION_ID", "account_id", "transaction_type", "transaction_date"]
    _write_xlsx(landing / "reordered.xlsx", [(2, 5, 42, 7, "Deposit", datetime(2024, 5, 1, 1, 2, 3))], header=header)
    excel.ingest_excel(spark, t5_cfg, str(landing), unity_catalog=False)
    r = spark.table(t5_cfg.table("bronze", "file_transaction_excel")).collect()[0]
    assert (r["transaction_id"], r["account_id"], r["amount"], r["branch_id"]) == (42, 7, Decimal("5.0000"), 2)
    assert r["transaction_date"] == datetime(2024, 5, 1, 1, 2, 3) and r["_rescued_data"] is None


def test_typing_handles_native_reader_display_strings(spark):
    """Native reader with inferColumnTypes=false returns Excel display text (m/d/yy h:mm)."""
    raw = excel._string_frame(spark, [
        {"transaction_id": "6", "account_id": "6.0", "transaction_date": "1/18/24 13:10", "amount": "50000",
         "transaction_type": "Withdrawal", "branch_id": "1", "_source_file": "/Volumes/c/s/landing/x.xlsx"},
    ])
    r = excel.with_lineage(excel.type_and_rescue(raw)).collect()[0]
    assert (r["transaction_id"], r["account_id"], r["transaction_date"], r["amount"], r["branch_id"]) == (
        6, 6, datetime(2024, 1, 18, 13, 10), Decimal("50000.0000"), 1)
    assert r["_rescued_data"] is None


@pytest.mark.parametrize("text,expected", [
    ("2024-01-03 0:00:00", datetime(2024, 1, 3, 0, 0)),
    ("2024-01-20 15:00:00", datetime(2024, 1, 20, 15, 0)),
    ("03-01-2024 9:05:00", datetime(2024, 1, 3, 9, 5)),
    ("20-01-2024 15:00:00", datetime(2024, 1, 20, 15, 0)),
])
def test_typing_accepts_single_digit_hours(spark, text, expected):
    """Native reader renders midnight as 'yyyy-MM-dd 0:00:00' (found in the live run)."""
    raw = excel._string_frame(spark, [
        {"transaction_id": "1", "account_id": "1", "transaction_date": text, "amount": "1",
         "transaction_type": "Deposit", "branch_id": "1", "_source_file": "/Volumes/c/s/landing/x.xlsx"},
    ])
    r = excel.with_lineage(excel.type_and_rescue(raw)).collect()[0]
    assert r["transaction_date"] == expected and r["_rescued_data"] is None


def test_unknown_reader_rejected(spark, t5_cfg, landing):
    _copy_committed(landing)
    with pytest.raises(ValueError, match="reader"):
        excel.ingest_excel(spark, t5_cfg, str(landing), reader="crealytics", unity_catalog=False)


def test_replace_predicate_escapes_quotes():
    assert excel.replace_predicate(["dbfs:/Volumes/a/b/it's.xlsx"]) == "_source_file IN ('/Volumes/a/b/it\\'s.xlsx')"


def test_default_source_is_config_landing_path():
    cfg = EnvConfig.from_target("dev", schema_prefix="bdwh_t5_")
    assert cfg.landing_path("excel_transaction") == "/Volumes/migration_demo/bdwh_t5_bronze/landing/excel/transaction"


# ---------------------------------------------------------------- notebook
def test_notebook_follows_conventions():
    text = NOTEBOOK.read_text()
    assert text.startswith("# Databricks notebook source")
    assert "# MAGIC %run ../00_setup/00_config" in text
    assert "# MAGIC %pip install openpyxl" in text
    assert text.index("%pip install") < text.index("%run ../00_setup/00_config")
    assert "dbutils.notebook.exit(json.dumps(" in text
    for hard_coded in ("migration_demo", "\"banking_dwh.", "'banking_dwh.", "bdwh_"):
        assert hard_coded not in text
    assert ".try_cast(" not in (ROOT / "src/banking_dwh/bronze/excel.py").read_text()
