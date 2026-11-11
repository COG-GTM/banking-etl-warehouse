"""Spark tests: transaction CSV / Excel drops into append-only bronze Delta tables."""

import datetime as dt
import io
import json
import shutil

import pytest
from banking_etl.ingestion.metadata import METADATA_COLUMNS
from banking_etl.ingestion.run import run_bronze_ingestion
from banking_etl.ingestion.sources import TRANSACTION_CSV, TRANSACTION_EXCEL

EXPECTED_TYPES = {
    "transaction_id": "int",
    "account_id": "int",
    "transaction_date": "timestamp",
    "amount": "int",
    "transaction_type": "string",
    "branch_id": "int",
    "_source_system": "string",
    "_source_file": "string",
    "_file_modification_time": "timestamp",
    "_ingested_at": "timestamp",
    "_ingest_run_id": "string",
    "_rescued_data": "string",
}


def _land(cfg, source, src_path, name=None):
    target = cfg.landing_path(source.name)
    import os

    os.makedirs(target, exist_ok=True)
    dest = os.path.join(target, name or src_path.name)
    shutil.copy(src_path, dest)
    return dest


def _write(cfg, source, name, content):
    import os

    target = cfg.landing_path(source.name)
    os.makedirs(target, exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    with open(os.path.join(target, name), mode) as fh:
        fh.write(content)


def _ingest(spark, cfg, source, run_id="run-1"):
    return run_bronze_ingestion(
        spark, cfg, sources=[source.name], use_auto_loader=False, run_id=run_id, create_objects=False
    )


def _rows(spark, cfg, source):
    return spark.table(cfg.table(source.name)).orderBy("_ingested_at", "transaction_id").collect()


def test_real_csv_lands_typed_with_metadata(spark, bronze_config, data_sources):
    _land(bronze_config, TRANSACTION_CSV, data_sources / "transaction_csv.csv")
    summary = _ingest(spark, bronze_config, TRANSACTION_CSV)

    df = spark.table(bronze_config.table("transaction_csv"))
    assert dict(df.dtypes) == EXPECTED_TYPES
    assert summary["sources"]["transaction_csv"] == bronze_config.table("transaction_csv")
    rows = {r.transaction_id: r for r in df.collect()}
    assert sorted(rows) == list(range(14, 26))
    r14 = rows[14]
    assert (r14.account_id, r14.amount, r14.transaction_type, r14.branch_id) == (13, 1500000, "Deposit", 4)
    assert r14.transaction_date == dt.datetime(2024, 1, 21, 14, 0, 0)
    assert rows[25].transaction_date == dt.datetime(2024, 1, 22, 14, 30, 0)
    assert sum(r.amount for r in rows.values()) == 7180000
    for r in rows.values():
        assert r._source_system == "file:transaction_csv"
        assert r._source_file.endswith("/transaction_csv/transaction_csv.csv")
        assert r._file_modification_time is not None
        assert r._ingested_at is not None
        assert r._ingest_run_id == "run-1"
        assert r._rescued_data is None


def test_real_excel_lands_typed_with_metadata(spark, bronze_config, data_sources):
    _land(bronze_config, TRANSACTION_EXCEL, data_sources / "transaction_excel.xlsx")
    _ingest(spark, bronze_config, TRANSACTION_EXCEL)

    df = spark.table(bronze_config.table("transaction_excel"))
    assert dict(df.dtypes) == {**EXPECTED_TYPES, "_excel_sheet": "string", "_excel_row": "int"}
    assert df.columns[-len(METADATA_COLUMNS) :] == list(METADATA_COLUMNS)
    rows = {r.transaction_id: r for r in df.collect()}
    assert sorted(rows) == [6, 7, 11, 12, 13, 14, 15]
    assert rows[6].transaction_date == dt.datetime(2024, 1, 18, 13, 10)
    assert rows[15].transaction_date == dt.datetime(2024, 1, 21, 8, 0)
    assert (rows[11].amount, rows[11].transaction_type) == (1000000, "Transfer")
    assert sum(r.amount for r in rows.values()) == 4150000
    assert {r._excel_row for r in rows.values()} == set(range(2, 9))
    assert all(r._rescued_data is None for r in rows.values())
    assert all(r._source_system == "file:transaction_excel" for r in rows.values())
    assert all(r._source_file.endswith("/transaction_excel/transaction_excel.xlsx") for r in rows.values())


def test_csv_bad_values_are_rescued_not_dropped_or_silently_nulled(spark, bronze_config):
    _write(
        bronze_config,
        TRANSACTION_CSV,
        "bad.csv",
        "transaction_id,account_id,transaction_date,amount,transaction_type,branch_id\n"
        "100,1,31-02-2024 10:00:00,500,Deposit,1\n"  # impossible date
        "101,1,2024-01-21 14:00:00,500,Deposit,1\n"  # wrong date format
        "102,x1,21-01-2024 14:00:00,1.5,Deposit,\n"  # bad int, fractional amount, blank id
        "103,1,21-01-2024 14:00:00,500,Deposit,1,EXTRA\n"  # extra column
        "104,1,21-01-2024 25:61:00,500,Deposit,1\n",  # impossible time
    )
    _ingest(spark, bronze_config, TRANSACTION_CSV)
    rows = {r.transaction_id: r for r in spark.table(bronze_config.table("transaction_csv")).collect()}
    assert sorted(rows) == [100, 101, 102, 103, 104]

    assert rows[100].transaction_date is None
    assert json.loads(rows[100]._rescued_data) == {"transaction_date": "31-02-2024 10:00:00"}
    assert json.loads(rows[101]._rescued_data) == {"transaction_date": "2024-01-21 14:00:00"}
    assert json.loads(rows[104]._rescued_data) == {"transaction_date": "21-01-2024 25:61:00"}

    assert rows[102].account_id is None and rows[102].amount is None and rows[102].branch_id is None
    assert json.loads(rows[102]._rescued_data) == {"account_id": "x1", "amount": "1.5"}
    assert rows[102].transaction_date == dt.datetime(2024, 1, 21, 14, 0)

    rescued_103 = json.loads(rows[103]._rescued_data)
    assert rescued_103["_corrupt_record"].endswith(",EXTRA")
    assert rows[103].amount == 500


def test_excel_bad_cell_does_not_fail_file_and_unreadable_file_is_rescued(spark, bronze_config):
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(TRANSACTION_EXCEL.column_names)
    ws.append([1, 1, dt.datetime(2024, 1, 1, 9, 0), 100, "Deposit", 1])
    ws.append([2, 1, "not-a-date", "abc", "Deposit", 1])
    ws.append([3, 1, "21-01-2024 14:00:00", 300, "Deposit", 1])
    buf = io.BytesIO()
    wb.save(buf)
    _write(bronze_config, TRANSACTION_EXCEL, "mixed.xlsx", buf.getvalue())
    _write(bronze_config, TRANSACTION_EXCEL, "broken.xlsx", b"PK\x03\x04 truncated")

    _ingest(spark, bronze_config, TRANSACTION_EXCEL)
    rows = spark.table(bronze_config.table("transaction_excel")).collect()
    good = {r.transaction_id: r for r in rows if r.transaction_id is not None}
    assert sorted(good) == [1, 2, 3]
    assert good[1].transaction_date == dt.datetime(2024, 1, 1, 9, 0)
    assert good[3].transaction_date == dt.datetime(2024, 1, 21, 14, 0)
    assert good[2].transaction_date is None and good[2].amount is None
    assert json.loads(good[2]._rescued_data) == {"transaction_date": "not-a-date", "amount": "abc"}

    broken = [r for r in rows if r.transaction_id is None]
    assert len(broken) == 1
    assert broken[0]._source_file.endswith("broken.xlsx")
    assert json.loads(broken[0]._rescued_data)["_corrupt_record"].startswith("unreadable workbook")


@pytest.mark.parametrize("source", [TRANSACTION_CSV, TRANSACTION_EXCEL], ids=lambda s: s.name)
def test_bronze_is_append_only_ingestion_history(spark, bronze_config, data_sources, source):
    sample = data_sources / source.sample_file
    _land(bronze_config, source, sample)
    _ingest(spark, bronze_config, source, run_id="run-1")
    first = spark.table(bronze_config.table(source.name)).count()
    assert first > 0

    _ingest(spark, bronze_config, source, run_id="run-2")
    assert spark.table(bronze_config.table(source.name)).count() == first

    _land(bronze_config, source, sample, name="redelivery" + sample.suffix)
    _ingest(spark, bronze_config, source, run_id="run-3")
    df = spark.table(bronze_config.table(source.name))
    assert df.count() == 2 * first
    by_run = {r["_ingest_run_id"]: r["count"] for r in df.groupBy("_ingest_run_id").count().collect()}
    assert by_run == {"run-1": first, "run-3": first}
    assert df.select("_source_file").distinct().count() == 2
