from datetime import datetime
from io import BytesIO
from pathlib import Path

import pytest
import yaml
from openpyxl import Workbook
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType, TimestampType

from banking_etl.bronze.files import (
    CSV_SCHEMA_HINTS,
    METADATA_COLUMNS,
    TRANSACTION_COLUMNS,
    apply_schema_hints,
    csv_autoloader_options,
    excel_autoloader_options,
    file_source,
    infer_excel_schema,
    land_transaction_files,
    landing_targets,
    parse_excel_bytes,
    parse_sources,
    read_transaction_csv,
    read_transaction_excel,
    with_file_metadata,
    write_available_now,
)
from banking_etl.config import Settings

ROOT = Path(__file__).resolve().parents[2]
DATA_SOURCES = ROOT.parent / "data_sources"
CSV_PATH = DATA_SOURCES / "transaction_csv.csv"
XLSX_PATH = DATA_SOURCES / "transaction_excel.xlsx"
DEV = Settings(catalog="migration_demo", schema_prefix="banking_etl_")
EXPECTED_COLUMNS = [*TRANSACTION_COLUMNS, *METADATA_COLUMNS]


def _types(df):
    return {f.name: f.dataType for f in df.schema.fields}


def _assert_metadata(rows, source, file_name):
    for r in rows:
        assert r["_rescued_data"] is None
        assert isinstance(r["_ingested_at"], datetime)
        assert r["_source"] == source
        assert r["_source_file"].startswith("file:") and r["_source_file"].endswith(file_name)


# ----------------------------------------------------------------------------- CSV


def test_csv_batch_read_has_12_rows_raw_date_and_metadata(spark):
    df = read_transaction_csv(spark, str(CSV_PATH))
    assert df.columns == EXPECTED_COLUMNS
    types = _types(df)
    assert types["transaction_date"] == StringType()
    assert types["transaction_id"] == IntegerType() and types["amount"] == IntegerType()
    assert types["_ingested_at"] == TimestampType()
    rows = df.orderBy("transaction_id").collect()
    assert len(rows) == 12
    assert [r.transaction_id for r in rows] == list(range(14, 26))
    first = rows[0]
    assert (first.account_id, first.transaction_date, first.amount, first.transaction_type, first.branch_id) == (
        13, "21-01-2024 14:00:00", 1500000, "Deposit", 4,
    )
    assert sum(r.amount for r in rows) == 7_180_000
    _assert_metadata(rows, "file_csv", "transaction_csv.csv")


def test_apply_schema_hints_overrides_only_hinted_columns():
    inferred = StructType([StructField("transaction_id", IntegerType()), StructField("transaction_date", TimestampType())])
    hinted = apply_schema_hints(inferred, CSV_SCHEMA_HINTS)
    assert [(f.name, f.dataType) for f in hinted] == [("transaction_id", IntegerType()), ("transaction_date", StringType())]


def test_csv_autoloader_options():
    opts = csv_autoloader_options(file_source(DEV, "csv"))
    assert opts["cloudFiles.format"] == "csv"
    assert opts["cloudFiles.schemaHints"] == "transaction_date STRING"
    assert opts["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert opts["cloudFiles.inferColumnTypes"] == "true"
    assert opts["rescuedDataColumn"] == "_rescued_data"
    assert opts["header"] == "true"
    assert opts["cloudFiles.schemaLocation"] == (
        "/Volumes/migration_demo/banking_etl_ops/checkpoints/bronze/file_transaction_csv/_schema"
    )


# ----------------------------------------------------------------------------- Excel


def test_excel_batch_read_has_7_rows_and_metadata(spark):
    df = read_transaction_excel(spark, str(XLSX_PATH))
    assert df.columns == EXPECTED_COLUMNS
    types = _types(df)
    # Same types the native Databricks Excel reader infers for this sheet.
    assert types["transaction_id"] == LongType() and types["amount"] == LongType() and types["branch_id"] == LongType()
    assert types["transaction_date"] == TimestampNTZType()
    assert types["transaction_type"] == StringType()
    rows = df.orderBy("transaction_id").collect()
    assert len(rows) == 7
    assert [r.transaction_id for r in rows] == [6, 7, 11, 12, 13, 14, 15]
    assert rows[0].transaction_date == datetime(2024, 1, 18, 13, 10)
    assert (rows[0].account_id, rows[0].amount, rows[0].transaction_type, rows[0].branch_id) == (6, 50000, "Withdrawal", 1)
    _assert_metadata(rows, "file_excel", "transaction_excel.xlsx")


def _xlsx(rows, sheet="Sheet1"):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    for r in rows:
        ws.append(r)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_parse_excel_bytes_skips_blank_rows_and_pads_short_rows():
    content = _xlsx([["a", "b", "c"], [1, 2.5, "x"], [None, None, None], [2, None]])
    header, rows = parse_excel_bytes(content)
    assert header == ["a", "b", "c"]
    assert rows == [(1, 2.5, "x"), (2, None, None)]
    assert [f.dataType.typeName() for f in infer_excel_schema(header, rows)] == ["long", "double", "string"]


def test_parse_excel_bytes_unknown_sheet():
    with pytest.raises(ValueError, match="Sheet9"):
        parse_excel_bytes(_xlsx([["a"], [1]]), sheet="Sheet9")


def test_excel_autoloader_options_use_native_reader_without_evolution():
    opts = excel_autoloader_options(file_source(DEV, "excel"))
    assert opts["cloudFiles.format"] == "excel"
    assert opts["cloudFiles.schemaEvolutionMode"] == "none"
    assert opts["headerRows"] == "1" and opts["dataAddress"] == "Sheet1"
    assert opts["cloudFiles.schemaLocation"].endswith("/checkpoints/bronze/file_transaction_excel/_schema")


# ----------------------------------------------------------------------------- locations / sources


def test_file_source_resolves_through_settings():
    src = file_source(DEV, "excel")
    assert src.table == "migration_demo.banking_etl_bronze.file_transaction_excel"
    assert src.landing_dir == "/Volumes/migration_demo/banking_etl_bronze/landing/transactions/excel"
    assert src.checkpoint_location == (
        "/Volumes/migration_demo/banking_etl_ops/checkpoints/bronze/file_transaction_excel/_checkpoint"
    )
    assert file_source(Settings(catalog="c"), "csv").table == "c.bronze.file_transaction_csv"
    with pytest.raises(ValueError):
        file_source(DEV, "json")


def test_parse_sources():
    assert parse_sources("") == ("csv", "excel")
    assert parse_sources(None) == ("csv", "excel")
    assert parse_sources(" Excel , csv,excel") == ("excel", "csv")
    with pytest.raises(ValueError):
        parse_sources("csv,parquet")


# ----------------------------------------------------------------------------- incremental idempotency


def test_available_now_stream_appends_each_file_once(spark, tmp_path):
    """Same write path as Auto Loader, driven by the OSS file stream source: re-runs are no-ops."""
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "transaction_csv.csv").write_bytes(CSV_PATH.read_bytes())
    schema = read_transaction_csv(spark, str(CSV_PATH)).select(*TRANSACTION_COLUMNS).schema
    table = "bronze.file_transaction_csv_stream_test"
    checkpoint = str(tmp_path / "checkpoint")

    def run():
        stream = spark.readStream.schema(schema).option("header", "true").csv(str(landing))
        write_available_now(with_file_metadata(stream, "csv"), table, checkpoint)
        return spark.table(table)

    assert run().count() == 12
    assert run().count() == 12  # second run: no new files -> no-op
    (landing / "transaction_csv_2.csv").write_text(
        "transaction_id,account_id,transaction_date,amount,transaction_type,branch_id\n"
        "26,23,23-01-2024 09:00:00,1000,Deposit,1\n"
    )
    df = run()
    assert df.count() == 13
    assert df.columns == EXPECTED_COLUMNS
    assert df.filter("transaction_id = 26").select("_source_file").first()[0].endswith("transaction_csv_2.csv")
    spark.sql(f"DROP TABLE {table}")


# ----------------------------------------------------------------------------- landing helper


def test_land_transaction_files_copies_then_skips(tmp_path):
    volume_root = tmp_path / "vol"

    def to_local(target):
        return volume_root / target.lstrip("/")

    def copy(src, dst):
        p = to_local(dst)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(Path(src).read_bytes())

    def exists(dst):
        return to_local(dst).exists()

    first = land_transaction_files(DEV, DATA_SOURCES, copy=copy, exists=exists)
    assert first == [
        ("/Volumes/migration_demo/banking_etl_bronze/landing/transactions/csv/transaction_csv.csv", "copied"),
        ("/Volumes/migration_demo/banking_etl_bronze/landing/transactions/excel/transaction_excel.xlsx", "copied"),
    ]
    assert to_local(first[1][0]).read_bytes() == XLSX_PATH.read_bytes()
    second = land_transaction_files(DEV, DATA_SOURCES, copy=copy, exists=exists)
    assert [s for _, s in second] == ["skipped (exists)"] * 2
    third = land_transaction_files(DEV, DATA_SOURCES, sources="csv", overwrite=True, copy=copy, exists=exists)
    assert [s for _, s in third] == ["copied"]


def test_land_transaction_files_missing_source(tmp_path):
    with pytest.raises(FileNotFoundError):
        land_transaction_files(DEV, tmp_path, copy=lambda *_: None, exists=lambda _: False)
    assert landing_targets(DEV, tmp_path, "excel")[0][0].endswith("transaction_excel.xlsx")


# ----------------------------------------------------------------------------- bundle resource


def test_job_resource_runs_notebook_on_serverless():
    doc = yaml.safe_load((ROOT / "resources" / "ticket-03-file-ingestion.yml").read_text())
    job = doc["resources"]["jobs"]["bronze_ingest_transaction_files"]
    (task,) = job["tasks"]
    path = (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve()
    assert path == ROOT / "notebooks" / "bronze" / "ingest_transaction_files.py"
    assert {p["name"] for p in job["parameters"]} == {"catalog", "schema_prefix", "sources", "land_from"}
    assert not ({"new_cluster", "job_cluster_key", "existing_cluster_id"} & set(task))
    assert task["max_retries"] >= 1
