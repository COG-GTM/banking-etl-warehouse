"""Ticket 4: CSV -> Bronze (Auto Loader) parsing, typing, rescue, lineage and exactly-once."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

import pytest

from banking_dwh.bronze import csv_autoloader as t4
from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.tables import BRONZE_FILE_TRANSACTION_CSV as CONTRACT

HEADER = "transaction_id,account_id,transaction_date,amount,transaction_type,branch_id\n"


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _normalise(t: str) -> str:
    return t.replace(" ", "").lower()


# ----------------------------------------------------------------------------- spec
def test_reader_spec_matches_contract_and_talend():
    assert [n for n, _ in t4.source_columns()] == HEADER.strip().split(",")
    assert t4.source_schema() == (
        "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount DECIMAL(19,4), "
        "transaction_type STRING, branch_id INT"
    )
    assert t4.reader_schema() == (
        "transaction_id INT, account_id INT, transaction_date STRING, amount DECIMAL(19,4), "
        "transaction_type STRING, branch_id INT"
    )
    assert t4.CSV_OPTIONS["timestampFormat"] == "dd-MM-yyyy HH:mm:ss"
    assert t4.CSV_OPTIONS["header"] == "true" and t4.CSV_OPTIONS["sep"] == ","
    assert t4.CSV_OPTIONS["encoding"].upper() in ("UTF-8", "ISO-8859-1")


def test_autoloader_options_use_cfg_paths():
    cfg = EnvConfig.from_target("dev", schema_prefix="bdwh_t4_")
    opts = t4.autoloader_options(cfg)
    assert opts["cloudFiles.format"] == "csv"
    assert opts["rescuedDataColumn"] == "_rescued_data"
    assert opts["cloudFiles.schemaEvolutionMode"] == "rescue"
    assert opts["cloudFiles.schemaLocation"] == (
        "/Volumes/migration_demo/bdwh_t4_ops/checkpoints/csv_transaction/schema"
    )
    assert opts["timestampFormat"] == "dd-MM-yyyy HH:mm:ss"
    assert cfg.landing_path(*t4.LANDING) == "/Volumes/migration_demo/bdwh_t4_bronze/landing/csv/transaction"


# ----------------------------------------------------------------------------- repo sample
def test_batch_read_of_repo_csv_types_and_lineage(spark):
    df = t4.read_csv_batch(spark, str(t4.SAMPLE_CSV))
    assert df.columns == CONTRACT.column_names
    types = dict(df.dtypes)
    for c in CONTRACT.columns:
        assert _normalise(types[c.name]) == _normalise(c.type), c.name
    rows = {r.transaction_id: r for r in df.collect()}
    assert len(rows) == 12 and sorted(rows) == list(range(14, 26))
    first = rows[14]
    assert first.transaction_date == dt.datetime(2024, 1, 21, 14, 0, 0)
    assert first.amount == Decimal("1500000.0000")
    assert (first.account_id, first.transaction_type, first.branch_id) == (13, "Deposit", 4)
    assert rows[25].transaction_date == dt.datetime(2024, 1, 22, 14, 30, 0)  # last line has no newline
    assert all(r._rescued_data is None for r in rows.values())
    assert all(r._source_file.endswith("data_sources/transaction_csv.csv") for r in rows.values())
    assert all(r._ingested_at is not None for r in rows.values())


# ----------------------------------------------------------------------------- bad rows
def test_bad_values_are_rescued_not_failed(spark, tmp_path):
    f = _write(
        tmp_path / "bad" / "bad.csv",
        HEADER
        + "100,13,21-01-2024 14:00:00,1500000,Deposit,4\n"  # good
        + "101,13,2024-01-21 14:00:00,abc,Deposit,4\n"  # wrong date format + non-numeric amount
        + "x102,13,22-01-2024 09:00:00,100,Payment,1\n"  # non-numeric id
        + "\n"  # empty row (Talend REMOVE_EMPTY_ROW)
        + "103,14,32-01-2024 09:00:00,100,Transfer,2,surplus\n"  # impossible day + extra column
        + "104,15\n",  # short row
    )
    df = t4.read_csv_batch(spark, str(f))
    rows = df.orderBy("_rescued_data").collect()
    assert len(rows) == 5
    by_type = {r.transaction_type: r for r in rows}

    good = next(r for r in rows if r.transaction_id == 100)
    assert good._rescued_data is None and good.transaction_date == dt.datetime(2024, 1, 21, 14)

    r101 = next(r for r in rows if r.transaction_id == 101)
    assert r101.transaction_type == "Deposit" and r101.branch_id == 4
    assert r101.transaction_date is None and r101.amount is None
    rescued = json.loads(r101._rescued_data)
    assert rescued["transaction_date"] == "2024-01-21 14:00:00" and rescued["amount"] == "abc"
    assert rescued["_file_path"].endswith("bad.csv")
    assert "transaction_id" not in rescued

    r102 = by_type["Payment"]
    assert r102.transaction_id is None and r102.account_id == 13
    assert json.loads(r102._rescued_data)["transaction_id"] == "x102"

    r103 = by_type["Transfer"]
    assert r103.transaction_date is None
    rescued = json.loads(r103._rescued_data)
    # Impossible date is rescued (native Auto Loader would only null it); the trailing value
    # with no header column is dropped, as Auto Loader does.
    assert set(rescued) == {"transaction_date", "_file_path"}
    assert rescued["transaction_date"] == "32-01-2024 09:00:00"

    short = next(r for r in rows if r.transaction_id == 104)
    assert short.account_id == 15 and short.transaction_date is None and short._rescued_data is None

    rejects = df.where(t4.REJECT_PREDICATE).count()
    assert rejects == 3  # 101, x102, 103 (Talend REJECT flow)


# ----------------------------------------------------------------------------- write + exactly-once
@pytest.fixture()
def stream_dirs(tmp_path):
    return {
        "landing": tmp_path / "landing" / "csv" / "transaction",
        "checkpoint": str(tmp_path / "ops" / "checkpoints" / "csv_transaction" / "checkpoint"),
    }


def _ingest(spark, cfg, d):
    return t4.ingest(
        spark, cfg, source_dir=str(d["landing"]), checkpoint_location=d["checkpoint"],
        autoloader=False, unity_catalog=False,
    )


def test_stream_ingest_is_exactly_once(spark, local_schemas, stream_dirs):
    cfg = local_schemas
    target = cfg.table("bronze", CONTRACT.name)
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    t4.land_sample_csv(str(stream_dirs["landing"]))

    first = _ingest(spark, cfg, stream_dirs)
    assert (first["rows_added"], first["stream_input_rows"], first["rows_total"]) == (12, 12, 12)
    assert first["column_types"]["transaction_date"] == "timestamp"
    assert first["rescued_rows_total"] == 0 and first["files_total"] == 1

    second = _ingest(spark, cfg, stream_dirs)
    assert (second["rows_added"], second["stream_input_rows"], second["rows_total"]) == (0, 0, 12)

    _write(stream_dirs["landing"] / "late.csv", HEADER + "26,13,23-01-2024 10:00:00,1000,Deposit,1\n27,13,bad,1,Deposit,1\n")
    third = _ingest(spark, cfg, stream_dirs)
    assert (third["rows_added"], third["rows_total"], third["rescued_rows_total"]) == (2, 14, 1)

    stored = spark.table(target)
    assert stored.columns == CONTRACT.column_names
    assert stored.where("_source_file IS NULL OR _ingested_at IS NULL").count() == 0
    spark.sql(f"DROP TABLE IF EXISTS {target}")


# ----------------------------------------------------------------------------- demo helper
def test_land_sample_csv_is_idempotent(tmp_path):
    dest = tmp_path / "landing"
    out = t4.land_sample_csv(str(dest))
    assert out["copied"] and (dest / "transaction_csv.csv").read_bytes() == t4.SAMPLE_CSV.read_bytes()
    assert t4.land_sample_csv(str(dest)) == {"copied": False, "path": out["path"]}
    assert t4.land_sample_csv(str(dest), overwrite=True)["copied"]
    with pytest.raises(FileNotFoundError):
        t4.land_sample_csv(str(dest), tmp_path / "missing.csv")


def test_land_sample_commands_target_scratch_volume(capsys):
    cfg = EnvConfig.from_target("dev", schema_prefix="bdwh_t4_")
    mkdirs, cp = t4.land_sample_commands(cfg)
    vol = "dbfs:/Volumes/migration_demo/bdwh_t4_bronze/landing/csv/transaction"
    assert mkdirs == ["databricks", "fs", "mkdirs", vol]
    assert cp[:3] == ["databricks", "fs", "cp"] and cp[-1] == f"{vol}/transaction_csv.csv"
    t4.main(["land", "--schema-prefix", "bdwh_t4_", "--dry-run"])
    assert f"databricks fs mkdirs {vol}" in capsys.readouterr().out
    with pytest.raises(ValueError):
        EnvConfig.from_target("dev", schema_prefix="bad;prefix")


def test_to_bronze_parses_dates_and_merges_rescue(spark, tmp_path):
    """to_bronze is the transform the cloudFiles stream uses: native rescue JSON is kept and
    a date-parse failure is merged into it."""
    f = _write(tmp_path / "native" / "n.csv", HEADER + "1,2,99-99-2024 00:00:00,abc,Deposit,3\n2,2,01-02-2024 23:59:59,5,Deposit,3\n")
    raw = spark.read.options(**t4.CSV_OPTIONS).schema(t4.reader_schema()).csv(str(f))
    from pyspark.sql import functions as F

    native = raw.withColumn(
        "_rescued_data",
        F.when(F.col("transaction_id") == 1, F.lit('{"amount":"abc","_file_path":"/Volumes/x/n.csv"}')),
    )
    rows = {r.transaction_id: r for r in t4.to_bronze(native).collect()}
    assert json.loads(rows[1]._rescued_data) == {
        "amount": "abc", "_file_path": "/Volumes/x/n.csv", "transaction_date": "99-99-2024 00:00:00",
    }
    assert rows[1].transaction_date is None
    assert rows[2]._rescued_data is None and rows[2].transaction_date == dt.datetime(2024, 2, 1, 23, 59, 59)
