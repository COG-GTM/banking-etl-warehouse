import json
import re
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from banking_etl.bronze.sqlserver import (
    AUDIT_COLUMNS,
    add_audit_columns,
    ingest_all,
    ingest_table,
    load_config,
    load_manifest,
    new_batch_id,
    staged_reader,
    staged_source_label,
    validate_staged_extract,
    verify_against_manifest,
)

TS = datetime(2026, 1, 1, 12, 0, 0)


def frame_reader(frames):
    return lambda table: frames[table.name]


def ingest(spark, frames, table_name, schema_prefix, batch_id, mode=None):
    config = load_config()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_prefix}bronze")
    return ingest_table(
        spark,
        config.table(table_name),
        frame_reader(frames),
        target_schema=f"{schema_prefix}bronze",
        source=f"sqlserver://sample/dbo.{table_name}",
        batch_id=batch_id,
        ingested_at=TS,
        mode=mode,
    )


def test_new_batch_id_is_sortable_and_unique():
    a, b = new_batch_id(), new_batch_id()
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", a)
    assert a != b


def test_audit_columns_appended_after_source_columns(source_frames):
    df = add_audit_columns(source_frames["branch"], TS, "sqlserver://sample/dbo.branch", "b1")
    assert df.columns == ["branch_id", "branch_name", "branch_location", *AUDIT_COLUMNS]
    row = df.select(*AUDIT_COLUMNS).distinct().collect()
    assert [tuple(r) for r in row] == [(TS, "sqlserver://sample/dbo.branch", "b1")]
    assert dict(df.dtypes)["_ingested_at"] == "timestamp"


def test_audit_column_clash_rejected(source_frames):
    clashing = source_frames["branch"].withColumn("_batch_id", F.lit("x"))
    with pytest.raises(ValueError, match="audit"):
        add_audit_columns(clashing, TS, "s", "b")


def test_full_ingest_all_tables_preserves_source_schema(spark, source_frames, fixture_meta, schema_prefix):
    config = load_config()
    results = ingest_all(
        spark, config, frame_reader(source_frames), lambda t: f"sqlserver://sample/dbo.{t.name}",
        catalog=None, schema_prefix=schema_prefix, batch_id="batch-1",
    )
    counts = {r.table: (r.rows_written, r.target_rows) for r in results}
    assert counts == {t: (n, n) for t, n in fixture_meta["row_counts"].items()}
    for name, src in source_frames.items():
        bronze = spark.table(f"{schema_prefix}bronze.sqlserver_{name}")
        assert bronze.dtypes == src.dtypes + [
            ("_ingested_at", "timestamp"), ("_source", "string"), ("_batch_id", "string")
        ]
        assert bronze.drop(*AUDIT_COLUMNS).exceptAll(src).count() == 0
        assert bronze.select("_batch_id").distinct().collect()[0][0] == "batch-1"
    # one _ingested_at for the whole batch
    stamps = {
        spark.table(f"{schema_prefix}bronze.sqlserver_{n}").select("_ingested_at").first()[0]
        for n in source_frames
    }
    assert len(stamps) == 1


def test_full_mode_rerun_replaces_snapshot(spark, source_frames, schema_prefix):
    ingest(spark, source_frames, "branch", schema_prefix, "b1")
    changed = {"branch": source_frames["branch"].filter("branch_id <= 3")}
    result = ingest(spark, changed, "branch", schema_prefix, "b2")
    bronze = spark.table(f"{schema_prefix}bronze.sqlserver_branch")
    assert (result.rows_written, result.target_rows) == (3, 3)
    assert [r[0] for r in bronze.select("_batch_id").distinct().collect()] == ["b2"]


def test_incremental_appends_only_rows_above_watermark(spark, source_frames, schema_prefix):
    tx = source_frames["transaction_db"]
    first = ingest(spark, {"transaction_db": tx.filter("transaction_id <= 6")}, "transaction_db", schema_prefix, "b1")
    assert (first.mode, first.rows_written, first.watermark_after) == ("incremental(initial)", 6, 6)

    second = ingest(spark, {"transaction_db": tx}, "transaction_db", schema_prefix, "b2")
    assert (second.mode, second.rows_written, second.target_rows) == ("incremental", 4, 10)
    assert (second.watermark_before, second.watermark_after) == (6, 10)

    rerun = ingest(spark, {"transaction_db": tx}, "transaction_db", schema_prefix, "b3")
    assert (rerun.rows_written, rerun.target_rows) == (0, 10)

    bronze = spark.table(f"{schema_prefix}bronze.sqlserver_transaction_db")
    by_batch = {r["_batch_id"]: r["n"] for r in bronze.groupBy("_batch_id").agg(F.count("*").alias("n")).collect()}
    assert by_batch == {"b1": 6, "b2": 4}
    assert bronze.groupBy("transaction_id").count().filter("count > 1").count() == 0


def test_incremental_on_timestamp_watermark(spark, source_frames, schema_prefix):
    acct = source_frames["account"]
    dates = sorted(r[0] for r in acct.select("date_opened").distinct().collect() if r[0] is not None)
    cutoff = dates[len(dates) // 2]
    ingest(spark, {"account": acct.filter(F.col("date_opened") <= F.lit(cutoff))}, "account", schema_prefix, "b1", mode="incremental")
    result = ingest(spark, {"account": acct}, "account", schema_prefix, "b2", mode="incremental")
    assert result.watermark_before == cutoff
    assert result.rows_written == acct.filter(F.col("date_opened") > F.lit(cutoff)).count() > 0
    assert result.target_rows == acct.filter(F.col("date_opened").isNotNull()).count()
    assert result.watermark_after == dates[-1]


def test_mode_override_full_rebuilds_incremental_table(spark, source_frames, schema_prefix):
    tx = source_frames["transaction_db"]
    ingest(spark, {"transaction_db": tx}, "transaction_db", schema_prefix, "b1")
    result = ingest(spark, {"transaction_db": tx}, "transaction_db", schema_prefix, "b2", mode="full")
    assert (result.mode, result.rows_written, result.target_rows) == ("full", 10, 10)


def test_incremental_override_without_watermark_rejected(spark, source_frames, schema_prefix):
    with pytest.raises(ValueError, match="watermark_column"):
        ingest(spark, source_frames, "branch", schema_prefix, "b1", mode="incremental")


def write_staged_extract(source_frames, fixture_meta, root):
    for name, df in source_frames.items():
        df.coalesce(1).write.mode("overwrite").parquet(str(root / name))
    manifest = {
        "source_system": "sqlserver",
        "database": "sample",
        "schema": "dbo",
        "extracted_at": "2026-01-01T00:00:00+00:00",
        "tables": {n: {"row_count": c} for n, c in fixture_meta["row_counts"].items()},
    }
    (root / "_manifest.json").write_text(json.dumps(manifest))
    return manifest


def test_staged_extract_goes_through_same_writer(spark, source_frames, fixture_meta, schema_prefix, tmp_path):
    root = tmp_path / "landing" / "sqlserver" / "latest"
    write_staged_extract(source_frames, fixture_meta, root)
    manifest = load_manifest(str(root))
    config = load_config()
    results = ingest_all(
        spark, config, staged_reader(spark, str(root)), staged_source_label(str(root), manifest, config),
        catalog=None, schema_prefix=schema_prefix, batch_id="staged-1",
    )
    assert verify_against_manifest(results, manifest) == []
    assert {r.table: r.target_rows for r in results} == fixture_meta["row_counts"]
    tx = spark.table(f"{schema_prefix}bronze.sqlserver_transaction_db")
    assert tx.select("_source").first()[0] == (
        f"sqlserver://sample/dbo.transaction_db@staged:{root}/transaction_db"
    )
    assert tx.drop(*AUDIT_COLUMNS).dtypes == source_frames["transaction_db"].dtypes
    assert tx.drop(*AUDIT_COLUMNS).exceptAll(source_frames["transaction_db"]).count() == 0


def test_manifest_mismatch_reported(spark, source_frames, fixture_meta, schema_prefix, tmp_path):
    root = tmp_path / "extract"
    manifest = write_staged_extract(source_frames, fixture_meta, root)
    manifest["tables"]["city"]["row_count"] = 999
    config = load_config()
    results = ingest_all(
        spark, config, staged_reader(spark, str(root)), staged_source_label(str(root), manifest, config),
        catalog=None, schema_prefix=schema_prefix, tables=["city"], batch_id="staged-2",
    )
    assert verify_against_manifest(results, manifest) == ["city: bronze has 52 rows, extract had 999"]


def test_missing_manifest_rejected(tmp_path):
    with pytest.raises(FileNotFoundError, match="_manifest.json"):
        load_manifest(str(tmp_path))


def test_staged_extract_validated_before_any_write(spark, source_frames, fixture_meta, tmp_path):
    root = tmp_path / "extract"
    manifest = write_staged_extract(source_frames, fixture_meta, root)
    config = load_config()
    assert validate_staged_extract(spark, str(root), config, manifest) == []

    manifest["tables"]["branch"]["row_count"] = 6
    del manifest["tables"]["city"]
    assert validate_staged_extract(spark, str(root), config, manifest, ["branch", "city", "state"]) == [
        "branch: staged parquet has 5 rows, manifest says 6",
        "city: no row_count in _manifest.json",
    ]


def test_incremental_override_for_all_tables_explains_eligible_tables(spark, source_frames, schema_prefix):
    with pytest.raises(ValueError, match=r"Restrict tables to \['account', 'transaction_db'\]"):
        ingest_all(
            spark, load_config(), frame_reader(source_frames), lambda t: t.name,
            catalog=None, schema_prefix=schema_prefix, mode="incremental",
        )
    assert not spark.catalog.tableExists(f"{schema_prefix}bronze.sqlserver_branch")


def test_rows_written_counts_this_write_even_when_batch_id_reused(spark, source_frames, schema_prefix):
    tx = source_frames["transaction_db"]
    first = ingest(spark, {"transaction_db": tx.filter("transaction_id <= 6")}, "transaction_db", schema_prefix, "same")
    second = ingest(spark, {"transaction_db": tx}, "transaction_db", schema_prefix, "same")
    rerun = ingest(spark, {"transaction_db": tx}, "transaction_db", schema_prefix, "same")
    assert (first.rows_written, second.rows_written, rerun.rows_written) == (6, 4, 0)
    assert rerun.target_rows == 10
