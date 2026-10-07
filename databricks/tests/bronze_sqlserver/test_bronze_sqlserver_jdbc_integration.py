"""Against SQL Server 2022 (Docker) with data_sources/sample.bak restored.

Skipped unless SQLSERVER_PASSWORD is set and the server is reachable, e.g.:
    SQLSERVER_USER=sa SQLSERVER_PASSWORD=... pytest tests/bronze_sqlserver
"""

from pyspark.sql import functions as F

from banking_etl.bronze.sqlserver import (
    AUDIT_COLUMNS,
    check_coverage,
    discover_tables,
    extract_to_parquet,
    ingest_all,
    ingest_table,
    jdbc_reader,
    jdbc_source_label,
    load_config,
    load_manifest,
    staged_reader,
    staged_source_label,
    verify_against_manifest,
)


def test_discovered_source_tables_match_config(spark, sqlserver_conn):
    config = load_config()
    found = discover_tables(spark, sqlserver_conn)
    assert found == ["account", "branch", "city", "customer", "state", "sysdiagrams", "transaction_db"]
    assert check_coverage(config, found) == {"unconfigured": [], "missing_in_source": []}


def test_jdbc_ingest_row_counts_match_source(spark, sqlserver_conn, fixture_meta, source_frames, schema_prefix):
    config = load_config()
    results = ingest_all(
        spark, config, jdbc_reader(spark, sqlserver_conn, config), jdbc_source_label(sqlserver_conn, config),
        catalog=None, schema_prefix=schema_prefix, batch_id="jdbc-1",
    )
    assert {r.table: r.target_rows for r in results} == fixture_meta["row_counts"]
    for name, src in source_frames.items():
        bronze = spark.table(f"{schema_prefix}bronze.sqlserver_{name}").drop(*AUDIT_COLUMNS)
        assert bronze.dtypes == src.dtypes, name
        assert bronze.exceptAll(src).count() == 0 and src.exceptAll(bronze).count() == 0, name
    tx = spark.table(f"{schema_prefix}bronze.sqlserver_transaction_db")
    assert tx.select("_source").first()[0] == "sqlserver://sample/dbo.transaction_db"


def test_jdbc_incremental_pushes_watermark_filter(spark, sqlserver_conn, source_frames, schema_prefix):
    config = load_config()
    table = config.table("transaction_db")
    target_schema = f"{schema_prefix}bronze"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    ingest_table(
        spark, table, lambda t: source_frames["transaction_db"].filter("transaction_id <= 7"),
        target_schema=target_schema, source="seed", batch_id="seed", ingested_at=__import__("datetime").datetime(2026, 1, 1),
    )
    df = jdbc_reader(spark, sqlserver_conn, config)(table).filter(F.col("transaction_id") > F.lit(7))
    assert "PushedFilters: [*IsNotNull(transaction_id), *GreaterThan(transaction_id,7)]" in (
        df._jdf.queryExecution().executedPlan().toString()
    )
    result = ingest_table(
        spark, table, jdbc_reader(spark, sqlserver_conn, config),
        target_schema=target_schema, source="jdbc", batch_id="jdbc-inc",
        ingested_at=__import__("datetime").datetime(2026, 1, 2),
    )
    assert (result.watermark_before, result.rows_written, result.target_rows) == (7, 3, 10)


def test_staged_extract_matches_jdbc_ingest(spark, sqlserver_conn, fixture_meta, schema_prefix, tmp_path):
    config = load_config()
    manifest = extract_to_parquet(spark, sqlserver_conn, config, str(tmp_path / "extract"))
    assert {t: v["row_count"] for t, v in manifest["tables"].items()} == fixture_meta["row_counts"]
    assert "password" not in str(manifest).lower()
    root = str(tmp_path / "extract")
    staged = ingest_all(
        spark, config, staged_reader(spark, root), staged_source_label(root, load_manifest(root), config),
        catalog=None, schema_prefix=schema_prefix, batch_id="staged",
    )
    assert verify_against_manifest(staged, manifest) == []
    jdbc_prefix = schema_prefix + "jdbc_"
    spark.sql(f"DROP SCHEMA IF EXISTS {jdbc_prefix}bronze CASCADE")
    ingest_all(
        spark, config, jdbc_reader(spark, sqlserver_conn, config), jdbc_source_label(sqlserver_conn, config),
        catalog=None, schema_prefix=jdbc_prefix, batch_id="jdbc",
    )
    for t in config.tables:
        a = spark.table(f"{schema_prefix}bronze.{t.bronze_name}").drop(*AUDIT_COLUMNS)
        b = spark.table(f"{jdbc_prefix}bronze.{t.bronze_name}").drop(*AUDIT_COLUMNS)
        assert a.dtypes == b.dtypes, t.name
        assert a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0, t.name
