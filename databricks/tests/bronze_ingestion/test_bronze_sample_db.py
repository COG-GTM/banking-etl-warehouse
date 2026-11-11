"""sample.bak tables: registry vs Talend jobs, extract-file fallback, federation and live JDBC."""

import html
import os
import re
import zipfile
from pathlib import Path

import pytest
from banking_etl.ingestion.config import BronzeConfig
from banking_etl.ingestion.metadata import METADATA_COLUMNS
from banking_etl.ingestion.run import run_bronze_ingestion
from banking_etl.ingestion.sources import SAMPLE_DB_TABLES, SAMPLE_DB_TABLES_BY_NAME
from banking_etl.ingestion.sqlserver import (
    SqlServerConnection,
    federation_ddl,
    ingest_sample_db,
)

EXPECTED_ROW_COUNTS = {
    "account": 21,
    "branch": 5,
    "city": 52,
    "customer": 20,
    "state": 9,
    "transaction_db": 10,
}
SPARK_TYPES = {"INT": "int", "STRING": "string", "TIMESTAMP": "timestamp"}


def _talend_queries(repo_root: Path) -> dict[str, list[str]]:
    queries = {}
    for z in sorted((repo_root / "talend_jobs").glob("*.zip")):
        with zipfile.ZipFile(z) as zf:
            for name in zf.namelist():
                if not name.endswith(".item") or "/process/" not in name:
                    continue
                text = zf.read(name).decode("utf-8")
                for q in re.findall(r'name="QUERY"[^>]*value="([^"]*)"', text):
                    sql = html.unescape(q).strip('"')
                    m = re.search(r"FROM\s+dbo\.(\w+)", sql)
                    cols = re.findall(r"dbo\.\w+\.(\w+)", sql)
                    queries[m.group(1)] = cols
    return queries


def _land_extracts(cfg, data_sources):
    for table in SAMPLE_DB_TABLES:
        target = Path(cfg.landing_path(f"sample_db/{table.name}"))
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{table.name}.csv").write_bytes(
            (data_sources / "sample_db" / f"{table.name}.csv").read_bytes()
        )


def test_registry_matches_talend_source_queries(data_sources):
    queries = _talend_queries(data_sources.parent)
    assert set(queries) == set(SAMPLE_DB_TABLES_BY_NAME)
    for name, cols in queries.items():
        assert SAMPLE_DB_TABLES_BY_NAME[name].column_names == cols


def test_extract_files_match_registry(data_sources):
    for table in SAMPLE_DB_TABLES:
        lines = (data_sources / "sample_db" / f"{table.name}.csv").read_text().splitlines()
        assert lines[0].split(",") == table.column_names
        assert len(lines) - 1 == EXPECTED_ROW_COUNTS[table.name]


def test_select_sql_mirrors_talend_query():
    assert SAMPLE_DB_TABLES_BY_NAME["branch"].select_sql() == (
        "SELECT [branch_id], [branch_name], [branch_location] FROM [dbo].[branch]"
    )


def test_federation_ddl():
    conn_sql, catalog_sql = federation_ddl(
        "sample_sqlserver", "sample_db", "db.example.com", "banking_sample_db"
    )
    assert conn_sql.startswith("CREATE CONNECTION IF NOT EXISTS `sample_sqlserver` TYPE sqlserver OPTIONS (")
    assert "host 'db.example.com'" in conn_sql
    assert "user secret('banking_sample_db', 'user')" in conn_sql
    assert "password secret('banking_sample_db', 'password')" in conn_sql
    assert catalog_sql == (
        "CREATE FOREIGN CATALOG IF NOT EXISTS `sample_db` USING CONNECTION `sample_sqlserver` "
        "OPTIONS (database 'sample')"
    )


def test_jdbc_url():
    conn = SqlServerConnection(host="h", user="u", password="p", trust_server_certificate=True)
    assert (
        conn.jdbc_url
        == "jdbc:sqlserver://h:1433;databaseName=sample;encrypt=true;trustServerCertificate=true"
    )


def test_unknown_mode_rejected(spark, bronze_config):
    with pytest.raises(ValueError, match="unknown sample_db mode"):
        ingest_sample_db(spark, bronze_config, "odbc", "r")
    with pytest.raises(ValueError, match="needs a SqlServerConnection"):
        ingest_sample_db(spark, bronze_config, "jdbc", "r")


def test_extract_files_fallback_lands_every_table(spark, bronze_config, data_sources):
    _land_extracts(bronze_config, data_sources)
    summary = run_bronze_ingestion(
        spark,
        bronze_config,
        sources="sample_db",
        sample_db_mode="files",
        use_auto_loader=False,
        run_id="extract-1",
        create_objects=False,
    )
    assert set(summary["sources"]["sample_db"]) == set(EXPECTED_ROW_COUNTS)

    for table in SAMPLE_DB_TABLES:
        df = spark.table(bronze_config.table(table.name))
        assert df.columns == table.column_names + list(METADATA_COLUMNS)
        types = dict(df.dtypes)
        for col in table.columns:
            assert types[col.name] == SPARK_TYPES[col.spark_type]
        assert df.count() == EXPECTED_ROW_COUNTS[table.name]
        assert df.filter("_rescued_data IS NOT NULL").count() == 0
        assert (
            df.select("_source_system").distinct().collect()[0][0] == f"file_extract:sample.dbo.{table.name}"
        )
        assert df.filter("_source_file IS NULL OR _ingested_at IS NULL").count() == 0
        for pk in table.primary_key:
            assert df.filter(f"{pk} IS NULL").count() == 0

    acct = spark.table(bronze_config.table("account")).filter("account_id = 4").collect()[0]
    assert (acct.customer_id, acct.account_type, acct.balance, acct.status) == (
        3,
        "checking",
        4500000,
        "terminated",
    )
    assert str(acct.date_opened) == "2021-06-24 11:00:00"
    txn = spark.table(bronze_config.table("transaction_db"))
    assert txn.agg({"amount": "sum"}).collect()[0][0] == 19750000

    # Re-running with nothing new appends nothing; the checkpoint remembers the files.
    run_bronze_ingestion(
        spark,
        bronze_config,
        sources="sample_db",
        sample_db_mode="files",
        use_auto_loader=False,
        run_id="extract-2",
        create_objects=False,
    )
    assert spark.table(bronze_config.table("city")).count() == EXPECTED_ROW_COUNTS["city"]


def test_federation_path_snapshot_appends(spark, bronze_config, data_sources, tmp_path):
    """Stand in for the foreign catalog with local tables named <catalog>.dbo.<table>."""
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.dbo")
    try:
        for table in SAMPLE_DB_TABLES:
            (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(str(data_sources / "sample_db" / f"{table.name}.csv"))
                .write.mode("overwrite")
                .saveAsTable(f"spark_catalog.dbo.{table.name}")
            )
        for run in ("fed-1", "fed-2"):
            result = run_bronze_ingestion(
                spark,
                bronze_config,
                sources="sample_db",
                sample_db_mode="federation",
                foreign_catalog="spark_catalog",
                run_id=run,
                create_objects=False,
            )
            assert result["sources"]["sample_db"] == EXPECTED_ROW_COUNTS
        state = spark.table(bronze_config.table("state"))
        assert state.count() == 2 * EXPECTED_ROW_COUNTS["state"]
        assert {r[0] for r in state.select("_ingest_run_id").distinct().collect()} == {"fed-1", "fed-2"}
        assert (
            state.select("_source_system").distinct().collect()[0][0]
            == "lakehouse_federation:spark_catalog.dbo.state"
        )
        assert state.filter("_source_file IS NOT NULL").count() == 0
    finally:
        spark.sql("DROP DATABASE IF EXISTS spark_catalog.dbo CASCADE")


@pytest.mark.skipif(
    not os.environ.get("SAMPLE_DB_JDBC_URL"), reason="needs a restored sample.bak (SAMPLE_DB_JDBC_URL)"
)
def test_live_jdbc_matches_extract_fallback(spark, data_sources, tmp_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "extract_sample_bak", data_sources.parent / "databricks" / "scripts" / "extract_sample_bak.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    conn = mod.connection_from_url(
        os.environ["SAMPLE_DB_JDBC_URL"], os.environ["SAMPLE_DB_USER"], os.environ["SAMPLE_DB_PASSWORD"]
    )

    schema = "bronze_jdbc_parity"
    spark.sql(f"DROP DATABASE IF EXISTS spark_catalog.{schema} CASCADE")
    spark.sql(f"CREATE DATABASE spark_catalog.{schema}")
    jdbc_cfg = BronzeConfig("spark_catalog", schema, str(tmp_path / "l1"), str(tmp_path / "c1"))
    files_schema = "bronze_files_parity"
    spark.sql(f"DROP DATABASE IF EXISTS spark_catalog.{files_schema} CASCADE")
    spark.sql(f"CREATE DATABASE spark_catalog.{files_schema}")
    files_cfg = BronzeConfig("spark_catalog", files_schema, str(tmp_path / "l2"), str(tmp_path / "c2"))
    try:
        result = run_bronze_ingestion(
            spark,
            jdbc_cfg,
            sources="sample_db",
            sample_db_mode="jdbc",
            conn=conn,
            run_id="jdbc-1",
            create_objects=False,
        )
        assert result["sources"]["sample_db"] == EXPECTED_ROW_COUNTS

        _land_extracts(files_cfg, data_sources)
        run_bronze_ingestion(
            spark,
            files_cfg,
            sources="sample_db",
            sample_db_mode="files",
            use_auto_loader=False,
            run_id="files-1",
            create_objects=False,
        )
        for table in SAMPLE_DB_TABLES:
            live = spark.table(jdbc_cfg.table(table.name))
            fallback = spark.table(files_cfg.table(table.name))
            assert live.dtypes == fallback.dtypes
            assert (
                live.filter(f"_source_system = 'sqlserver:sample.dbo.{table.name}'").count() == live.count()
            )
            cols = table.column_names
            assert live.select(cols).exceptAll(fallback.select(cols)).count() == 0
            assert fallback.select(cols).exceptAll(live.select(cols)).count() == 0
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS spark_catalog.{schema} CASCADE")
        spark.sql(f"DROP DATABASE IF EXISTS spark_catalog.{files_schema} CASCADE")
