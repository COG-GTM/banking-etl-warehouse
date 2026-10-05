"""JDBC mode against a real SQL Server (Docker, sample.bak restored). Env-gated.

  docker run -d --name mssql -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=... -p 1433:1433 \
      -v $PWD/../data_sources:/backup:ro mcr.microsoft.com/mssql/server:2022-latest
  # RESTORE DATABASE sample FROM DISK='/backup/sample.bak' WITH MOVE 'sample' TO ..., MOVE 'sample_log' TO ...
  BANKING_ETL_JDBC_HOST=localhost BANKING_ETL_JDBC_PORT=1433 BANKING_ETL_JDBC_DATABASE=sample \
  BANKING_ETL_JDBC_USER=sa BANKING_ETL_JDBC_PASSWORD=... \
  SPARK_EXTRA_PACKAGES=com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 pytest tests/bronze
"""
import os
from dataclasses import replace

import pytest

from banking_etl.bronze.sqlserver import JdbcConnection, ingest_sqlserver, partition_hint, read_jdbc, read_fixture
from banking_etl.bronze.fixtures import stage_fixtures

EXPECTED_ROWS = {"customer": 20, "city": 52, "state": 9, "account": 21, "branch": 5, "transaction_db": 10}


def _connection():
    try:
        conn = JdbcConnection.from_env(os.environ)
    except ValueError:
        pytest.skip("BANKING_ETL_JDBC_* not set; no SQL Server to test against")
    if not conn.reachable():
        pytest.skip(f"SQL Server {conn.host}:{conn.port} not reachable")
    if "mssql-jdbc" not in os.environ.get("SPARK_EXTRA_PACKAGES", ""):
        pytest.skip("SPARK_EXTRA_PACKAGES must include com.microsoft.sqlserver:mssql-jdbc")
    return conn


@pytest.fixture(scope="module")
def conn():
    return _connection()


@pytest.fixture(scope="module")
def jdbc_settings():
    from banking_etl.config import Settings

    return Settings(catalog=None, schema_prefix="jdbc_", source_mode="jdbc")


@pytest.fixture(scope="module")
def ingested(spark, jdbc_settings, conn):
    spark.sql("CREATE DATABASE IF NOT EXISTS jdbc_bronze")
    return {r["table"]: r for r in ingest_sqlserver(spark, jdbc_settings, connection=conn)}


def test_jdbc_row_counts(ingested):
    assert {t: r["rows"] for t, r in ingested.items()} == EXPECTED_ROWS
    assert all(r["source"] == f"sqlserver:dbo.{t}" for t, r in ingested.items())


def test_jdbc_matches_fixture_exports(spark, ingested, tmp_path):
    """The committed CSV fixtures are exact exports of the SQL Server tables."""
    stage_fixtures(tmp_path)
    for table, r in ingested.items():
        jdbc = spark.table(r["target"]).drop("_ingested_at", "_source")
        fixture = read_fixture(spark, table, str(tmp_path))
        assert [(f.name, f.dataType) for f in jdbc.schema.fields] == [(f.name, f.dataType) for f in fixture.schema.fields], table
        assert not any("__CHAR_VARCHAR_TYPE_STRING" in f.metadata for f in jdbc.schema.fields), table  # STRING, not VARCHAR(n)
        assert spark.sql(f"DESCRIBE {r['target']}").where("data_type LIKE 'varchar%'").count() == 0, table
        assert jdbc.exceptAll(fixture).count() == 0 and fixture.exceptAll(jdbc).count() == 0, table


def test_jdbc_partitioned_read(spark, conn):
    hint = partition_hint(spark, conn, "customer", 4)
    assert (hint.column, hint.lower_bound, hint.upper_bound, hint.num_partitions) == ("customer_id", 1, 20, 4)
    df = read_jdbc(spark, "customer", conn, hint)
    assert df.rdd.getNumPartitions() == 4
    assert df.count() == 20


def test_jdbc_bad_credentials_fail(spark, conn):
    with pytest.raises(Exception, match="(?i)login failed"):
        read_jdbc(spark, "branch", replace(conn, password="wrong")).count()
