"""Unit tests for JDBC query/option construction (no live SQL Server)."""

from __future__ import annotations

import pytest
from bronze.config import (
    BATCH_ID_COLUMN,
    INGEST_TS_COLUMN,
    JDBC_SOURCES,
    SOURCE_TABLE_COLUMN,
)
from bronze.jdbc_ingest import (
    DRIVER,
    JdbcConnection,
    build_bounds_query,
    build_jdbc_options,
    build_source_query,
    ingest_jdbc_source,
)
from bronze.metadata import new_batch_id

CONNECTION = JdbcConnection(host="sql-src.internal", database="sample")
SECRETS = {("banking-etl", "sqlserver-user"): "etl_reader", ("banking-etl", "sqlserver-password"): "s3cret"}


def resolver(scope: str, key: str) -> str:
    return SECRETS[(scope, key)]


def test_source_tables_match_the_talend_jobs():
    assert set(JDBC_SOURCES) == {
        "transaction_db",
        "account",
        "customer",
        "city",
        "state",
        "branch",
    }
    assert JDBC_SOURCES["customer"].columns == [
        "customer_id",
        "customer_name",
        "address",
        "city_id",
        "age",
        "gender",
        "email",
    ]
    assert JDBC_SOURCES["transaction_db"].target_table == "sqlserver_transaction_db"


def test_full_refresh_query_selects_every_column_as_text():
    query = build_source_query(JDBC_SOURCES["branch"])

    assert "FROM   dbo.branch" in query
    assert "WHERE" not in query
    for column in JDBC_SOURCES["branch"].columns:
        assert f"CAST(dbo.branch.{column} AS NVARCHAR(4000)) AS {column}" in query


def test_incremental_query_filters_on_watermark():
    query = build_source_query(
        JDBC_SOURCES["transaction_db"], watermark_value="2024-01-20 00:00:00"
    )

    assert "WHERE  dbo.transaction_db.transaction_date > '2024-01-20 00:00:00'" in query


def test_incremental_query_rejects_sources_without_watermark():
    with pytest.raises(ValueError, match="watermark"):
        build_source_query(JDBC_SOURCES["city"], watermark_value="2024-01-01")


def test_bounds_query_uses_the_partition_key():
    query = build_bounds_query(JDBC_SOURCES["account"])

    assert "MIN(dbo.account.account_id) AS lower_bound" in query
    assert "MAX(dbo.account.account_id) AS upper_bound" in query


def test_options_use_secrets_and_never_inline_credentials():
    options = build_jdbc_options(CONNECTION, JDBC_SOURCES["account"], resolver)

    assert options["url"] == (
        "jdbc:sqlserver://sql-src.internal:1433;databaseName=sample;"
        "encrypt=true;trustServerCertificate=false"
    )
    assert options["driver"] == DRIVER
    assert options["user"] == "etl_reader"
    assert options["password"] == "s3cret"
    assert options["dbtable"].startswith("(SELECT ") and options["dbtable"].endswith(") AS src")
    assert "partitionColumn" not in options


def test_partitioned_read_options():
    options = build_jdbc_options(
        CONNECTION,
        JDBC_SOURCES["transaction_db"],
        resolver,
        num_partitions=4,
        lower_bound=1,
        upper_bound=25,
    )

    assert options["partitionColumn"] == "transaction_id"
    assert options["numPartitions"] == "4"
    assert options["lowerBound"] == "1"
    assert options["upperBound"] == "25"


def test_partial_partition_arguments_are_rejected():
    with pytest.raises(ValueError, match="must be supplied together"):
        build_jdbc_options(CONNECTION, JDBC_SOURCES["transaction_db"], resolver, num_partitions=4)


def test_ingest_jdbc_source_adds_metadata_with_a_local_reader(spark):
    source = JDBC_SOURCES["branch"]
    batch_id = new_batch_id()
    fake_rows = [("1", "Main", "Jakarta"), ("2", "North", "Bandung")]

    def local_reader(session, options):
        assert options["dbtable"].startswith("(SELECT ")
        return session.createDataFrame(fake_rows, schema=source.schema)

    options = build_jdbc_options(CONNECTION, source, resolver)
    df = ingest_jdbc_source(spark, source, options, batch_id, reader=local_reader)
    rows = df.collect()

    assert df.columns == source.columns + [
        INGEST_TS_COLUMN,
        SOURCE_TABLE_COLUMN,
        BATCH_ID_COLUMN,
    ]
    assert len(rows) == 2
    assert {row[SOURCE_TABLE_COLUMN] for row in rows} == {"dbo.branch"}
    assert {row[BATCH_ID_COLUMN] for row in rows} == {batch_id}
    assert all(row[INGEST_TS_COLUMN] is not None for row in rows)
