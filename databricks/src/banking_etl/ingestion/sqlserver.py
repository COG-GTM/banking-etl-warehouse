"""Ingestion of the sample.bak SQL Server tables.

Three interchangeable read paths produce the same bronze shape:

* ``jdbc``       - live SQL Server over JDBC (the Talend ``tMSSqlInput`` query).
* ``federation`` - live SQL Server through a Unity Catalog foreign catalog
                   (Lakehouse Federation); see ``federation_ddl``.
* ``files``      - fallback when the database is not reachable: CSV extracts
                   (``extract_tables_to_landing`` or ``scripts/extract_sample_bak.py``)
                   landed under ``<landing>/sample_db/<table>/`` and consumed
                   incrementally like the other file drops.

Live reads (``jdbc``/``federation``) append one full snapshot per run, tagged
with ``_ingest_run_id``; the ``files`` path appends each newly landed extract
file.  Bronze is therefore append-only history for every path, and consumers
select the latest ``_ingest_run_id`` / ``_source_file`` they need.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import BronzeConfig, quote_identifier
from .files import read_csv_as_strings, to_bronze, write_stream_append
from .metadata import add_ingestion_metadata, apply_schema_with_rescue
from .sources import SAMPLE_DB_LANDING_DIR, SAMPLE_DB_TABLES, SourceTable

SQLSERVER_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


@dataclass(frozen=True)
class SqlServerConnection:
    host: str
    user: str
    password: str
    port: int = 1433
    database: str = "sample"
    encrypt: bool = True
    trust_server_certificate: bool = False

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database};"
            f"encrypt={str(self.encrypt).lower()};"
            f"trustServerCertificate={str(self.trust_server_certificate).lower()}"
        )


def read_table_jdbc(spark: SparkSession, conn: SqlServerConnection, table: SourceTable) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", conn.jdbc_url)
        .option("driver", SQLSERVER_JDBC_DRIVER)
        .option("user", conn.user)
        .option("password", conn.password)
        .option("query", table.select_sql())
        .load()
    )


def read_table_federated(spark: SparkSession, foreign_catalog: str, table: SourceTable) -> DataFrame:
    fqn = ".".join(quote_identifier(p) for p in (foreign_catalog, table.source_schema, table.name))
    return spark.table(fqn).select(*table.column_names)


def federation_ddl(
    connection_name: str,
    foreign_catalog: str,
    host: str,
    secret_scope: str,
    user_key: str = "user",
    password_key: str = "password",
    port: int = 1433,
    database: str = "sample",
    trust_server_certificate: bool = False,
) -> list[str]:
    """SQL to register the SQL Server as a Lakehouse Federation foreign catalog."""

    def lit(v: str) -> str:
        return "'" + v.replace("'", "''") + "'"

    return [
        (
            f"CREATE CONNECTION IF NOT EXISTS {quote_identifier(connection_name)} TYPE sqlserver OPTIONS ("
            f"host {lit(host)}, port {lit(str(port))}, "
            f"user secret({lit(secret_scope)}, {lit(user_key)}), "
            f"password secret({lit(secret_scope)}, {lit(password_key)}), "
            f"trustServerCertificate {lit(str(trust_server_certificate).lower())})"
        ),
        (
            f"CREATE FOREIGN CATALOG IF NOT EXISTS {quote_identifier(foreign_catalog)} "
            f"USING CONNECTION {quote_identifier(connection_name)} OPTIONS (database {lit(database)})"
        ),
    ]


def source_system(path: str, table: SourceTable, foreign_catalog: str | None = None) -> str:
    if path == "jdbc":
        return f"sqlserver:{table.qualified_source_name}"
    if path == "federation":
        return f"lakehouse_federation:{foreign_catalog}.{table.source_schema}.{table.name}"
    if path == "files":
        return f"file_extract:{table.qualified_source_name}"
    raise ValueError(f"unknown sample_db path: {path}")


def snapshot_to_bronze(df: DataFrame, table: SourceTable, source_system_value: str, run_id: str) -> DataFrame:
    """Conform a live snapshot (any column types) to the bronze shape.

    Values go through STRING and the same typed-with-rescue step as the file
    paths, so every path yields identical columns and types.
    """
    as_strings = df.select(*[F.col(c).cast("string").alias(c) for c in table.column_names])
    typed = apply_schema_with_rescue(as_strings, table.columns)
    return add_ingestion_metadata(typed, source_system=source_system_value, run_id=run_id)


def ingest_snapshot(
    spark: SparkSession,
    config: BronzeConfig,
    table: SourceTable,
    df: DataFrame,
    source_system_value: str,
    run_id: str,
) -> int:
    bronze = snapshot_to_bronze(df, table, source_system_value, run_id).cache()
    try:
        rows = bronze.count()
        bronze.write.format("delta").mode("append").saveAsTable(config.table(table.name))
        return rows
    finally:
        bronze.unpersist()


def extract_bronze_frame(
    spark: SparkSession,
    config: BronzeConfig,
    table: SourceTable,
    run_id: str,
    *,
    use_auto_loader: bool = True,
) -> DataFrame:
    raw = read_csv_as_strings(
        spark,
        config.landing_path(f"{SAMPLE_DB_LANDING_DIR}/{table.name}"),
        table.column_names,
        streaming=True,
        use_auto_loader=use_auto_loader,
        encoding="UTF-8",
    )
    return to_bronze(raw, table.columns, source_system("files", table), run_id)


def ingest_extract_files(
    spark: SparkSession,
    config: BronzeConfig,
    table: SourceTable,
    run_id: str,
    *,
    use_auto_loader: bool = True,
) -> None:
    df = extract_bronze_frame(spark, config, table, run_id, use_auto_loader=use_auto_loader)
    write_stream_append(df, config.table(table.name), config.checkpoint_path(table.name))


def extract_tables_to_landing(
    spark: SparkSession,
    conn: SqlServerConnection,
    config: BronzeConfig,
    run_id: str,
    tables: Iterable[SourceTable] = SAMPLE_DB_TABLES,
) -> dict[str, str]:
    """Write each table as header CSV (all STRING) to ``<landing>/sample_db/<table>/extract_<run_id>/``."""
    written = {}
    for table in tables:
        df = read_table_jdbc(spark, conn, table)
        target = config.landing_path(f"{SAMPLE_DB_LANDING_DIR}/{table.name}/extract_{run_id}")
        (
            df.select(*[F.col(c).cast("string").alias(c) for c in table.column_names])
            .coalesce(1)
            .write.mode("errorifexists")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .csv(target)
        )
        written[table.name] = target
    return written


def ingest_sample_db(
    spark: SparkSession,
    config: BronzeConfig,
    mode: str,
    run_id: str,
    *,
    tables: Sequence[SourceTable] = SAMPLE_DB_TABLES,
    conn: SqlServerConnection | None = None,
    foreign_catalog: str | None = None,
    use_auto_loader: bool = True,
) -> dict[str, int | None]:
    """Load the sample.bak tables into ``<catalog>.<bronze>.<table>`` via ``mode``.

    Returns rows appended per table for live modes (``None`` for ``files``,
    where the stream decides what is new).
    """
    results: dict[str, int | None] = {}
    for table in tables:
        if mode == "jdbc":
            if conn is None:
                raise ValueError("mode='jdbc' needs a SqlServerConnection")
            df = read_table_jdbc(spark, conn, table)
            results[table.name] = ingest_snapshot(
                spark, config, table, df, source_system("jdbc", table), run_id
            )
        elif mode == "federation":
            if not foreign_catalog:
                raise ValueError("mode='federation' needs foreign_catalog")
            df = read_table_federated(spark, foreign_catalog, table)
            results[table.name] = ingest_snapshot(
                spark, config, table, df, source_system("federation", table, foreign_catalog), run_id
            )
        elif mode == "files":
            ingest_extract_files(spark, config, table, run_id, use_auto_loader=use_auto_loader)
            results[table.name] = None
        else:
            raise ValueError(f"unknown sample_db mode: {mode!r} (expected jdbc, federation or files)")
    return results
