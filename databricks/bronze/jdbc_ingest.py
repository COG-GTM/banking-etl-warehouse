"""Bronze ingestion for the SQL Server ``sample`` database.

Replaces the ``tMSSqlInput`` components of the four Talend jobs. Query and
option construction are pure functions so they can be unit-tested without a
live server; the single ``spark.read.format("jdbc")`` call is isolated in
:func:`read_jdbc`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .config import JdbcSource
from .metadata import with_table_metadata

DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
DEFAULT_PORT = 1433
DEFAULT_FETCHSIZE = 10_000


@dataclass(frozen=True)
class JdbcConnection:
    """Connection coordinates. Credentials are resolved from a secret scope."""

    host: str
    database: str = "sample"
    port: int = DEFAULT_PORT
    secret_scope: str = "banking-etl"
    user_key: str = "sqlserver-user"
    password_key: str = "sqlserver-password"
    encrypt: bool = True
    trust_server_certificate: bool = False

    @property
    def url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database};"
            f"encrypt={'true' if self.encrypt else 'false'};"
            f"trustServerCertificate={'true' if self.trust_server_certificate else 'false'}"
        )


SecretResolver = Callable[[str, str], str]


def dbutils_secret_resolver(dbutils) -> SecretResolver:  # pragma: no cover - Databricks only
    """Adapter around ``dbutils.secrets.get``; never accepts literal credentials."""

    def resolve(scope: str, key: str) -> str:
        return dbutils.secrets.get(scope=scope, key=key)

    return resolve


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_source_query(
    source: JdbcSource,
    *,
    watermark_value: str | None = None,
) -> str:
    """The ``SELECT`` pushed to SQL Server, mirroring the Talend job query.

    Every column is cast to ``NVARCHAR`` so bronze stores the raw source text
    and the value never depends on JDBC type coercion. When
    ``watermark_value`` is supplied the read is incremental on the source's
    watermark column; otherwise it is a full refresh.
    """
    columns = ",\n       ".join(
        f"CAST({source.qualified_source}.{column} AS NVARCHAR(4000)) AS {column}"
        for column in source.columns
    )
    query = f"SELECT {columns}\nFROM   {source.qualified_source}"
    if watermark_value is not None:
        if not source.watermark_column:
            raise ValueError(f"source {source.name!r} has no watermark column")
        query += (
            f"\nWHERE  {source.qualified_source}.{source.watermark_column}"
            f" > {_quote(watermark_value)}"
        )
    return query


def build_bounds_query(source: JdbcSource, *, watermark_value: str | None = None) -> str:
    """``MIN``/``MAX`` of the partition column, used to plan partitioned reads."""
    if not source.partition_column:
        raise ValueError(f"source {source.name!r} has no partition column")
    column = f"{source.qualified_source}.{source.partition_column}"
    query = f"SELECT MIN({column}) AS lower_bound, MAX({column}) AS upper_bound\nFROM   {source.qualified_source}"
    if watermark_value is not None:
        if not source.watermark_column:
            raise ValueError(f"source {source.name!r} has no watermark column")
        query += (
            f"\nWHERE  {source.qualified_source}.{source.watermark_column}"
            f" > {_quote(watermark_value)}"
        )
    return query


def build_jdbc_options(
    connection: JdbcConnection,
    source: JdbcSource,
    secret_resolver: SecretResolver,
    *,
    watermark_value: str | None = None,
    num_partitions: int | None = None,
    lower_bound: int | None = None,
    upper_bound: int | None = None,
    fetchsize: int = DEFAULT_FETCHSIZE,
) -> dict[str, str]:
    """Build the JDBC reader options for one bronze source.

    Partitioned reads are enabled only when the source exposes a numeric key
    and concrete bounds are supplied; partial partitioning arguments are
    rejected rather than silently ignored.
    """
    options = {
        "url": connection.url,
        "driver": DRIVER,
        "user": secret_resolver(connection.secret_scope, connection.user_key),
        "password": secret_resolver(connection.secret_scope, connection.password_key),
        "dbtable": f"({build_source_query(source, watermark_value=watermark_value)}) AS src",
        "fetchsize": str(fetchsize),
    }

    partition_args = (num_partitions, lower_bound, upper_bound)
    if any(arg is not None for arg in partition_args):
        if not all(arg is not None for arg in partition_args):
            raise ValueError(
                "num_partitions, lower_bound and upper_bound must be supplied together"
            )
        if not source.partition_column:
            raise ValueError(f"source {source.name!r} has no partition column")
        options.update(
            {
                "partitionColumn": source.partition_column,
                "numPartitions": str(num_partitions),
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
            }
        )
    return options


def read_jdbc(spark: SparkSession, options: dict[str, str]) -> DataFrame:  # pragma: no cover - needs a server
    """The single JDBC entry point; substituted by a local reader in tests."""
    return spark.read.format("jdbc").options(**options).load()


def ingest_jdbc_source(
    spark: SparkSession,
    source: JdbcSource,
    options: dict[str, str],
    batch_id: str,
    reader: Callable[[SparkSession, dict[str, str]], DataFrame] = read_jdbc,
) -> DataFrame:
    """Read one source table and attach the bronze metadata columns."""
    df = reader(spark, options)
    return with_table_metadata(
        df.select(*source.columns), batch_id, source_table=source.qualified_source
    )
