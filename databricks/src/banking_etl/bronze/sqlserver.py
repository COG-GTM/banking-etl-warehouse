"""SQL Server source tables -> ``bronze.sqlserver_<table>`` full snapshots (ticket 2).

Two source modes, selected by ``Settings.source_mode``:

* ``jdbc``: Spark JDBC against SQL Server with the exact column lists of the Talend
  ``tMSSqlInput`` queries; credentials come from the ``Settings.secret_scope`` secret scope.
* ``fixture``: CSV exports of ``data_sources/sample.bak`` landed at
  ``<landing volume>/sample_db/<table>/`` and read with explicit source-typed schemas.

Both modes keep the source column names and types (``int`` -> INT, ``varchar`` -> STRING,
``datetime2`` -> TIMESTAMP; ``customer.age`` stays a STRING in bronze), add ``_ingested_at``
and ``_source``, and overwrite the bronze table (Delta time travel keeps prior snapshots),
mirroring Talend's full extracts.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass, field
from typing import Iterable, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, IntegerType, StringType, StructField, StructType, TimestampType

from banking_etl.config import BRONZE_SQLSERVER, SQLSERVER_SOURCE_TABLES, Settings

SOURCE_MODES = ("jdbc", "fixture")
SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# Talend tMSSqlInput PROPERTIES used trustServerCertificate=true; keep TLS on.
DEFAULT_JDBC_PROPERTIES = "encrypt=true;trustServerCertificate=true"
FIXTURE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

_SPARK_TYPES: dict[str, DataType] = {"int": IntegerType(), "varchar": StringType(), "datetime2": TimestampType()}


@dataclass(frozen=True)
class SourceTable:
    """A ``dbo`` table in the ``sample`` database as read by the Talend jobs."""

    name: str
    columns: tuple[tuple[str, str], ...]  # (column, SQL Server type)
    partition_column: str  # integer primary key, used for JDBC partitioning hints
    talend_job: str

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    @property
    def schema(self) -> StructType:
        return StructType([StructField(c, spark_type(t), True) for c, t in self.columns])

    @property
    def query(self) -> str:
        """The Talend ``tMSSqlInput`` QUERY (same column list and order)."""
        cols = ",\n\t\t".join(f"dbo.{self.name}.{c}" for c in self.column_names)
        return f"SELECT {cols}\nFROM\tdbo.{self.name}"


def spark_type(sql_type: str) -> DataType:
    base = sql_type.split("(", 1)[0].strip().lower()
    if base not in _SPARK_TYPES:
        raise ValueError(f"unsupported SQL Server type {sql_type!r}")
    return _SPARK_TYPES[base]


# Column lists/types: INFORMATION_SCHEMA.COLUMNS of sample.bak, identical to the Talend input metadata.
SOURCE_TABLES: dict[str, SourceTable] = {
    t.name: t
    for t in (
        SourceTable(
            "customer",
            (
                ("customer_id", "int"),
                ("customer_name", "varchar(50)"),
                ("address", "varchar(max)"),
                ("city_id", "int"),
                ("age", "varchar(3)"),
                ("gender", "varchar(10)"),
                ("email", "varchar(50)"),
            ),
            "customer_id",
            "Load_DimCustomer",
        ),
        SourceTable("city", (("city_id", "int"), ("city_name", "varchar(50)"), ("state_id", "int")), "city_id", "Load_DimCustomer"),
        SourceTable("state", (("state_id", "int"), ("state_name", "varchar(50)")), "state_id", "Load_DimCustomer"),
        SourceTable(
            "account",
            (
                ("account_id", "int"),
                ("customer_id", "int"),
                ("account_type", "varchar(10)"),
                ("balance", "int"),
                ("date_opened", "datetime2"),
                ("status", "varchar(10)"),
            ),
            "account_id",
            "Load_DimAccount",
        ),
        SourceTable(
            "branch",
            (("branch_id", "int"), ("branch_name", "varchar(50)"), ("branch_location", "varchar(50)")),
            "branch_id",
            "Load_DimBranch",
        ),
        SourceTable(
            "transaction_db",
            (
                ("transaction_id", "int"),
                ("account_id", "int"),
                ("transaction_date", "datetime2"),
                ("amount", "int"),
                ("transaction_type", "varchar(50)"),
                ("branch_id", "int"),
            ),
            "transaction_id",
            "Load_FactTransaction",
        ),
    )
}

def parse_tables(value: str | Iterable[str] | None) -> tuple[str, ...]:
    """``"customer, city"`` / list / None (= all six) -> validated, de-duplicated tuple in input order."""
    if value is None:
        return SQLSERVER_SOURCE_TABLES
    items = value.split(",") if isinstance(value, str) else list(value)
    names = tuple(dict.fromkeys(i.strip().lower() for i in items if i and i.strip()))
    if not names or names == ("all",):
        return SQLSERVER_SOURCE_TABLES
    unknown = [n for n in names if n not in SOURCE_TABLES]
    if unknown:
        raise ValueError(f"unknown source table(s) {unknown}; expected any of {SQLSERVER_SOURCE_TABLES}")
    return names


def bronze_table(settings: Settings, table: str) -> str:
    return settings.table("bronze", BRONZE_SQLSERVER[table])


# --------------------------------------------------------------------------- JDBC


@dataclass(frozen=True)
class JdbcConnection:
    host: str
    port: str
    database: str
    user: str
    password: str = field(repr=False)
    properties: str = DEFAULT_JDBC_PROPERTIES

    @property
    def url(self) -> str:
        props = f";{self.properties.strip(';')}" if self.properties else ""
        return f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database}{props}"

    def options(self) -> dict[str, str]:
        # Credentials go in options, never in the URL (which Spark may log).
        return {"url": self.url, "user": self.user, "password": self.password, "driver": SQLSERVER_DRIVER}

    @classmethod
    def from_secrets(cls, dbutils, scope: str, properties: str = DEFAULT_JDBC_PROPERTIES) -> "JdbcConnection":
        get = lambda key: dbutils.secrets.get(scope=scope, key=key)  # noqa: E731
        return cls(get("jdbc-host"), get("jdbc-port"), get("jdbc-database"), get("jdbc-user"), get("jdbc-password"), properties)

    @classmethod
    def from_env(cls, env: Mapping[str, str], properties: str = DEFAULT_JDBC_PROPERTIES) -> "JdbcConnection":
        """Uses the ``BANKING_ETL_JDBC_*`` variables of ``banking_etl.setup.secrets``."""
        from banking_etl.setup.secrets import SECRET_KEYS, env_var_for

        missing = [env_var_for(k) for k in SECRET_KEYS if not env.get(env_var_for(k))]
        if missing:
            raise ValueError(f"missing env vars: {', '.join(missing)}")
        v = {k: env[env_var_for(k)] for k in SECRET_KEYS}
        return cls(v["jdbc-host"], v["jdbc-port"], v["jdbc-database"], v["jdbc-user"], v["jdbc-password"], properties)

    def reachable(self, timeout: float = 2.0) -> bool:
        try:
            with socket.create_connection((self.host, int(self.port)), timeout=timeout):
                return True
        except (OSError, ValueError):
            return False


@dataclass(frozen=True)
class JdbcPartitioning:
    column: str
    lower_bound: int
    upper_bound: int
    num_partitions: int

    def options(self) -> dict[str, str]:
        return {
            "partitionColumn": self.column,
            "lowerBound": str(self.lower_bound),
            "upperBound": str(self.upper_bound),
            "numPartitions": str(self.num_partitions),
        }


def _jdbc_reader(spark: SparkSession, conn: JdbcConnection, fetchsize: int):
    return spark.read.format("jdbc").options(**conn.options()).option("fetchsize", str(fetchsize))


def partition_hint(
    spark: SparkSession, conn: JdbcConnection, table: str, num_partitions: int, fetchsize: int = 10000
) -> JdbcPartitioning | None:
    """Bounds on the table's integer key for ``num_partitions`` > 1 (None for a single-partition read)."""
    if num_partitions <= 1:
        return None
    src = SOURCE_TABLES[table]
    col = src.partition_column
    bounds_sql = f"(SELECT MIN({col}) AS lo, MAX({col}) AS hi FROM dbo.{src.name}) AS b"
    row = _jdbc_reader(spark, conn, fetchsize).option("dbtable", bounds_sql).load().first()
    if row is None or row["lo"] is None:
        return None
    return JdbcPartitioning(col, int(row["lo"]), int(row["hi"]), num_partitions)


def read_jdbc(
    spark: SparkSession,
    table: str,
    conn: JdbcConnection,
    partitioning: JdbcPartitioning | None = None,
    fetchsize: int = 10000,
) -> DataFrame:
    src = SOURCE_TABLES[table]
    # dbtable subquery (not the `query` option) so partitionColumn hints are allowed.
    reader = _jdbc_reader(spark, conn, fetchsize).option("dbtable", f"({src.query}) AS src")
    if partitioning is not None:
        reader = reader.options(**partitioning.options())
    return conform(_load_varchar_as_string(spark, reader), table)


CHAR_VARCHAR_AS_STRING = "spark.sql.legacy.charVarcharAsString"


def _load_varchar_as_string(spark: SparkSession, reader) -> DataFrame:
    """Resolve the JDBC schema with VARCHAR(n) mapped to plain STRING (bronze has no length checks).

    Spark otherwise tags the columns VARCHAR(n) via field metadata that aliases cannot strip and
    that Delta persists. The schema is resolved eagerly in ``load()``, so the conf is scoped to it.
    """
    previous = spark.conf.get(CHAR_VARCHAR_AS_STRING, None)
    try:
        spark.conf.set(CHAR_VARCHAR_AS_STRING, "true")
    except Exception:  # conf not settable on this compute: keep Spark's default typing
        return reader.load()
    try:
        return reader.load()
    finally:
        if previous is None:
            spark.conf.unset(CHAR_VARCHAR_AS_STRING)
        else:
            spark.conf.set(CHAR_VARCHAR_AS_STRING, previous)


# --------------------------------------------------------------------------- fixture


def fixture_path(root: str, table: str) -> str:
    return f"{root.rstrip('/')}/{table}/"


def read_fixture(spark: SparkSession, table: str, root: str) -> DataFrame:
    src = SOURCE_TABLES[table]
    df = (
        spark.read.format("csv")
        .schema(src.schema)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("timestampFormat", FIXTURE_TIMESTAMP_FORMAT)
        .option("pathGlobFilter", "*.csv")
        .load(fixture_path(root, table))
    )
    return conform(df, table)


# --------------------------------------------------------------------------- write


def conform(df: DataFrame, table: str) -> DataFrame:
    """Explicit source column list, cast to the source-typed schema."""
    src = SOURCE_TABLES[table]
    missing = [c for c in src.column_names if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: source is missing columns {missing}")
    actual = {f.name: f.dataType for f in df.schema.fields}
    # Cast only on type mismatch: identity casts over JDBC columns trip Spark 4.0's SimplifyCasts plan validation.
    return df.select(
        [
            F.col(f.name) if actual[f.name] == f.dataType else F.col(f.name).cast(f.dataType).alias(f.name)
            for f in src.schema.fields
        ]
    )


def with_audit_columns(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.lit(source))


def write_snapshot(df: DataFrame, target: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)


def source_label(settings: Settings, table: str, fixture_root: str | None = None) -> str:
    if settings.source_mode == "jdbc":
        return f"sqlserver:dbo.{table}"
    return f"fixture:{fixture_path(fixture_root or '', table)}"


def ingest_table(
    spark: SparkSession,
    settings: Settings,
    table: str,
    *,
    connection: JdbcConnection | None = None,
    fixture_root: str | None = None,
    num_partitions: int = 1,
) -> dict:
    """Extract one source table and overwrite ``bronze.sqlserver_<table>``."""
    mode = settings.source_mode
    if mode == "jdbc":
        if connection is None:
            raise ValueError("jdbc mode needs a JdbcConnection")
        df = read_jdbc(spark, table, connection, partition_hint(spark, connection, table, num_partitions))
    elif mode == "fixture":
        if fixture_root is None:
            raise ValueError("fixture mode needs fixture_root")
        df = read_fixture(spark, table, fixture_root)
    else:
        raise ValueError(f"unknown source_mode {mode!r}; expected one of {SOURCE_MODES}")
    target = bronze_table(settings, table)
    source = source_label(settings, table, fixture_root)
    write_snapshot(with_audit_columns(df, source), target)
    return {"table": table, "target": target, "source": source, "rows": spark.table(target).count()}


def ingest_sqlserver(
    spark: SparkSession,
    settings: Settings,
    tables: str | Iterable[str] | None = None,
    *,
    dbutils=None,
    connection: JdbcConnection | None = None,
    fixture_root: str | None = None,
    num_partitions: int = 1,
) -> list[dict]:
    """Ingest ``tables`` (default: all six) in ``settings.source_mode``.

    jdbc: ``connection`` or secrets from ``settings.secret_scope`` via ``dbutils``.
    fixture: ``fixture_root`` or ``Settings.landing_path("sample_db")``.
    """
    names = parse_tables(tables)
    if settings.source_mode == "jdbc" and connection is None:
        if dbutils is None:
            raise ValueError("jdbc mode needs dbutils (secret scope) or an explicit connection")
        connection = JdbcConnection.from_secrets(dbutils, settings.secret_scope)
    if settings.source_mode == "fixture" and fixture_root is None:
        fixture_root = settings.landing_path("sample_db")
    return [
        ingest_table(spark, settings, t, connection=connection, fixture_root=fixture_root, num_partitions=num_partitions)
        for t in names
    ]
