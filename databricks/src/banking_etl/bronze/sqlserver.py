"""Config-driven ingestion of the legacy SQL Server ``sample`` DB into bronze Delta tables.

Replaces the ``tMSSqlInput`` side of the Talend jobs. Every table listed in
``sources.yml`` lands verbatim (source column names and JDBC types) in
``<catalog>.<prefix>bronze.sqlserver_<table>`` with three audit columns:

* ``_ingested_at`` - batch start timestamp (UTC), identical for every table in a batch
* ``_source``      - ``sqlserver://<database>/<schema>.<table>`` (``@staged:<path>`` suffix
  when the rows came from a staged parquet extract instead of a live JDBC read)
* ``_batch_id``    - id shared by every table written in one run

Two readers feed the same writer (:func:`ingest_table`):

* :func:`read_jdbc` - live JDBC read of the source table.
* :func:`read_staged` - parquet produced by :func:`extract_to_parquet` (used when the
  Databricks compute cannot reach the SQL Server; the extract is uploaded to the
  landing volume and replayed through the identical bronze write path).

Run locally as ``python -m banking_etl.bronze.sqlserver {discover,extract,ingest} ...``.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_CONFIG_PATH = Path(__file__).with_name("sources.yml")
MODES = ("full", "incremental")
AUDIT_COLUMNS = ("_ingested_at", "_source", "_batch_id")
MANIFEST_FILE = "_manifest.json"
MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


# --------------------------------------------------------------------------- config


@dataclass(frozen=True)
class TableConfig:
    name: str
    primary_key: List[str]
    mode: str = "full"
    watermark_column: Optional[str] = None
    partition_column: Optional[str] = None
    num_partitions: int = 1

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"{self.name}: mode must be one of {MODES}, got {self.mode!r}")
        if self.mode == "incremental" and not self.watermark_column:
            raise ValueError(f"{self.name}: incremental mode requires watermark_column")

    @property
    def bronze_name(self) -> str:
        return f"sqlserver_{self.name}"


@dataclass(frozen=True)
class SourcesConfig:
    tables: List[TableConfig]
    connection: Dict[str, Any] = field(default_factory=dict)
    source_system: str = "sqlserver"
    source_schema: str = "dbo"
    exclude_tables: List[str] = field(default_factory=list)

    def table(self, name: str) -> TableConfig:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(f"table {name!r} not in sources config")

    def select(self, names: Optional[Sequence[str]]) -> List[TableConfig]:
        if not names:
            return list(self.tables)
        return [self.table(n) for n in names]


def load_config(path: Optional[os.PathLike] = None) -> SourcesConfig:
    with open(path or DEFAULT_CONFIG_PATH, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    tables = [
        TableConfig(
            name=t["name"],
            primary_key=list(t.get("primary_key") or []),
            mode=t.get("mode", "full"),
            watermark_column=t.get("watermark_column"),
            partition_column=t.get("partition_column"),
            num_partitions=int(t.get("num_partitions", 1)),
        )
        for t in raw["tables"]
    ]
    names = [t.name for t in tables]
    if len(names) != len(set(names)):
        raise ValueError(f"duplicate table names in sources config: {names}")
    return SourcesConfig(
        tables=tables,
        connection=dict(raw.get("connection") or {}),
        source_system=raw.get("source_system", "sqlserver"),
        source_schema=raw.get("source_schema", "dbo"),
        exclude_tables=list(raw.get("exclude_tables") or []),
    )


@dataclass(frozen=True)
class Connection:
    host: str
    port: str
    database: str
    user: Optional[str]
    password: Optional[str] = field(default=None, repr=False)
    jdbc_options: Dict[str, str] = field(default_factory=dict)
    fetchsize: int = 10000

    @property
    def url(self) -> str:
        opts = "".join(f";{k}={v}" for k, v in self.jdbc_options.items())
        return f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database}{opts}"

    def reader_options(self) -> Dict[str, str]:
        opts = {"url": self.url, "driver": MSSQL_DRIVER, "fetchsize": str(self.fetchsize)}
        if self.user is not None:
            opts["user"] = self.user
        if self.password is not None:
            opts["password"] = self.password
        return opts


_MISSING_SECRET_MARKERS = ("does not exist", "not found", "RESOURCE_DOES_NOT_EXIST", "NOT_FOUND")


def _is_missing_secret(exc: BaseException) -> bool:
    msg = str(exc)
    return isinstance(exc, KeyError) or any(m in msg for m in _MISSING_SECRET_MARKERS)


def resolve_connection(
    config: SourcesConfig,
    dbutils: Any = None,
    env: Optional[Mapping[str, str]] = None,
    overrides: Optional[Mapping[str, Optional[str]]] = None,
) -> Connection:
    """Resolve each field: override > secret scope (when ``dbutils`` given) > env > default.

    Only a missing scope/key falls through to env/default; any other secret error fails the run.
    """
    env = os.environ if env is None else env
    overrides = overrides or {}
    conn = config.connection
    scope = conn.get("secret_scope")

    def field_value(name: str) -> Optional[str]:
        if overrides.get(name):
            return overrides[name]
        spec = conn.get(name) or {}
        if dbutils is not None and scope and spec.get("secret"):
            try:
                return dbutils.secrets.get(scope=scope, key=spec["secret"])
            except Exception as exc:
                if not _is_missing_secret(exc):
                    raise RuntimeError(
                        f"reading secret {scope}/{spec['secret']} failed: {type(exc).__name__}"
                    ) from None
        if spec.get("env") and env.get(spec["env"]):
            return env[spec["env"]]
        default = spec.get("default")
        return None if default is None else str(default)

    host, port, database = field_value("host"), field_value("port"), field_value("database")
    if not (host and port and database):
        raise ValueError("SQL Server host/port/database could not be resolved")
    jdbc_options = {k: str(v) for k, v in (conn.get("jdbc_options") or {}).items()}
    if conn.get("trust_server_certificate"):
        trust = (field_value("trust_server_certificate") or "false").strip().lower()
        if trust not in ("true", "false"):
            raise ValueError(f"trust_server_certificate must be true/false, got {trust!r}")
        jdbc_options["trustServerCertificate"] = trust
    return Connection(
        host=host,
        port=port,
        database=database,
        user=field_value("user"),
        password=field_value("password"),
        jdbc_options=jdbc_options,
        fetchsize=int(conn.get("fetchsize", 10000)),
    )


# --------------------------------------------------------------------------- readers

Reader = Callable[[TableConfig], DataFrame]


@contextmanager
def _sql_conf(spark: SparkSession, key: str, value: str) -> Iterator[None]:
    previous = spark.conf.get(key, None)
    spark.conf.set(key, value)
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)


def _jdbc_query(spark: SparkSession, conn: Connection, query: str) -> DataFrame:
    with _sql_conf(spark, "spark.sql.legacy.charVarcharAsString", "true"):
        return spark.read.format("jdbc").options(**conn.reader_options()).option("query", query).load()


def discover_tables(spark: SparkSession, conn: Connection, schema: str = "dbo") -> List[str]:
    """Base tables in ``schema`` of the source database, sorted by name."""
    df = _jdbc_query(
        spark,
        conn,
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema}'",
    )
    return sorted(r[0] for r in df.collect())


def check_coverage(config: SourcesConfig, discovered: Sequence[str]) -> Dict[str, List[str]]:
    """Compare configured tables with the source catalog (excluded tables are ignored)."""
    configured = {t.name for t in config.tables}
    found = set(discovered) - set(config.exclude_tables)
    return {
        "unconfigured": sorted(found - configured),
        "missing_in_source": sorted(configured - found),
    }


def assert_source_coverage(spark: SparkSession, conn: Connection, config: SourcesConfig) -> Dict[str, List[str]]:
    """Fail when the live source has tables that ``sources.yml`` does not configure."""
    coverage = check_coverage(config, discover_tables(spark, conn, config.source_schema))
    if coverage["unconfigured"]:
        raise RuntimeError(f"source tables missing from sources.yml: {coverage['unconfigured']}")
    return coverage


def read_jdbc(
    spark: SparkSession, conn: Connection, config: SourcesConfig, table: TableConfig
) -> DataFrame:
    """Live JDBC read of ``<schema>.<table>``; watermark filters added later are pushed down."""
    qualified = f"{config.source_schema}.{table.name}"
    reader = spark.read.format("jdbc").options(**conn.reader_options()).option("dbtable", qualified)
    if table.partition_column and table.num_partitions > 1:
        bounds = _jdbc_query(
            spark,
            conn,
            f"SELECT MIN({table.partition_column}) AS lo, MAX({table.partition_column}) AS hi "
            f"FROM {qualified}",
        ).first()
        if bounds is not None and bounds["lo"] is not None and bounds["hi"] > bounds["lo"]:
            reader = reader.options(
                partitionColumn=table.partition_column,
                lowerBound=str(bounds["lo"]),
                upperBound=str(bounds["hi"]),
                numPartitions=str(table.num_partitions),
            )
    with _sql_conf(spark, "spark.sql.legacy.charVarcharAsString", "true"):
        return reader.load()


def jdbc_reader(spark: SparkSession, conn: Connection, config: SourcesConfig) -> Reader:
    return lambda table: read_jdbc(spark, conn, config, table)


def jdbc_source_label(conn: Connection, config: SourcesConfig) -> Callable[[TableConfig], str]:
    return lambda t: f"{config.source_system}://{conn.database}/{config.source_schema}.{t.name}"


def read_staged(spark: SparkSession, staged_root: str, table: TableConfig) -> DataFrame:
    return spark.read.parquet(f"{staged_root.rstrip('/')}/{table.name}")


def staged_reader(spark: SparkSession, staged_root: str) -> Reader:
    return lambda table: read_staged(spark, staged_root, table)


def load_manifest(staged_root: str) -> Dict[str, Any]:
    path = Path(staged_root.replace("dbfs:", "", 1)) / MANIFEST_FILE
    if not path.exists():
        raise FileNotFoundError(f"staged extract has no {MANIFEST_FILE}: {staged_root}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate_staged_extract(
    spark: SparkSession,
    staged_root: str,
    config: SourcesConfig,
    manifest: Mapping[str, Any],
    tables: Optional[Sequence[str]] = None,
) -> List[str]:
    """Pre-write completeness check: every selected table needs a manifest row count that its parquet matches."""
    problems = []
    entries = manifest.get("tables") or {}
    for t in config.select(tables):
        expected = (entries.get(t.name) or {}).get("row_count")
        if expected is None:
            problems.append(f"{t.name}: no row_count in {MANIFEST_FILE}")
            continue
        actual = read_staged(spark, staged_root, t).count()
        if actual != expected:
            problems.append(f"{t.name}: staged parquet has {actual} rows, manifest says {expected}")
    return problems


def staged_source_label(
    staged_root: str, manifest: Mapping[str, Any], config: SourcesConfig
) -> Callable[[TableConfig], str]:
    database = manifest.get("database", "sample")
    root = staged_root.rstrip("/")
    return lambda t: (
        f"{config.source_system}://{database}/{config.source_schema}.{t.name}@staged:{root}/{t.name}"
    )


# --------------------------------------------------------------------------- writer


def new_batch_id(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def bronze_schema(catalog: Optional[str], schema_prefix: str) -> str:
    schema = f"{schema_prefix}bronze"
    return f"{catalog}.{schema}" if catalog else schema


def add_audit_columns(df: DataFrame, ingested_at: datetime, source: str, batch_id: str) -> DataFrame:
    clash = [c for c in AUDIT_COLUMNS if c in df.columns]
    if clash:
        raise ValueError(f"source already has audit column(s) {clash}")
    return df.select(
        "*",
        F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
        F.lit(source).alias("_source"),
        F.lit(batch_id).alias("_batch_id"),
    )


def _current_watermark(spark: SparkSession, target: str, column: str) -> Any:
    return spark.table(target).agg(F.max(column).alias("wm")).first()["wm"]


def _last_commit(spark: SparkSession, target: str) -> Tuple[int, Dict[str, str]]:
    row = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()
    return int(row["version"]), dict(row["operationMetrics"] or {})


def _rows_written_since(spark: SparkSession, target: str, version_before: Optional[int]) -> int:
    """Rows in the commit this write produced (Delta ``numOutputRows``); 0 if the write was a no-op."""
    version, metrics = _last_commit(spark, target)
    if version_before is not None and version == version_before:
        return 0
    return int(metrics.get("numOutputRows", 0))


@dataclass
class IngestResult:
    table: str
    target: str
    mode: str
    source: str
    rows_written: int
    target_rows: int
    watermark_column: Optional[str] = None
    watermark_before: Any = None
    watermark_after: Any = None

    def as_dict(self) -> Dict[str, Any]:
        d = dict(self.__dict__)
        for k in ("watermark_before", "watermark_after"):
            if d[k] is not None and not isinstance(d[k], (int, float, str)):
                d[k] = str(d[k])
        return d


def ingest_table(
    spark: SparkSession,
    table: TableConfig,
    reader: Reader,
    *,
    target_schema: str,
    source: str,
    batch_id: str,
    ingested_at: datetime,
    mode: Optional[str] = None,
) -> IngestResult:
    """Read one source table with ``reader`` and write it to ``<target_schema>.sqlserver_<table>``."""
    mode = mode or table.mode
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    if mode == "incremental" and not table.watermark_column:
        raise ValueError(f"{table.name}: incremental mode requires watermark_column")
    target = f"{target_schema}.{table.bronze_name}"
    exists = spark.catalog.tableExists(target)

    version_before = _last_commit(spark, target)[0] if exists else None
    df = reader(table)
    watermark_before = None
    if mode == "incremental" and exists:
        watermark_before = _current_watermark(spark, target, table.watermark_column)
        if watermark_before is not None:
            df = df.filter(F.col(table.watermark_column) > F.lit(watermark_before))

    out = add_audit_columns(df, ingested_at, source, batch_id)
    writer = out.write.format("delta")
    if mode == "full" or not exists:
        writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    else:
        writer.mode("append").option("mergeSchema", "true").saveAsTable(target)

    rows_written = _rows_written_since(spark, target, version_before)
    return IngestResult(
        table=table.name,
        target=target,
        mode=mode if exists or mode == "full" else "incremental(initial)",
        source=source,
        rows_written=rows_written,
        target_rows=spark.table(target).count(),
        watermark_column=table.watermark_column,
        watermark_before=watermark_before,
        watermark_after=(
            _current_watermark(spark, target, table.watermark_column)
            if table.watermark_column
            else None
        ),
    )


def ingest_all(
    spark: SparkSession,
    config: SourcesConfig,
    reader: Reader,
    source_label: Callable[[TableConfig], str],
    *,
    catalog: Optional[str],
    schema_prefix: str,
    tables: Optional[Sequence[str]] = None,
    mode: Optional[str] = None,
    batch_id: Optional[str] = None,
    create_schema: bool = True,
) -> List[IngestResult]:
    """Ingest every configured table (or ``tables``) in one batch."""
    selected = config.select(tables)
    if mode == "incremental":
        no_wm = [t.name for t in selected if not t.watermark_column]
        if no_wm:
            eligible = [t.name for t in config.tables if t.watermark_column]
            raise ValueError(
                f"mode=incremental needs a watermark_column; {no_wm} have none. "
                f"Restrict tables to {eligible} or leave mode blank for per-table modes."
            )
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    batch_id = batch_id or new_batch_id()
    target_schema = bronze_schema(catalog, schema_prefix)
    if create_schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    return [
        ingest_table(
            spark,
            t,
            reader,
            target_schema=target_schema,
            source=source_label(t),
            batch_id=batch_id,
            ingested_at=ingested_at,
            mode=mode,
        )
        for t in selected
    ]


# --------------------------------------------------------------------------- staged extract


def extract_to_parquet(
    spark: SparkSession,
    conn: Connection,
    config: SourcesConfig,
    out_dir: str,
    tables: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Full JDBC extract of each table to ``<out_dir>/<table>/`` parquet plus ``_manifest.json``."""
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    manifest: Dict[str, Any] = {
        "source_system": config.source_system,
        "database": conn.database,
        "schema": config.source_schema,
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tables": {},
    }
    for t in config.select(tables):
        df = read_jdbc(spark, conn, config, t)
        df.coalesce(1).write.mode("overwrite").parquet(str(root / t.name))
        manifest["tables"][t.name] = {
            "row_count": spark.read.parquet(str(root / t.name)).count(),
            "columns": [[f.name, f.dataType.simpleString()] for f in df.schema.fields],
        }
    (root / MANIFEST_FILE).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def verify_against_manifest(
    results: Sequence[IngestResult], manifest: Mapping[str, Any]
) -> List[str]:
    """Bronze row counts that differ from the extract manifest (sources are insert-only or fully refreshed)."""
    problems = []
    for r in results:
        expected = (manifest.get("tables") or {}).get(r.table, {}).get("row_count")
        if expected is not None and r.target_rows != expected:
            problems.append(f"{r.table}: bronze has {r.target_rows} rows, extract had {expected}")
    return problems


# --------------------------------------------------------------------------- local CLI


def local_spark(app_name: str = "bronze_sqlserver", warehouse: Optional[str] = None) -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.repositories",
            os.environ.get(
                "SPARK_JARS_REPOSITORIES",
                "https://maven-central.storage-download.googleapis.com/maven2/",
            ),
        )
    )
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
    return configure_spark_with_delta_pip(
        builder, extra_packages=["com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"]
    ).getOrCreate()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m banking_etl.bronze.sqlserver")
    parser.add_argument("command", choices=["discover", "extract", "ingest"])
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--tables", default="", help="comma separated subset")
    parser.add_argument("--out", help="extract: output dir for parquet + manifest")
    parser.add_argument("--staged", help="ingest: read this staged extract instead of JDBC")
    parser.add_argument("--mode", choices=MODES, help="override per-table mode")
    parser.add_argument("--schema-prefix", default="banking_mig_")
    parser.add_argument("--warehouse", default=None, help="local spark.sql.warehouse.dir")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    tables = [t for t in args.tables.split(",") if t] or None
    if args.command == "extract" and not args.out:
        parser.error("extract requires --out")
    spark = local_spark(warehouse=args.warehouse)
    try:
        return _run_cli(spark, args, config, tables)
    finally:
        spark.stop()


def _run_cli(spark: SparkSession, args: argparse.Namespace, config: SourcesConfig,
             tables: Optional[List[str]]) -> int:
    if args.command == "ingest" and args.staged:
        manifest = load_manifest(args.staged)
        problems = validate_staged_extract(spark, args.staged, config, manifest, tables)
        if problems:
            print("\n".join(problems))
            return 1
        reader = staged_reader(spark, args.staged)
        label = staged_source_label(args.staged, manifest, config)
    else:
        conn = resolve_connection(config)
        manifest = {}
        if args.command != "discover":
            assert_source_coverage(spark, conn, config)
        reader, label = jdbc_reader(spark, conn, config), jdbc_source_label(conn, config)

    if args.command == "discover":
        found = discover_tables(spark, conn, config.source_schema)
        print(json.dumps({"tables": found, **check_coverage(config, found)}, indent=2))
    elif args.command == "extract":
        print(json.dumps(extract_to_parquet(spark, conn, config, args.out, tables), indent=2))
    else:
        results = ingest_all(
            spark, config, reader, label, catalog=None, schema_prefix=args.schema_prefix,
            tables=tables, mode=args.mode,
        )
        print(json.dumps([r.as_dict() for r in results], indent=2))
        problems = verify_against_manifest(results, manifest)
        if problems:
            print("\n".join(problems))
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
