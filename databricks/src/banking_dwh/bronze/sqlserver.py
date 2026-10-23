"""SQL Server `sample` DB extraction (JDBC) -> Bronze (Ticket 3).

Replaces the six Talend ``tMSSqlInput`` components (``Load_Dim{Branch,Account,Customer}``,
``Load_FactTransaction``). Each one runs ``SELECT dbo.<t>.<col>, ... FROM dbo.<t>`` against
the ``Sample_DB_Connection``; here the same column list is read with the Spark JDBC
connector and written to the ``BRONZE_SQLSERVER_*`` contracts (``bronze.sqlserver_<table>``).

* Full refresh per batch: every run overwrites the Bronze table with the current source
  snapshot plus the contract lineage columns ``_ingested_at`` / ``_batch_id`` (one value
  per run, shared by all tables).
* ``source="jdbc"`` reads SQL Server and (optionally) stages each raw extract as parquet
  under ``cfg.landing_path("sqlserver", <table>)``.
* ``source="staged"`` reads previously staged parquet from that landing path instead.
  Used where the cluster cannot reach SQL Server (serverless dev workspace).

Local staging for the staged path (needs a reachable SQL Server and the mssql-jdbc jar,
fetched as a Spark package)::

    MSSQL_PASSWORD=... python -m banking_dwh.bronze.sqlserver extract \
        --host localhost --port 1433 --database sample --user sa --out /tmp/landing/sqlserver \
        --trust-server-certificate   # only for a throwaway container with a self-signed cert
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping

from ..common.config import EnvConfig, validate_identifier
from ..contracts.ddl import create_table_sql
from ..contracts.tables import (
    BRONZE_SQLSERVER_ACCOUNT,
    BRONZE_SQLSERVER_BRANCH,
    BRONZE_SQLSERVER_CITY,
    BRONZE_SQLSERVER_CUSTOMER,
    BRONZE_SQLSERVER_STATE,
    BRONZE_SQLSERVER_TRANSACTION,
    TableContract,
)

# source table -> Bronze contract (order = Talend job order: dims first, then the fact source).
CONTRACTS: dict[str, TableContract] = {
    "branch": BRONZE_SQLSERVER_BRANCH,
    "account": BRONZE_SQLSERVER_ACCOUNT,
    "customer": BRONZE_SQLSERVER_CUSTOMER,
    "city": BRONZE_SQLSERVER_CITY,
    "state": BRONZE_SQLSERVER_STATE,
    "transaction_db": BRONZE_SQLSERVER_TRANSACTION,
}
# SQL Server system / tooling tables that must never be extracted.
EXCLUDED_TABLES = frozenset({"sysdiagrams"})
# Row counts of data_sources/sample.bak (verified against a SQL Server 2022 restore).
EXPECTED_ROW_COUNTS: dict[str, int] = {
    "branch": 5, "state": 9, "city": 52, "customer": 20, "account": 21, "transaction_db": 10,
}
SOURCES = ("jdbc", "staged")
MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
LINEAGE_COLUMNS = ("_ingested_at", "_batch_id")
_CHAR_AS_STRING = "spark.sql.legacy.charVarcharAsString"
_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")  # alnum first: no "." / ".." path segments
_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------
def contract_for(table: str) -> TableContract:
    if table not in CONTRACTS:
        raise ValueError(f"No Bronze contract for SQL Server table {table!r}; expected one of {list(CONTRACTS)}")
    return CONTRACTS[table]


def resolve_tables(cfg: EnvConfig, requested: str | Iterable[str] | None = None) -> list[str]:
    """Tables to extract: the configured list (``jdbc.source_tables``) or a validated subset."""
    configured = [t for t in (cfg.jdbc.source_tables if cfg.jdbc else CONTRACTS) if t not in EXCLUDED_TABLES]
    if requested is None or requested == "" or requested == "all":
        return configured
    names = [t.strip() for t in requested.split(",")] if isinstance(requested, str) else list(requested)
    out = []
    for t in names:
        if not t:
            continue
        validate_identifier(t, "source table")
        if t in EXCLUDED_TABLES or t not in configured:
            raise ValueError(f"Table {t!r} is not an extractable source table; expected a subset of {configured}")
        if t not in out:
            out.append(t)
    if not out:
        raise ValueError(f"No source tables selected from {requested!r}; expected 'all' or a subset of {configured}")
    return out


def source_query(table: str, source_schema: str = "dbo") -> str:
    """The Talend tMSSqlInput query: explicit contract column list, no filter."""
    validate_identifier(source_schema, "source schema")
    cols = ",\n       ".join(f"{source_schema}.{table}.{c}" for c in contract_for(table).business_columns)
    return f"SELECT {cols}\nFROM {source_schema}.{table}"


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def validate_batch_id(value: str) -> str:
    if not isinstance(value, str) or not _BATCH_ID.match(value):
        raise ValueError(f"Invalid batch_id: {value!r} (expected [A-Za-z0-9][A-Za-z0-9_.:-]{{0,127}})")
    return value


def spark_type(contract_type: str):
    """Contract type string -> Spark DataType (pure Python; safe on Spark Connect)."""
    from pyspark.sql import types as T

    simple = {
        "INT": T.IntegerType(), "INTEGER": T.IntegerType(), "BIGINT": T.LongType(),
        "STRING": T.StringType(), "TIMESTAMP": T.TimestampType(), "DATE": T.DateType(),
        "BOOLEAN": T.BooleanType(), "DOUBLE": T.DoubleType(),
    }
    key = contract_type.strip().upper()
    if key in simple:
        return simple[key]
    m = _DECIMAL.match(key)
    if m:
        return T.DecimalType(int(m.group(1)), int(m.group(2)))
    raise ValueError(f"Unsupported contract type {contract_type!r}")


def _same_type(actual, expected) -> bool:
    from pyspark.sql import types as T

    # varchar/char metadata (Spark 4 JDBC) still means STRING for Bronze.
    if isinstance(expected, T.StringType):
        return isinstance(actual, (T.StringType, T.VarcharType, T.CharType))
    return actual == expected


@contextmanager
def char_varchar_as_string(spark) -> Iterator[bool]:
    """Spark 4 JDBC tags varchar columns with char/varchar metadata, which makes Delta
    store VARCHAR(n) when it creates a table from the frame. Treat them as plain STRING while
    the extract is planned/written. Serverless (Spark Connect) rejects this legacy conf; the
    guard then is that Bronze tables are pre-created from the contract (STRING) and written
    without schema evolution. Yields whether the conf was applied."""
    try:
        previous = spark.conf.get(_CHAR_AS_STRING, None)
        spark.conf.set(_CHAR_AS_STRING, "true")
    except Exception:  # CONFIG_NOT_AVAILABLE on Databricks serverless
        yield False
        return
    try:
        yield True
    finally:
        if previous is None:
            try:
                spark.conf.unset(_CHAR_AS_STRING)
            except Exception:
                spark.conf.set(_CHAR_AS_STRING, "false")
        else:
            spark.conf.set(_CHAR_AS_STRING, previous)


# ---------------------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------------------
def read_jdbc(spark, options: Mapping[str, str], table: str, source_schema: str = "dbo"):
    """Read one source table over JDBC with the Talend column list."""
    reader = spark.read.format("jdbc")
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.option("query", source_query(table, source_schema)).load()


def list_source_tables(spark, options: Mapping[str, str], source_schema: str = "dbo") -> list[str]:
    """User tables present in the source schema, excluding ``sysdiagrams``."""
    validate_identifier(source_schema, "source schema")
    q = (
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{source_schema}'"
    )
    reader = spark.read.format("jdbc")
    for k, v in options.items():
        reader = reader.option(k, v)
    rows = reader.option("query", q).load().collect()
    return sorted(r[0] for r in rows if r[0] not in EXCLUDED_TABLES)


def read_staged(spark, path: str):
    return spark.read.parquet(path)


# ---------------------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------------------
def conform_to_contract(df, contract: TableContract, *, batch_id: str, ingested_at: datetime):
    """Select the contract business columns (case-insensitive match), cast only where the
    source type differs (identity casts over JDBC columns fail plan validation) and append
    the contract lineage columns. Missing columns fail fast; extra source columns are dropped."""
    from pyspark.sql import functions as F

    by_lower = {f.name.lower(): f for f in df.schema.fields}
    missing = [c for c in contract.business_columns if c.lower() not in by_lower]
    if missing:
        raise ValueError(f"{contract.name}: source is missing contract columns {missing}")
    cols = []
    for c in contract.columns:
        if c.name in LINEAGE_COLUMNS:
            continue
        field = by_lower[c.name.lower()]
        expected = spark_type(c.type)
        col = F.col(f"`{field.name}`")
        if not _same_type(field.dataType, expected):
            col = col.cast(expected)
        cols.append(col.alias(c.name))
    extra = {
        "_ingested_at": F.lit(ingested_at).cast("timestamp"),
        "_batch_id": F.lit(batch_id),
    }
    for c in contract.columns:
        if c.name in LINEAGE_COLUMNS:
            cols.append(extra[c.name].alias(c.name))
        elif c.name.startswith("_"):
            raise ValueError(f"{contract.name}: unknown lineage column {c.name}")
    return df.select(*cols)


def null_key_count(df, contract: TableContract) -> int:
    """Rows violating the contract's NOT NULL business columns (would fail the Delta write)."""
    from pyspark.sql import functions as F

    required = [c.name for c in contract.columns if not c.nullable and c.name not in LINEAGE_COLUMNS]
    if not required:
        return 0
    cond = None
    for name in required:
        part = F.col(name).isNull()
        cond = part if cond is None else (cond | part)
    return df.filter(cond).count()


# ---------------------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------------------
def ensure_bronze_table(spark, cfg: EnvConfig, contract: TableContract, *, unity_catalog: bool = True) -> str:
    spark.sql(create_table_sql(cfg, contract, unity_catalog=unity_catalog))
    return cfg.table(contract.layer, contract.name)


def write_bronze(spark, cfg: EnvConfig, contract: TableContract, df, *, unity_catalog: bool = True) -> str:
    """Full-refresh overwrite that keeps the contract DDL (comments, properties, NOT NULL)."""
    from pyspark.sql import functions as F

    name = ensure_bronze_table(spark, cfg, contract, unity_catalog=unity_catalog)
    df.writeTo(name).overwrite(F.lit(True))
    return name


def stage_parquet(df, path: str) -> str:
    """Write the raw extract (source columns only) as parquet, replacing whatever is at ``path``."""
    df.write.mode("overwrite").parquet(path)
    return path


def incoming_root(landing_root: str, batch_id: str) -> str:
    """Per-batch scratch folder next to the published ``<root>/<table>`` folders. JDBC extracts
    land here first so a failed read never replaces the last good staged snapshot; the
    ``_`` prefix keeps Spark from ever listing it as data."""
    return f"{landing_root.rstrip('/')}/_incoming/{validate_batch_id(batch_id)}"


def remove_tree(path: str, spark=None, remove: Callable[[str], Any] | None = None) -> None:
    """Best-effort recursive delete of a scratch folder. Local/FUSE paths (``/Volumes`` is
    FUSE-mounted on Databricks) use ``shutil``; URIs use ``remove`` (e.g. ``dbutils.fs.rm``) or,
    on classic Spark, the Hadoop FileSystem of the path's scheme."""
    try:
        if remove is not None:
            remove(path)
        elif "://" in path or path.startswith("dbfs:"):
            jvm = spark._jvm  # not available on Spark Connect -> AttributeError -> warning
            hpath = jvm.org.apache.hadoop.fs.Path(path)
            hpath.getFileSystem(spark._jsc.hadoopConfiguration()).delete(hpath, True)
        else:
            shutil.rmtree(path, ignore_errors=True)
            try:
                os.rmdir(os.path.dirname(path.rstrip("/")))  # the shared _incoming folder, once empty
            except OSError:
                pass
    except Exception as e:  # cleanup must never mask the load result
        warnings.warn(f"Could not remove scratch folder {path}: {e}", stacklevel=2)


# ---------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------
@dataclass
class TableResult:
    source_table: str
    bronze_table: str
    source: str
    source_location: str
    rows: int
    expected_rows: int | None
    staged_to: str | None = None

    @property
    def count_ok(self) -> bool:
        return self.expected_rows is None or self.rows == self.expected_rows

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table, "bronze_table": self.bronze_table,
            "source": self.source, "source_location": self.source_location,
            "rows": self.rows, "expected_rows": self.expected_rows,
            "count_ok": self.count_ok, "staged_to": self.staged_to,
        }


def ingest(
    spark,
    cfg: EnvConfig,
    *,
    source: str = "jdbc",
    jdbc: Mapping[str, str] | None = None,
    tables: str | Iterable[str] | None = None,
    batch_id: str | None = None,
    stage: bool = True,
    staged_root: str | None = None,
    expected_counts: Mapping[str, int] | None = EXPECTED_ROW_COUNTS,
    assert_counts: bool = False,
    unity_catalog: bool = True,
    remove_path: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Extract every requested source table into its Bronze contract table.

    ``staged_root`` overrides the landing root (``cfg.landing_path("sqlserver")``), e.g. a
    local directory in tests. With ``assert_counts`` the run fails after loading if any
    table's row count differs from ``expected_counts``.

    JDBC + ``stage``: extracts land in ``<landing>/_incoming/<batch_id>/`` and are published to
    the landing folders *before* Bronze is written, so the landing snapshot is never older than
    Bronze and a staged rerun restores Bronze after a failed write. If publishing a snapshot fails,
    the scratch copy is kept and ``recover_batch`` finishes the batch from it.
    ``remove_path`` deletes the scratch folder for non-local URIs (e.g. ``dbutils.fs.rm``).
    """
    from pyspark.sql import functions as F

    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES}, got {source!r}")
    if source == "jdbc" and not jdbc:
        raise ValueError("source='jdbc' needs JDBC options (see banking_dwh.common.config.jdbc_options)")
    names = resolve_tables(cfg, tables)
    batch_id = validate_batch_id(batch_id or new_batch_id())
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0)
    source_schema = cfg.jdbc.source_schema if cfg.jdbc else "dbo"

    landing_root = staged_root.rstrip("/") if staged_root else cfg.landing_path("sqlserver")
    scratch = incoming_root(landing_root, batch_id)
    stage_jdbc = source == "jdbc" and stage

    def landing(table: str) -> str:
        return f"{landing_root}/{table}" if staged_root else cfg.landing_path("sqlserver", table)

    # Phase 1 (no writes to Bronze or the published landing folders): read, conform, validate
    # every table, so a bad table or count mismatch can't leave a mix of old and new snapshots.
    prepared: list[tuple[str, TableContract, str, Any, int, int | None]] = []
    results: list[TableResult] = []
    keep_scratch = False
    try:
        with char_varchar_as_string(spark) as char_as_string:
            if source == "jdbc":
                present = set(list_source_tables(spark, jdbc, source_schema))
                absent = [t for t in names if t not in present]
                if absent:
                    raise ValueError(f"Source tables not found in {source_schema}: {absent}")
            for table in names:
                contract = contract_for(table)
                if source == "jdbc":
                    location = f"jdbc:{source_schema}.{table}"
                    raw = read_jdbc(spark, jdbc, table, source_schema)
                    if stage_jdbc:  # single source read; Bronze == published staged copy
                        raw = read_staged(spark, stage_parquet(raw, f"{scratch}/{table}"))
                else:
                    location = landing(table)
                    raw = read_staged(spark, location)
                df = conform_to_contract(raw, contract, batch_id=batch_id, ingested_at=ingested_at)
                bad = null_key_count(df, contract)
                if bad:
                    raise ValueError(f"{table}: {bad} rows have NULL in NOT NULL contract columns")
                prepared.append((table, contract, location, df, df.count(), (expected_counts or {}).get(table)))
            mismatch = {t: (n, e) for t, _, _, _, n, e in prepared if e is not None and n != e}
            if assert_counts and mismatch:
                raise AssertionError(f"Row-count mismatch (actual, expected): {mismatch}; no table was written")

            # Phase 2: publish the staged snapshots, then Bronze, for every validated table.
            staged_to: dict[str, str] = {}
            if stage_jdbc:
                for table, *_ in prepared:
                    try:
                        staged_to[table] = stage_parquet(read_staged(spark, f"{scratch}/{table}"), landing(table))
                    except Exception as e:
                        keep_scratch = True
                        raise RuntimeError(
                            f"Publishing the staged snapshot of {table} failed; batch {batch_id} is intact in "
                            f"{scratch} (finish it with recover_batch(spark, cfg, '{scratch}'))"
                        ) from e
            for table, contract, location, df, _, exp in prepared:
                name = write_bronze(spark, cfg, contract, df, unity_catalog=unity_catalog)
                rows = spark.table(name).filter(F.col("_batch_id") == F.lit(batch_id)).count()
                results.append(TableResult(table, name, source, location, rows, exp, staged_to.get(table)))
    finally:
        if stage_jdbc and not keep_scratch:
            remove_tree(scratch, spark, remove_path)

    summary = {
        "batch_id": batch_id,
        "ingested_at": ingested_at.isoformat(),
        "source": source,
        "tables": [r.as_dict() for r in results],
        "row_counts": {r.source_table: r.rows for r in results},
        "counts_ok": all(r.count_ok for r in results),
        "char_varchar_as_string": char_as_string,
    }
    if assert_counts and not summary["counts_ok"]:
        bad = {r.source_table: (r.rows, r.expected_rows) for r in results if not r.count_ok}
        raise AssertionError(f"Row-count mismatch (actual, expected): {bad}")
    return summary


def recover_batch(spark, cfg: EnvConfig, scratch: str, *, staged_root: str | None = None,
                  remove_path: Callable[[str], Any] | None = None, **ingest_kwargs: Any) -> dict[str, Any]:
    """Finish a JDBC batch whose snapshot publication failed: publish every table kept in
    ``scratch`` to the landing folders, load Bronze from them (staged ingest), then drop scratch."""
    root = staged_root.rstrip("/") if staged_root else None
    tables = resolve_tables(cfg, ingest_kwargs.pop("tables", None))
    for table in tables:
        dest = f"{root}/{table}" if root else cfg.landing_path("sqlserver", table)
        stage_parquet(read_staged(spark, f"{scratch.rstrip('/')}/{table}"), dest)
    summary = ingest(spark, cfg, source="staged", staged_root=staged_root, tables=tables, **ingest_kwargs)
    remove_tree(scratch, spark, remove_path)
    summary["recovered_from"] = scratch
    return summary


# ---------------------------------------------------------------------------------------
# Local CLI: extract SQL Server -> parquet staging folders (for the serverless staged path)
# ---------------------------------------------------------------------------------------
def local_spark(jars: str | None = None, repositories: str | None = None):
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master("local[2]").appName("banking_dwh-sqlserver-extract")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.ui.enabled", "false")
    )
    if jars:
        b = b.config("spark.jars", jars)
    else:
        b = b.config("spark.jars.packages", MSSQL_JDBC_PACKAGE)
        if repositories:
            b = b.config("spark.jars.repositories", repositories)
    return b.getOrCreate()


def local_jdbc_options(cfg: EnvConfig, *, host: str, port: str | int, database: str, user: str,
                       password: str, trust_server_certificate: bool = False) -> dict[str, str]:
    """JDBC options for a directly reachable SQL Server (local CLI / tests). TLS certificates
    are verified unless ``trust_server_certificate`` is set for a throwaway local container."""
    assert cfg.jdbc is not None
    url = cfg.jdbc.url(host, port, database)
    if trust_server_certificate:
        url = url.replace("trustServerCertificate=false", "trustServerCertificate=true")
    return {"url": url, "user": user, "password": password, "driver": cfg.jdbc.driver}


def extract_to_parquet(spark, jdbc: Mapping[str, str], out_dir: str, tables: Iterable[str],
                       source_schema: str = "dbo") -> dict[str, int]:
    """Extract every table to ``<out_dir>/_incoming/<id>/`` first and publish to
    ``<out_dir>/<table>`` only once all reads succeeded, keeping the previous snapshot otherwise."""
    tables = list(tables)
    scratch = incoming_root(out_dir, new_batch_id())
    counts = {}
    try:
        with char_varchar_as_string(spark):
            present = set(list_source_tables(spark, jdbc, source_schema))
            missing = [t for t in tables if t not in present]
            if missing:
                raise ValueError(f"Source tables not found in {source_schema}: {missing}")
            for t in tables:
                stage_parquet(read_jdbc(spark, jdbc, t, source_schema), f"{scratch}/{t}")
            for t in tables:
                path = stage_parquet(spark.read.parquet(f"{scratch}/{t}"), os.path.join(out_dir, t))
                counts[t] = spark.read.parquet(path).count()
    finally:
        remove_tree(scratch, spark)
    return counts


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Extract the SQL Server sample DB to parquet staging folders.")
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("extract")
    e.add_argument("--host", default="localhost")
    e.add_argument("--port", default="1433")
    e.add_argument("--database", default="sample")
    e.add_argument("--user", default="sa")
    e.add_argument("--password-env", default="MSSQL_PASSWORD", help="env var holding the password")
    e.add_argument("--out", required=True, help="output root; one sub-folder per table")
    e.add_argument("--tables", default="all")
    e.add_argument("--target", default="dev")
    e.add_argument("--jars", default=os.environ.get("MSSQL_JDBC_JAR"), help="mssql-jdbc jar (else Spark package)")
    e.add_argument("--trust-server-certificate", action="store_true",
                   help="accept a self-signed certificate (throwaway local container only)")
    e.add_argument("--repositories", default="https://maven-central.storage-download.googleapis.com/maven2/")
    a = p.parse_args(argv)

    cfg = EnvConfig.from_target(a.target)
    password = os.environ.get(a.password_env)
    if not password:
        raise SystemExit(f"Set the SQL Server password in ${a.password_env}")
    opts = local_jdbc_options(cfg, host=a.host, port=a.port, database=a.database, user=a.user,
                              password=password, trust_server_certificate=a.trust_server_certificate)
    spark = local_spark(a.jars, a.repositories)
    try:
        counts = extract_to_parquet(spark, opts, a.out, resolve_tables(cfg, a.tables), cfg.jdbc.source_schema)
    finally:
        spark.stop()
    import json

    print(json.dumps({"out": a.out, "row_counts": counts}))


if __name__ == "__main__":
    main()
