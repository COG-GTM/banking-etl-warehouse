# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — legacy SQL Server `sample` database
# MAGIC
# MAGIC Replaces the `tMSSqlInput` components of the four Talend jobs
# MAGIC (`Load_DimBranch`, `Load_DimAccount`, `Load_DimCustomer`, `Load_FactTransaction`)
# MAGIC with a single parameter-driven JDBC ingestion into `dwh.bronze.<source_table>`.
# MAGIC
# MAGIC Bronze is **raw**: no casting, no cleansing, no renaming. The source schema as
# MAGIC returned by the JDBC driver is preserved verbatim and only audit columns are added:
# MAGIC
# MAGIC | column | meaning |
# MAGIC | --- | --- |
# MAGIC | `_ingested_at` | `TIMESTAMP` — wall-clock start of this notebook run (same value for the whole run, so a run is easy to isolate) |
# MAGIC | `_source_system` | `STRING` — always `sqlserver_sample` |
# MAGIC | `_source_table` | `STRING` — `dbo.<table>` in the source database |
# MAGIC
# MAGIC Conforming, type mapping (`MONEY -> DECIMAL(19,4)`, `DATETIME -> TIMESTAMP`, ...),
# MAGIC deduplication and the uppercase cleansing that the Talend `tMap`s performed all
# MAGIC belong to silver/gold, not here.
# MAGIC
# MAGIC No live SQL Server was available when this notebook was written; see
# MAGIC `README_sqlserver.md` for what was and was not validated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")
dbutils.widgets.text("catalog", "dwh", "Unity Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("source_database", "sample", "Source SQL Server database")
dbutils.widgets.text("tables", "", "Table subset (comma separated, blank = all)")
dbutils.widgets.dropdown("load_mode", "full_refresh", ["full_refresh", "incremental"], "Load mode")
dbutils.widgets.text("incremental_start", "", "Incremental lower bound override (ISO ts, blank = derive from bronze)")
dbutils.widgets.text("secret_scope", "dwh", "Databricks secret scope")
dbutils.widgets.dropdown("num_partitions", "8", ["1", "2", "4", "8", "16", "32"], "Max JDBC partitions")
dbutils.widgets.text("fetchsize", "10000", "JDBC fetchsize (rows per round trip)")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run (plan only, no writes)")

# COMMAND ----------

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=logging.INFO)
log = logging.getLogger("bronze.sqlserver")
log.setLevel(logging.INFO)

spark: SparkSession = spark  # noqa: F821 - provided by Databricks

ENV = dbutils.widgets.get("env").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SOURCE_DATABASE = dbutils.widgets.get("source_database").strip()
TABLE_SUBSET = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
LOAD_MODE = dbutils.widgets.get("load_mode").strip()
INCREMENTAL_START = dbutils.widgets.get("incremental_start").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
MAX_PARTITIONS = int(dbutils.widgets.get("num_partitions"))
FETCHSIZE = int(dbutils.widgets.get("fetchsize"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

SOURCE_SYSTEM = "sqlserver_sample"
SOURCE_SCHEMA = "dbo"

# One timestamp for the whole run so every row written by this run shares an ingest stamp.
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source table manifest
# MAGIC
# MAGIC Derived from the Talend job definitions in `talend_jobs/*.zip`
# MAGIC (`IDX_INTERNSHIP/process/<Job>_0.1.item` input components and
# MAGIC `metadata/connections/Sample_DB_Connection_0.1.item`, which carries the full
# MAGIC `dbo` catalog for the `sample` database).
# MAGIC
# MAGIC * `Load_DimBranch`  -> `dbo.branch`
# MAGIC * `Load_DimAccount` -> `dbo.account`
# MAGIC * `Load_DimCustomer` -> `dbo.customer`, `dbo.city`, `dbo.state`
# MAGIC * `Load_FactTransaction` -> `dbo.transaction_db` (the SQL-sourced transactions; the CSV and Excel streams are ticket 5)
# MAGIC
# MAGIC Per table the manifest records:
# MAGIC * `primary_key` — used for merge-mode dedup and as the natural parallel-read key.
# MAGIC * `partition_column` — numeric, monotonically-ish distributed column used for
# MAGIC   `partitionColumn`/`lowerBound`/`upperBound` parallel reads. Bounds are computed at
# MAGIC   runtime from a `MIN`/`MAX`/`COUNT` probe, never hard-coded.
# MAGIC * `watermark_column` — column used by the incremental path. `None` means the table
# MAGIC   has no usable watermark in the source and is always fully refreshed.
# MAGIC * `parallel_read_threshold` — below this row count a single-partition read is used;
# MAGIC   splitting tiny tables costs more in connections than it saves.

# COMMAND ----------


@dataclass(frozen=True)
class SourceTable:
    name: str
    primary_key: List[str]
    columns: List[str]
    partition_column: Optional[str] = None
    watermark_column: Optional[str] = None
    parallel_read_threshold: int = 1_000_000
    notes: str = ""
    target_table: str = field(default="", compare=False)

    @property
    def source_qualified(self) -> str:
        return f"{SOURCE_SCHEMA}.{self.name}"

    def target(self, catalog: str, schema: str) -> str:
        return f"{catalog}.{schema}.{self.target_table or self.name}"


MANIFEST: List[SourceTable] = [
    SourceTable(
        name="branch",
        primary_key=["branch_id"],
        columns=["branch_id", "branch_name", "branch_location"],
        partition_column="branch_id",
        watermark_column=None,
        notes="Load_DimBranch source. Small master data, no watermark -> always full refresh.",
    ),
    SourceTable(
        name="account",
        primary_key=["account_id"],
        columns=[
            "account_id",
            "customer_id",
            "account_type",
            "balance",
            "date_opened",
            "status",
        ],
        partition_column="account_id",
        watermark_column="date_opened",
        notes=(
            "Load_DimAccount source. date_opened only advances for new accounts, so an "
            "incremental run picks up new accounts but NOT balance/status changes on "
            "existing ones - run full_refresh when those matter."
        ),
    ),
    SourceTable(
        name="customer",
        primary_key=["customer_id"],
        columns=[
            "customer_id",
            "customer_name",
            "address",
            "city_id",
            "age",
            "gender",
            "email",
        ],
        partition_column="customer_id",
        watermark_column=None,
        notes="Load_DimCustomer main source. No date column in dbo.customer -> full refresh only.",
    ),
    SourceTable(
        name="city",
        primary_key=["city_id"],
        columns=["city_id", "city_name", "state_id"],
        partition_column="city_id",
        watermark_column=None,
        notes="Load_DimCustomer lookup (tMap join customer.city_id = city.city_id).",
    ),
    SourceTable(
        name="state",
        primary_key=["state_id"],
        columns=["state_id", "state_name"],
        partition_column="state_id",
        watermark_column=None,
        notes="Load_DimCustomer lookup (tMap join city.state_id = state.state_id).",
    ),
    SourceTable(
        name="transaction_db",
        primary_key=["transaction_id"],
        columns=[
            "transaction_id",
            "account_id",
            "transaction_date",
            "amount",
            "transaction_type",
            "branch_id",
        ],
        partition_column="transaction_id",
        watermark_column="transaction_date",
        parallel_read_threshold=200_000,
        notes=(
            "Load_FactTransaction SQL stream (tUnite input 1). The CSV and Excel streams "
            "land as dwh.bronze.transaction_csv / dwh.bronze.transaction_excel (ticket 5)."
        ),
    ),
]

MANIFEST_BY_NAME: Dict[str, SourceTable] = {t.name: t for t in MANIFEST}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection
# MAGIC
# MAGIC Everything comes from `dbutils.secrets.get(scope="dwh", ...)`; nothing is hard-coded
# MAGIC and no secret is ever logged. Expected keys in the scope (suffixed per environment so
# MAGIC one scope can serve dev/test/prod):
# MAGIC
# MAGIC ```
# MAGIC sqlserver-host-<env>      e.g. sqlserver-host-dev
# MAGIC sqlserver-port-<env>      optional, defaults to 1433
# MAGIC sqlserver-database-<env>  optional, defaults to the source_database widget
# MAGIC sqlserver-user-<env>
# MAGIC sqlserver-password-<env>
# MAGIC sqlserver-encrypt-<env>                 optional, "true"/"false", default "true"
# MAGIC sqlserver-trust-server-certificate-<env> optional, "true"/"false", default "false"
# MAGIC ```
# MAGIC
# MAGIC The Talend `Sample_DB_Connection` used
# MAGIC `trustServerCertificate=true;integratedSecurity=true` against `localhost:1433`.
# MAGIC That is a developer-laptop setup: here we default to `encrypt=true` with certificate
# MAGIC validation **on** and require the trust override to be an explicit, per-environment
# MAGIC opt-in.

# COMMAND ----------

JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def _secret(key: str, default: Optional[str] = None) -> str:
    """Read `<key>-<env>` from the secret scope, falling back to `<key>` then `default`."""
    for candidate in (f"{key}-{ENV}", key):
        try:
            value = dbutils.secrets.get(scope=SECRET_SCOPE, key=candidate)
        except Exception:  # noqa: BLE001 - dbutils raises a generic exception for a missing key
            continue
        if value:
            return value
    if default is not None:
        return default
    raise ValueError(
        f"Secret '{key}-{ENV}' (or '{key}') not found in scope '{SECRET_SCOPE}'. "
        f"Create it with: databricks secrets put --scope {SECRET_SCOPE} --key {key}-{ENV}"
    )


def build_jdbc_options() -> Dict[str, str]:
    host = _secret("sqlserver-host")
    port = _secret("sqlserver-port", "1433")
    database = _secret("sqlserver-database", SOURCE_DATABASE)
    encrypt = _secret("sqlserver-encrypt", "true").lower()
    trust_cert = _secret("sqlserver-trust-server-certificate", "false").lower()

    url = (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        f"encrypt={encrypt};"
        f"trustServerCertificate={trust_cert};"
        "loginTimeout=30"
    )
    if encrypt == "true" and trust_cert == "true":
        log.warning(
            "trustServerCertificate=true: the TLS certificate of %s is NOT validated. "
            "Acceptable for dev, not for prod.",
            host,
        )
    return {
        "url": url,
        "driver": JDBC_DRIVER,
        "user": _secret("sqlserver-user"),
        "password": _secret("sqlserver-password"),
        # fetchsize = rows the driver buffers per network round trip. The SQL Server
        # driver defaults to 0 (driver-chosen, effectively row-at-a-time for large
        # result sets), which makes wide scans network-bound. 10k rows is a good
        # trade-off for these narrow tables; lower it if executors hit memory pressure.
        "fetchsize": str(FETCHSIZE),
        "queryTimeout": "0",
    }


JDBC_OPTIONS = build_jdbc_options() if not DRY_RUN else {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: retry, existence checks, bounds probe

# COMMAND ----------


def with_retry(fn, *, description: str, attempts: int = 3, base_delay: float = 5.0):
    """Retry a JDBC/Delta operation with exponential backoff. Re-raises the last error."""
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - we retry any transient driver/cluster error
            last_error = exc
            if attempt == attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            log.warning(
                "%s failed (attempt %d/%d): %s - retrying in %.0fs",
                description,
                attempt,
                attempts,
                exc,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"{description} failed after {attempts} attempts") from last_error


def jdbc_query(query: str) -> DataFrame:
    """Run an arbitrary query against the source and return it as a single-partition DataFrame."""
    return (
        spark.read.format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", f"({query}) AS q")
        .load()
    )


def assert_source_table_exists(table: SourceTable) -> None:
    query = (
        "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = '{table.name}'"
    )
    found = with_retry(
        lambda: jdbc_query(query).collect()[0]["c"],
        description=f"existence probe for {table.source_qualified}",
    )
    if not found:
        raise RuntimeError(
            f"Source table {SOURCE_DATABASE}.{table.source_qualified} does not exist. "
            "The bronze manifest was derived from the Talend job definitions in "
            "talend_jobs/*.zip; either the source database was restored from a different "
            "sample.bak, or the manifest entry is stale. Fix the manifest or restore the "
            "expected database - do not silently skip the table."
        )


def assert_source_columns(table: SourceTable) -> None:
    query = (
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = '{table.name}'"
    )
    actual = {
        row["COLUMN_NAME"].lower()
        for row in with_retry(
            lambda: jdbc_query(query).collect(),
            description=f"column probe for {table.source_qualified}",
        )
    }
    missing = [c for c in table.columns if c.lower() not in actual]
    if missing:
        raise RuntimeError(
            f"Source table {table.source_qualified} is missing manifest column(s) {missing}. "
            f"Columns present: {sorted(actual)}."
        )


@dataclass
class ReadBounds:
    row_count: int
    lower: Optional[int]
    upper: Optional[int]

    @property
    def usable(self) -> bool:
        return (
            self.lower is not None
            and self.upper is not None
            and self.upper > self.lower
            and self.row_count > 0
        )


def probe_bounds(table: SourceTable, predicate: Optional[str]) -> ReadBounds:
    """MIN/MAX/COUNT over the partition column so parallel reads are never hard-coded."""
    if not table.partition_column:
        return ReadBounds(row_count=0, lower=None, upper=None)
    where = f" WHERE {predicate}" if predicate else ""
    query = (
        f"SELECT COUNT_BIG(*) AS row_count, "
        f"MIN({table.partition_column}) AS lo, MAX({table.partition_column}) AS hi "
        f"FROM {table.source_qualified}{where}"
    )
    row = with_retry(
        lambda: jdbc_query(query).collect()[0],
        description=f"bounds probe for {table.source_qualified}",
    )
    lo, hi = row["lo"], row["hi"]
    return ReadBounds(
        row_count=int(row["row_count"] or 0),
        lower=int(lo) if lo is not None else None,
        upper=int(hi) if hi is not None else None,
    )


def plan_partitions(table: SourceTable, bounds: ReadBounds) -> int:
    """How many JDBC partitions to use. 1 means a single-partition read."""
    if MAX_PARTITIONS <= 1 or not bounds.usable:
        return 1
    if bounds.row_count < table.parallel_read_threshold:
        return 1
    # Never create more partitions than there are distinct key values to spread over.
    span = bounds.upper - bounds.lower + 1
    return max(1, min(MAX_PARTITIONS, span))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading
# MAGIC
# MAGIC A read is either:
# MAGIC * **parallel** — `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` on the
# MAGIC   manifest's numeric key, bounds from `probe_bounds`. Spark issues `numPartitions`
# MAGIC   concurrent queries, each with a generated range predicate.
# MAGIC * **single-partition** (the documented fallback) — one query, one connection. Used when
# MAGIC   the table has no numeric partition column, when the probe returns degenerate bounds
# MAGIC   (empty table, or every row shares one key value), when the row count is below the
# MAGIC   table's `parallel_read_threshold`, or when the `num_partitions` widget is `1`.
# MAGIC
# MAGIC Either way the projection is the manifest column list in source order and types are
# MAGIC left exactly as the driver reports them.

# COMMAND ----------


def build_select(table: SourceTable, predicate: Optional[str]) -> str:
    cols = ", ".join(table.columns)
    where = f" WHERE {predicate}" if predicate else ""
    return f"SELECT {cols} FROM {table.source_qualified}{where}"


def read_source(table: SourceTable, predicate: Optional[str], bounds: ReadBounds) -> DataFrame:
    num_partitions = plan_partitions(table, bounds)
    dbtable = f"({build_select(table, predicate)}) AS src"
    reader = spark.read.format("jdbc").options(**JDBC_OPTIONS).option("dbtable", dbtable)

    if num_partitions > 1:
        log.info(
            "%s: parallel read, %d partitions on %s in [%d, %d] (~%d rows)",
            table.name,
            num_partitions,
            table.partition_column,
            bounds.lower,
            bounds.upper,
            bounds.row_count,
        )
        reader = (
            reader.option("partitionColumn", table.partition_column)
            .option("lowerBound", str(bounds.lower))
            # upperBound is exclusive in Spark's stride arithmetic only in the sense that
            # rows above it all land in the last partition; +1 keeps the top key balanced.
            .option("upperBound", str(bounds.upper + 1))
            .option("numPartitions", str(num_partitions))
        )
    else:
        log.info(
            "%s: single-partition read (%s)",
            table.name,
            "no numeric partition column"
            if not table.partition_column
            else "below parallel threshold or degenerate bounds",
        )
    return with_retry(reader.load, description=f"JDBC read of {table.source_qualified}")


def add_audit_columns(df: DataFrame, table: SourceTable) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.lit(RUN_TS).cast("timestamp"))
        .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("_source_table", F.lit(f"{SOURCE_DATABASE}.{table.source_qualified}"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental watermark
# MAGIC
# MAGIC The high-water mark is read back from the bronze table itself (`MAX(<watermark>)`),
# MAGIC so the pipeline holds no external state and a re-run after a failure resumes from
# MAGIC whatever actually landed. The `incremental_start` widget overrides it for backfills.
# MAGIC
# MAGIC The predicate is `>=`, not `>`: same-timestamp rows are re-read and de-duplicated by
# MAGIC the primary-key MERGE, which is cheaper than risking a silently dropped row.

# COMMAND ----------


def table_exists(fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except AnalysisException:
        return False


def current_watermark(table: SourceTable, target_fqn: str) -> Optional[str]:
    if INCREMENTAL_START:
        log.info("%s: watermark overridden by widget -> %s", table.name, INCREMENTAL_START)
        return INCREMENTAL_START
    if not table_exists(target_fqn):
        log.info("%s: %s does not exist yet, incremental run falls back to a full read", table.name, target_fqn)
        return None
    row = spark.sql(f"SELECT MAX({table.watermark_column}) AS hwm FROM {target_fqn}").collect()[0]
    hwm = row["hwm"]
    if hwm is None:
        log.info("%s: bronze table is empty, incremental run falls back to a full read", table.name)
        return None
    return str(hwm)


def incremental_predicate(table: SourceTable, target_fqn: str) -> Optional[str]:
    if LOAD_MODE != "incremental" or not table.watermark_column:
        return None
    hwm = current_watermark(table, target_fqn)
    if hwm is None:
        return None
    # Literal is a quoted ISO timestamp; SQL Server parses it against the column type.
    safe = hwm.replace("'", "''")
    return f"{table.watermark_column} >= '{safe}'"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing
# MAGIC
# MAGIC * `full_refresh` -> `overwrite` with `overwriteSchema` **off**. A genuine source schema
# MAGIC   change must be an explicit, reviewed act, not an accident of a nightly run.
# MAGIC * `incremental` -> `MERGE` on the manifest primary key, so re-running a window is a
# MAGIC   no-op rather than a duplicate. Tables without a watermark are fully refreshed even in
# MAGIC   incremental mode (logged), because there is no correct way to append them.
# MAGIC
# MAGIC `mergeSchema` is off on every write path.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")


def ensure_schema() -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")


def write_full_refresh(df: DataFrame, target_fqn: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .option("overwriteSchema", "false")
        .saveAsTable(target_fqn)
    )


def write_merge(df: DataFrame, table: SourceTable, target_fqn: str) -> None:
    if not table_exists(target_fqn):
        log.info("%s: target missing, first incremental load writes the table outright", table.name)
        write_full_refresh(df, target_fqn)
        return

    staging = f"_bronze_stage_{table.name}"
    # Dedup inside the batch: MERGE fails if the source matches a target row more than once.
    ordering = F.col(table.watermark_column).desc() if table.watermark_column else F.lit(1)
    deduped = (
        df.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(*[F.col(c) for c in table.primary_key]).orderBy(ordering)
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    deduped.createOrReplaceTempView(staging)

    on_clause = " AND ".join(f"t.{c} = s.{c}" for c in table.primary_key)
    spark.sql(
        f"""
        MERGE INTO {target_fqn} AS t
        USING {staging} AS s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(staging)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-table driver

# COMMAND ----------


def ingest_table(table: SourceTable) -> Dict[str, object]:
    target_fqn = table.target(CATALOG, BRONZE_SCHEMA)
    started = time.time()
    log.info("=== %s -> %s (mode=%s)", table.source_qualified, target_fqn, LOAD_MODE)

    if LOAD_MODE == "incremental" and not table.watermark_column:
        log.info(
            "%s has no watermark column in the source (%s); falling back to full refresh.",
            table.name,
            table.notes,
        )

    if DRY_RUN:
        log.info("%s: dry run, nothing read or written", table.name)
        return {"table": table.name, "target": target_fqn, "status": "skipped_dry_run"}

    assert_source_table_exists(table)
    assert_source_columns(table)

    predicate = incremental_predicate(table, target_fqn)
    if predicate:
        log.info("%s: incremental predicate -> %s", table.name, predicate)

    bounds = probe_bounds(table, predicate)
    if bounds.row_count == 0:
        log.info("%s: source returned 0 rows for this window, nothing to do", table.name)
        return {"table": table.name, "target": target_fqn, "status": "no_new_rows", "rows": 0}

    df = add_audit_columns(read_source(table, predicate, bounds), table)

    def _write() -> None:
        if predicate and table.watermark_column:
            write_merge(df, table, target_fqn)
        else:
            write_full_refresh(df, target_fqn)

    with_retry(_write, description=f"write of {target_fqn}", attempts=2, base_delay=10.0)

    rows = spark.table(target_fqn).count()
    elapsed = time.time() - started
    log.info("%s: done in %.1fs, %s now holds %d rows", table.name, elapsed, target_fqn, rows)
    return {
        "table": table.name,
        "target": target_fqn,
        "status": "merged" if predicate else "overwritten",
        "source_rows": bounds.row_count,
        "target_rows": rows,
        "seconds": round(elapsed, 1),
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------


def selected_tables() -> List[SourceTable]:
    if not TABLE_SUBSET:
        return list(MANIFEST)
    unknown = [t for t in TABLE_SUBSET if t not in MANIFEST_BY_NAME]
    if unknown:
        raise ValueError(
            f"Unknown table(s) in the 'tables' widget: {unknown}. "
            f"Manifest contains: {sorted(MANIFEST_BY_NAME)}."
        )
    return [MANIFEST_BY_NAME[t] for t in TABLE_SUBSET]


tables = selected_tables()
log.info(
    "env=%s catalog=%s schema=%s source=%s mode=%s tables=%s dry_run=%s",
    ENV,
    CATALOG,
    BRONZE_SCHEMA,
    SOURCE_DATABASE,
    LOAD_MODE,
    [t.name for t in tables],
    DRY_RUN,
)

if not DRY_RUN:
    ensure_schema()

results: List[Dict[str, object]] = []
failures: List[str] = []
for source_table in tables:
    try:
        results.append(ingest_table(source_table))
    except Exception as exc:  # noqa: BLE001 - collect all failures, report at the end
        log.error("%s: FAILED - %s", source_table.name, exc)
        failures.append(f"{source_table.name}: {exc}")
        results.append({"table": source_table.name, "status": "failed", "error": str(exc)})

for result in results:
    log.info("%s", result)

if failures:
    raise RuntimeError(
        "Bronze SQL Server ingestion failed for "
        f"{len(failures)}/{len(tables)} table(s):\n  - " + "\n  - ".join(failures)
    )

# COMMAND ----------

dbutils.notebook.exit(
    str(
        {
            "source_system": SOURCE_SYSTEM,
            "run_ts": RUN_TS.isoformat(),
            "load_mode": LOAD_MODE,
            "results": results,
        }
    )
)
