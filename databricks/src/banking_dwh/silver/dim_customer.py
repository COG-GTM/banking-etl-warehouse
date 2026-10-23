"""Silver ``dim_customer`` load (Ticket 7): port of Talend ``Load_DimCustomer``.

Talend job (``talend_jobs/Load_DimCustomer.zip`` -> ``process/Load_DimCustomer_0.1.item``):

* ``tDBInput_1`` (``customer``) is the tMap main flow; ``tDBInput_2`` (``city``) and
  ``tDBInput_3`` (``state``) are lookups joined on ``row1.city_id`` and ``row2.state_id``.
  Neither lookup sets ``innerJoin="true"``, so both are LEFT OUTER joins: a customer with an
  unknown city still loads, with null ``city_name`` / ``state_name``.
* ``StringHandling.UPCASE`` is applied to ``customer_name``, ``address`` and ``gender`` only.
  ``age``, ``email``, ``city_name`` and ``state_name`` are copied unchanged. No trimming
  (``TRIM_ALL_COLUMN=false`` and every ``TRIM_COLUMN`` entry is false on all three inputs).
* ``age`` is ``varchar(3)`` in the source and ``INT`` in ``DimCustomer``; non-numeric values
  become NULL here (``try_cast``).
* ``tDBOutput_1`` INSERTs into ``DimCustomer`` with ``DIE_ON_ERROR=false``.

Documented deviations: the load MERGEs on ``customer_id`` (SCD-1 upsert) instead of a plain
INSERT, so reruns are idempotent and source changes are applied rather than rejected as PK
violations. Bronze is append-only, so each source is first deduplicated on its key (latest
``_ingested_at``, then ``_batch_id``). A non-numeric ``age`` loads as NULL where SQL Server's
implicit conversion would have rejected the row. Rows deleted at source are not deleted
(Talend never deleted either).
"""

from __future__ import annotations

import csv
import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..common.config import EnvConfig, validate_identifier
from ..contracts.ddl import create_table_sql
from ..contracts.tables import (
    BRONZE_SQLSERVER_CITY,
    BRONZE_SQLSERVER_CUSTOMER,
    BRONZE_SQLSERVER_STATE,
    DIM_CUSTOMER,
    TableContract,
)

UPPERCASED_COLUMNS = ("customer_name", "address", "gender")
KEY = DIM_CUSTOMER.primary_key[0]
FIXTURE_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "t7_sample_db"


def latest_per_key(df, key: str):
    """Keep one row per ``key``: the most recently ingested (Bronze is append-only)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = [F.col(c).desc_nulls_last() for c in ("_ingested_at", "_batch_id") if c in df.columns]
    # Deterministic tie-break when two versions share _ingested_at/_batch_id.
    order += [F.col(c).asc_nulls_last() for c in df.columns if c != key and not c.startswith("_")]
    if not order:
        return df.dropDuplicates([key])
    w = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def transform(customer, city, state):
    """tMap ``to_DimCustomer``: customer LEFT JOIN city LEFT JOIN state, contract column order."""
    from pyspark.sql import functions as F

    c = latest_per_key(customer, "customer_id").alias("c")
    ci = latest_per_key(city, "city_id").select("city_id", "city_name", "state_id").alias("ci")
    s = latest_per_key(state, "state_id").select("state_id", "state_name").alias("s")
    joined = (
        c.join(ci, F.col("c.city_id") == F.col("ci.city_id"), "left")
        .join(s, F.col("ci.state_id") == F.col("s.state_id"), "left")
    )
    exprs = {
        "customer_id": F.col("c.customer_id").cast("int"),
        "customer_name": F.upper(F.col("c.customer_name")),
        "address": F.upper(F.col("c.address")),
        "city_name": F.col("ci.city_name"),
        "state_name": F.col("s.state_name"),
        # Spark Connect (serverless) has no Column.try_cast; the SQL function works everywhere.
        "age": F.expr("try_cast(c.age AS INT)"),
        "gender": F.upper(F.col("c.gender")),
        "email": F.col("c.email"),
        "_loaded_at": F.current_timestamp(),
    }
    return joined.select(*[exprs[name].alias(name) for name in DIM_CUSTOMER.column_names])


def ensure_target(spark, cfg: EnvConfig, *, unity_catalog: bool = True) -> str:
    spark.sql(create_table_sql(cfg, DIM_CUSTOMER, unity_catalog=unity_catalog))
    return cfg.table("silver", DIM_CUSTOMER.name)


def merge_sql(target: str, source_view: str) -> str:
    validate_identifier(source_view, "source view")
    business = [c for c in DIM_CUSTOMER.business_columns if c != KEY]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in business)
    cols = DIM_CUSTOMER.column_names
    return (
        f"MERGE INTO {target} AS t\nUSING {source_view} AS s\nON t.{KEY} = s.{KEY}\n"
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET "
        + ", ".join(f"t.{c} = s.{c}" for c in cols)
        + f"\nWHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
    )


def _last_merge_metrics(spark, target: str) -> dict[str, int]:
    row = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]
    m = row["operationMetrics"] or {}
    if row["operation"] != "MERGE":
        return {}
    keys = ("numSourceRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")
    return {k: int(m.get(k, 0)) for k in keys}


def load(spark, cfg: EnvConfig, *, unity_catalog: bool = True) -> dict[str, Any]:
    """Read the Bronze customer/city/state contracts and MERGE into ``silver.dim_customer``."""
    from pyspark.sql import functions as F

    customer = spark.table(cfg.table("bronze", BRONZE_SQLSERVER_CUSTOMER.name))
    city = spark.table(cfg.table("bronze", BRONZE_SQLSERVER_CITY.name))
    state = spark.table(cfg.table("bronze", BRONZE_SQLSERVER_STATE.name))
    target = ensure_target(spark, cfg, unity_catalog=unity_catalog)

    src = transform(customer, city, state)
    view = f"t7_dim_customer_src_{uuid.uuid4().hex[:12]}"
    src.createOrReplaceTempView(view)
    try:
        stats = src.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("city_name").isNull(), 1).otherwise(0)).alias("null_city"),
            F.sum(F.when(F.col("state_name").isNull(), 1).otherwise(0)).alias("null_state"),
        ).collect()[0]
        spark.sql(merge_sql(target, view))
    finally:
        spark.catalog.dropTempView(view)

    return {
        "target": target,
        "bronze_rows": {
            "customer": customer.count(), "city": city.count(), "state": state.count(),
        },
        "source_rows_after_dedup": int(stats["rows"]),
        "customers_without_city": int(stats["null_city"] or 0),
        "customers_without_state": int(stats["null_state"] or 0),
        "merge": _last_merge_metrics(spark, target),
        "target_rows": spark.table(target).count(),
    }


def run_dq(spark, cfg: EnvConfig, run_id: str, *, fail_on_error: bool = True) -> list[dict]:
    """Ticket 2 PK checks (``pk_not_null`` / ``pk_unique``) for dim_customer -> ops.dq_results."""
    from ..dq.checks import assert_all_passed, primary_key_checks, results_as_dicts, write_results

    results = primary_key_checks(spark, cfg, tables=(DIM_CUSTOMER,))
    write_results(spark, cfg, results, run_id)
    if fail_on_error:
        assert_all_passed(results)
    return results_as_dicts(results)


# ------------------------------------------------------------------ fixture seeding
_FIXTURE_TABLES: tuple[tuple[str, TableContract], ...] = (
    ("customer", BRONZE_SQLSERVER_CUSTOMER),
    ("city", BRONZE_SQLSERVER_CITY),
    ("state", BRONZE_SQLSERVER_STATE),
)


def _protected_namespaces() -> tuple[set[tuple[str, str]], set[str]]:
    """(catalog, schema_prefix) of every configured target, and the catalogs of targets that own one."""
    from ..common.config import load_raw_config

    targets = load_raw_config()["targets"].values()
    shared = {(t["catalog"], t.get("schema_prefix", "")) for t in targets}
    owned_catalogs = {t["catalog"] for t in targets if t.get("create_catalog") or not t.get("schema_prefix")}
    return shared, owned_catalogs


def assert_scratch_namespace(cfg: EnvConfig) -> None:
    """Fixture seeding overwrites Bronze tables: only allow a ticket scratch namespace."""
    shared, owned_catalogs = _protected_namespaces()
    where = cfg.schema("bronze")
    if cfg.target == "prod" or cfg.catalog in owned_catalogs:
        raise ValueError(f"Refusing to seed Bronze from fixtures in production namespace {where}.")
    if not cfg.schema_prefix or cfg.schema_prefix in {prefix for _, prefix in shared}:
        raise ValueError(
            f"Refusing to seed Bronze in the shared namespace {where}; "
            "use a ticket scratch schema_prefix (e.g. bdwh_t7_)."
        )


def read_fixture(name: str, contract: TableContract, fixture_dir: Path | str = FIXTURE_DIR) -> list[tuple]:
    """Rows of ``<fixture_dir>/<name>.csv`` cast to the contract's business column types."""
    casts = {"INT": lambda v: None if v == "" else int(v), "STRING": lambda v: None if v == "" else v}
    cols = [c for c in contract.columns if not c.name.startswith("_")]
    with open(Path(fixture_dir) / f"{name}.csv", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [tuple(casts[c.type](r[c.name]) for c in cols) for r in reader]


def seed_bronze_from_fixture(
    spark, cfg: EnvConfig, fixture_dir: Path | str = FIXTURE_DIR, *, unity_catalog: bool = True
) -> dict[str, int]:
    """Overwrite this run's Bronze customer/city/state with the sample.bak extract.

    Only for local tests and the ticket's scratch prefix: refuses the target's shared prefix
    (those Bronze tables belong to Ticket 3) and never touches silver/gold.
    """
    assert_scratch_namespace(cfg)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    batch_id = f"t7-fixture-{now:%Y%m%dT%H%M%S}"
    counts = {}
    for name, contract in _FIXTURE_TABLES:
        rows = [(*r, now, batch_id) for r in read_fixture(name, contract, fixture_dir)]
        spark.sql(create_table_sql(cfg, contract, unity_catalog=unity_catalog))
        df = spark.createDataFrame(rows, contract.ddl_schema())
        df.write.format("delta").mode("overwrite").saveAsTable(cfg.table("bronze", contract.name))
        counts[contract.name] = len(rows)
    return counts


def parity_with_fixture(spark, cfg: EnvConfig, fixture_dir: Path | str = FIXTURE_DIR) -> dict[str, Any]:
    """Compare silver.dim_customer with the legacy DimCustomer result in ``expected_dim_customer.csv``."""
    with open(Path(fixture_dir) / "expected_dim_customer.csv", newline="", encoding="utf-8") as fh:
        expected = {int(r[KEY]): r for r in csv.DictReader(fh)}
    cols = DIM_CUSTOMER.business_columns
    rows = spark.table(cfg.table("silver", DIM_CUSTOMER.name)).select(*cols).collect()
    got = {r[KEY]: r for r in rows}
    duplicates = sorted(k for k, n in Counter(r[KEY] for r in rows).items() if n > 1)

    def norm(row) -> tuple:
        return tuple(None if row[c] in (None, "") else str(row[c]) for c in cols)

    mismatched = sorted(k for k in expected.keys() & got.keys() if norm(expected[k]) != norm(got[k]))
    return {
        "expected_rows": len(expected),
        "actual_rows": len(rows),
        "missing": sorted(expected.keys() - got.keys()),
        "unexpected": sorted(got.keys() - expected.keys()),
        "duplicate_keys": duplicates,
        "mismatched": mismatched,
        "match": not mismatched and not duplicates and len(rows) == len(expected) and expected.keys() == got.keys(),
    }


def sample_rows(spark, cfg: EnvConfig, n: int = 5) -> list[dict]:
    df = spark.table(cfg.table("silver", DIM_CUSTOMER.name)).orderBy(KEY).limit(n)
    return [json.loads(json.dumps(r.asDict(), default=str)) for r in df.collect()]
