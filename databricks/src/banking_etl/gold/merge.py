"""Generic SCD-1 MERGE into a Delta table (used by the dim loads, tickets 5/6/7)."""
from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

IDENTITY_METADATA_KEY = "delta.identity.start"
_IDENTITY_DDL = re.compile(
    r"^\s*(?:`([^`]+)`|(\w+))\s+\w+\s+GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY",
    re.IGNORECASE | re.MULTILINE,
)


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def identity_columns(spark: SparkSession, table: str) -> list[str]:
    """Columns of ``table`` declared ``GENERATED ... AS IDENTITY``.

    Delta strips identity metadata from read schemas, and UC ``information_schema`` does not flag
    Delta identity columns, so this parses ``SHOW CREATE TABLE`` (Databricks) and falls back to the
    Delta log for local (``file:``) OSS Delta tables, where ``SHOW CREATE TABLE`` is unsupported.
    """
    found = [f.name for f in spark.table(table).schema.fields if IDENTITY_METADATA_KEY in f.metadata]
    if found:
        return found
    try:
        ddl = spark.sql(f"SHOW CREATE TABLE {table}").first()[0]
    except Exception:  # noqa: BLE001 - OSS Delta: "SHOW CREATE TABLE is not supported for Delta tables"
        return _identity_from_local_delta_log(spark, table)
    return [m.group(1) or m.group(2) for m in _IDENTITY_DDL.finditer(ddl)]


def _identity_from_local_delta_log(spark: SparkSession, table: str) -> list[str]:
    location = spark.sql(f"DESCRIBE DETAIL {table}").first()["location"]
    if not location or not location.startswith("file:"):
        return []
    schema_string = None
    for path in sorted(Path(urlparse(location).path, "_delta_log").glob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            action = json.loads(line)
            if "metaData" in action:
                schema_string = action["metaData"]["schemaString"]
    if schema_string is None:
        return []
    return [f["name"] for f in json.loads(schema_string)["fields"] if IDENTITY_METADATA_KEY in f.get("metadata", {})]


def _last_merge_metrics(spark: SparkSession, table: str) -> dict[str, int]:
    from delta.tables import DeltaTable

    row = DeltaTable.forName(spark, table).history(1).select("operation", "operationMetrics").first()
    metrics = (row["operationMetrics"] or {}) if row and row["operation"] == "MERGE" else {}
    keys = ("numSourceRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")
    return {k: int(metrics.get(k, 0)) for k in keys}


def merge_scd1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_cols: Sequence[str],
    update_cols: Sequence[str] | None = None,
) -> dict[str, int]:
    """SCD-1 upsert of ``source_df`` into Delta ``target_table``.

    - Matches on the natural ``key_cols``; ``source_df`` must be unique and non-null on them.
    - Updates a matched row only when at least one ``update_cols`` value differs (null-safe ``<=>``);
      unchanged rows are not rewritten, so re-running with the same source is a no-op.
    - Inserts unmatched source rows with ``key_cols + update_cols``; identity (surrogate key)
      columns are never written, so Delta generates them and they stay stable across runs.
    - Never deletes target rows missing from the source.

    ``update_cols`` defaults to every source column that is not a key. Source columns are cast to
    the target column types. Returns the MERGE operation metrics
    (``numSourceRows``, ``numTargetRowsInserted``, ``numTargetRowsUpdated``, ``numTargetRowsDeleted``).
    """
    from delta.tables import DeltaTable

    keys = list(key_cols)
    if not keys:
        raise ValueError("key_cols must not be empty")
    updates = [c for c in source_df.columns if c not in keys] if update_cols is None else list(update_cols)
    overlap = set(keys) & set(updates)
    if overlap:
        raise ValueError(f"columns cannot be both key and update columns: {sorted(overlap)}")

    target_types = {f.name: f.dataType for f in spark.table(target_table).schema.fields}
    written = keys + updates
    missing_source = [c for c in written if c not in source_df.columns]
    missing_target = [c for c in written if c not in target_types]
    if missing_source:
        raise ValueError(f"columns missing from source_df: {missing_source}")
    if missing_target:
        raise ValueError(f"columns missing from {target_table}: {missing_target}")
    identity = set(identity_columns(spark, target_table)) & set(written)
    if identity:
        raise ValueError(f"identity columns must not be written by merge_scd1: {sorted(identity)}")

    source = source_df.select([F.col(_quote(c)).cast(target_types[c]).alias(c) for c in written])

    null_key = F.lit(False)
    for k in keys:
        null_key = null_key | F.col(_quote(k)).isNull()
    check = source.agg(
        F.sum(F.when(null_key, 1).otherwise(0)).alias("null_keys"),
        (F.count(F.lit(1)) - F.countDistinct(*[F.col(_quote(k)) for k in keys])).alias("dup_keys"),
    ).first()
    if check["null_keys"]:
        raise ValueError(f"source_df has {check['null_keys']} row(s) with NULL in key columns {keys}")
    if check["dup_keys"]:
        raise ValueError(f"source_df has {check['dup_keys']} duplicate row(s) on key columns {keys}")

    on = " AND ".join(f"t.{_quote(k)} = s.{_quote(k)}" for k in keys)
    merge = DeltaTable.forName(spark, target_table).alias("t").merge(source.alias("s"), on)
    if updates:
        changed = " OR ".join(f"NOT (t.{_quote(c)} <=> s.{_quote(c)})" for c in updates)
        merge = merge.whenMatchedUpdate(condition=changed, set={_quote(c): f"s.{_quote(c)}" for c in updates})
    merge.whenNotMatchedInsert(values={_quote(c): f"s.{_quote(c)}" for c in written}).execute()
    return _last_merge_metrics(spark, target_table)
