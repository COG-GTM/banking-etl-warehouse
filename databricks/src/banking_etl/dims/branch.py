"""Port of the Talend job ``Load_DimBranch``.

Legacy job (talend_jobs/Load_DimBranch.zip, ``Load_DimBranch_0.1.item``)::

    tMSSqlInput  sample: SELECT dbo.branch.branch_id, dbo.branch.branch_name,
                                dbo.branch.branch_location FROM dbo.branch
    tMap         1:1 rename  branch_id -> BranchID, branch_name -> BranchName,
                             branch_location -> BranchLocation
    tMSSqlOutput DWH.dbo.DimBranch  DATA_ACTION=INSERT  DIE_ON_ERROR=false

Databricks flow::

    bronze.sqlserver_branch -> silver.branch (typed, trimmed, deduped on branch_id)
                            -> MERGE (SCD-1 upsert on branch_id) -> gold.dim_branch

This module also holds the small generic helpers (table resolution, dedupe,
SCD-1 MERGE) reused by :mod:`banking_etl.dims.account`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

DEFAULT_CATALOG = "migration_demo"
DEFAULT_SCHEMA_PREFIX = "banking_mig_"

# Bronze ingestion metadata columns that, when present, order duplicates (latest wins).
_INGEST_ORDER_CANDIDATES = (
    "_ingested_at",
    "_ingest_ts",
    "_ingestion_ts",
    "_load_ts",
    "_loaded_at",
    "ingested_at",
)

BRANCH_KEY = "branch_id"
BRANCH_COLUMNS = ("branch_id", "branch_name", "branch_location")
GOLD_DIM_BRANCH_DDL = """
    branch_id INT NOT NULL COMMENT 'Legacy DimBranch.BranchID (business key, PK)',
    branch_name STRING COMMENT 'Legacy DimBranch.BranchName VARCHAR(100)',
    branch_location STRING COMMENT 'Legacy DimBranch.BranchLocation VARCHAR(255)'
"""


@dataclass(frozen=True)
class Layers:
    """Fully-qualified schema names for the medallion layers."""

    bronze: str
    silver: str
    gold: str

    def table(self, layer: str, name: str) -> str:
        return f"{getattr(self, layer)}.{name}"


def resolve_layers(
    catalog: Optional[str] = DEFAULT_CATALOG,
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX,
    bronze_schema: Optional[str] = None,
    silver_schema: Optional[str] = None,
    gold_schema: Optional[str] = None,
) -> Layers:
    """Resolve ``<catalog>.<prefix><layer>`` schema names.

    ``catalog`` may be empty/None for a two-part namespace (local Spark). Any of
    the ``*_schema`` overrides replaces the derived schema name, e.g. to point
    every layer at a ticket-scoped schema such as ``banking_mig_t5``.
    """

    def qualify(layer: str, override: Optional[str]) -> str:
        schema = override or f"{schema_prefix}{layer}"
        return f"{catalog}.{schema}" if catalog else schema

    return Layers(
        bronze=qualify("bronze", bronze_schema),
        silver=qualify("silver", silver_schema),
        gold=qualify("gold", gold_schema),
    )


def try_cast(column: str, sql_type: str) -> Column:
    # Column.try_cast fails on serverless Spark Connect; the SQL form works everywhere.
    return F.expr(f"try_cast(`{column}` AS {sql_type})")


def trimmed(column: str) -> Column:
    return F.trim(F.col(column).cast("string"))


def dedupe_on_key(df: DataFrame, key: str, columns: Sequence[str]) -> DataFrame:
    """Keep one row per business key, dropping rows whose key is NULL.

    Legacy Talend inserts into a PK column, so NULL keys and repeat keys were
    rejected (DIE_ON_ERROR=false). Here the latest row by bronze ingestion
    metadata wins when such a column exists; otherwise the order is made
    deterministic over the business columns.
    """
    order_cols = [c for c in _INGEST_ORDER_CANDIDATES if c in df.columns]
    order: list = [F.col(c).desc_nulls_last() for c in order_cols]
    order += [F.col(c).asc_nulls_last() for c in columns if c != key]
    window = Window.partitionBy(key).orderBy(*order)
    return (
        df.where(F.col(key).isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .select(*columns)
    )


def write_silver(df: DataFrame, table: str) -> int:
    """Full-refresh the silver snapshot (idempotent)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    return df.sparkSession.table(table).count()


def ensure_table(spark: SparkSession, table: str, ddl: str, comment: str) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA COMMENT '{comment}'"
    )


def _last_operation_metrics(spark: SparkSession, table: str) -> Dict[str, int]:
    row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]
    metrics = row["operationMetrics"] or {}
    wanted = (
        "numSourceRows",
        "numTargetRowsInserted",
        "numTargetRowsUpdated",
        "numTargetRowsDeleted",
        "numTargetRowsCopied",
    )
    return {k: int(metrics[k]) for k in wanted if k in metrics}


def scd1_merge(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    key: str,
    columns: Iterable[str],
) -> Dict[str, int]:
    """SCD-1 upsert ``source_table`` into ``target_table`` on ``key``.

    Matched rows are updated only when a non-key column actually changed
    (null-safe), new keys are inserted, and target rows missing from the source
    are kept (the legacy job never deleted from the DWH).
    """
    columns = list(columns)
    non_key = [c for c in columns if c != key]
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in non_key)
    set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in non_key)
    insert_cols = ", ".join(f"`{c}`" for c in columns)
    insert_vals = ", ".join(f"s.`{c}`" for c in columns)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING {source_table} AS s
        ON t.`{key}` = s.`{key}`
        WHEN MATCHED AND ({changed}) THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )
    return _last_operation_metrics(spark, target_table)


def transform_silver_branch(bronze: DataFrame) -> DataFrame:
    """bronze.sqlserver_branch -> silver.branch columns (typed, trimmed, deduped)."""
    typed = bronze.select(
        try_cast("branch_id", "INT").alias("branch_id"),
        trimmed("branch_name").alias("branch_name"),
        trimmed("branch_location").alias("branch_location"),
        *[F.col(c) for c in _INGEST_ORDER_CANDIDATES if c in bronze.columns],
    )
    return dedupe_on_key(typed, BRANCH_KEY, BRANCH_COLUMNS)


def build_silver_branch(spark: SparkSession, layers: Layers) -> int:
    bronze = spark.table(layers.table("bronze", "sqlserver_branch"))
    return write_silver(transform_silver_branch(bronze), layers.table("silver", "branch"))


def merge_gold_dim_branch(spark: SparkSession, layers: Layers) -> Dict[str, int]:
    target = layers.table("gold", "dim_branch")
    ensure_table(spark, target, GOLD_DIM_BRANCH_DDL, "Port of DWH.dbo.DimBranch (SCD-1)")
    metrics = scd1_merge(
        spark, layers.table("silver", "branch"), target, BRANCH_KEY, BRANCH_COLUMNS
    )
    metrics["gold_rows"] = spark.table(target).count()
    return metrics


def run(spark: SparkSession, layers: Layers, steps: Sequence[str] = ("silver", "gold")) -> Dict:
    result: Dict = {"entity": "branch"}
    if "silver" in steps:
        result["silver_rows"] = build_silver_branch(spark, layers)
    if "gold" in steps:
        result["gold_merge"] = merge_gold_dim_branch(spark, layers)
    return result
