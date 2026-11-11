"""Port of the Talend job ``Load_DimBranch`` (talend_jobs/Load_DimBranch.zip, v0.1).

Talend flow: ``tDBInput_1`` (MSSQL ``sample.dbo.branch``) -> ``tMap_1`` -> ``tDBOutput_1``
(MSSQL ``DWH.DimBranch``). Rules reproduced here:

R1 Source      ``SELECT branch_id, branch_name, branch_location FROM dbo.branch`` (no WHERE),
               read from ``<catalog>.<bronze_schema>.branch``; only these three columns are used.
R2 Not-null key ``branch_id`` is ``nullable="false"`` in the input schema; Talend aborts the job
               with "Null value in non-Nullable column", so a NULL key raises ``ValueError``.
R3 No trimming ``TRIM_ALL_COLUMN=false`` and every ``TRIM_COLUMN`` entry is false: values are
               passed through untouched (whitespace and NULLs preserved).
R4 tMap        Pure 1:1 mapping, no filter, no expressions:
               branch_id -> BranchID (INT), branch_name -> BranchName, branch_location -> BranchLocation
               (gold: ``branch_id``, ``branch_name``, ``branch_location``).
R5 Load mode   ``TABLE_ACTION=CREATE_IF_NOT_EXISTS`` + ``DATA_ACTION=INSERT`` into DimBranch
               (``BranchID INT PRIMARY KEY, BranchName VARCHAR(100), BranchLocation VARCHAR(255)``)
               with ``DIE_ON_ERROR=false`` and no REJECT link. A row whose key already exists
               (in the table or earlier in the same batch) or whose value overflows a VARCHAR width
               fails its INSERT and is silently skipped; existing rows are never updated.
               Reproduced as ``CREATE TABLE IF NOT EXISTS`` + ``MERGE ... WHEN NOT MATCHED THEN INSERT``
               after dropping over-width rows and in-batch duplicate keys, which makes re-runs
               idempotent. ``update_existing=True`` opts in to an SCD-1 refresh of changed rows
               (not Talend behaviour).
"""

from __future__ import annotations

import argparse
import re
import uuid
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SOURCE_TABLE = "branch"
TARGET_TABLE = "dim_branch"
KEY_COLUMN = "branch_id"

# Talend tMap output column -> (source column, gold column)
COLUMN_MAPPING = (
    ("BranchID", "branch_id", "branch_id"),
    ("BranchName", "branch_name", "branch_name"),
    ("BranchLocation", "branch_location", "branch_location"),
)

# DimBranch VARCHAR widths from sql_scripts/01_create_tables.sql
MAX_LENGTHS = {"branch_name": 100, "branch_location": 255}

TARGET_DDL_COLUMNS = (
    "branch_id INT NOT NULL, branch_name STRING, branch_location STRING"
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class LoadStats:
    source_rows: int
    rejected_width: int
    rejected_duplicate_key: int
    inserted: int
    updated: int


def _quote(identifier: str) -> str:
    if not _IDENTIFIER.match(identifier):
        raise ValueError(f"Invalid identifier: {identifier!r}")
    return f"`{identifier}`"


def table_name(catalog: str | None, schema: str, table: str) -> str:
    parts = ([catalog] if catalog else []) + [schema, table]
    return ".".join(_quote(p) for p in parts)


def read_source(spark: SparkSession, source_table: str) -> DataFrame:
    """R1: projection of the three columns used by the Talend input query."""
    return spark.table(source_table).select(*[src for _, src, _ in COLUMN_MAPPING])


def transform(source: DataFrame) -> DataFrame:
    """R2-R4: enforce the non-nullable key, then map 1:1 with no trim/filter/expression."""
    if source.filter(F.col(KEY_COLUMN).isNull()).limit(1).count() > 0:
        raise ValueError("Null value in non-Nullable column: branch_id")
    return source.select(
        F.col("branch_id").cast("int").alias("branch_id"),
        F.col("branch_name").cast("string").alias("branch_name"),
        F.col("branch_location").cast("string").alias("branch_location"),
    )


def split_rejects(mapped: DataFrame) -> tuple[DataFrame, int, int, int]:
    """R5: drop rows the SQL Server INSERT would reject (width overflow, duplicate key in batch).

    Returns (loadable rows, input count, over-width count, duplicate-key count).
    """
    fits = F.lit(True)
    for column, width in MAX_LENGTHS.items():
        fits = fits & (F.col(column).isNull() | (F.length(F.col(column)) <= width))
    flagged = mapped.withColumn("_fits", fits)
    within_width = flagged.filter("_fits").drop("_fits")

    first_per_key = Window.partitionBy(KEY_COLUMN).orderBy(
        F.col("branch_name").asc_nulls_last(), F.col("branch_location").asc_nulls_last()
    )
    deduped = (
        within_width.withColumn("_rn", F.row_number().over(first_per_key))
        .filter("_rn = 1")
        .drop("_rn")
    )

    total = mapped.count()
    width_ok = within_width.count()
    loadable = deduped.count()
    return deduped, total, total - width_ok, width_ok - loadable


def ensure_target(spark: SparkSession, target_table: str) -> None:
    """R5: TABLE_ACTION=CREATE_IF_NOT_EXISTS."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} ({TARGET_DDL_COLUMNS}) USING DELTA")


def merge_into_target(
    spark: SparkSession, rows: DataFrame, target_table: str, update_existing: bool = False
) -> tuple[int, int]:
    """R5: insert-only MERGE on branch_id (optional SCD-1 update). Returns (inserted, updated)."""
    view = f"dim_branch_src_{uuid.uuid4().hex}"
    rows.createOrReplaceTempView(view)
    update_clause = ""
    if update_existing:
        update_clause = (
            "WHEN MATCHED AND NOT (t.branch_name <=> s.branch_name "
            "AND t.branch_location <=> s.branch_location) THEN UPDATE SET "
            "t.branch_name = s.branch_name, t.branch_location = s.branch_location "
        )
    try:
        result = spark.sql(
            f"MERGE INTO {target_table} AS t USING {view} AS s "
            f"ON t.branch_id = s.branch_id "
            f"{update_clause}"
            "WHEN NOT MATCHED THEN INSERT (branch_id, branch_name, branch_location) "
            "VALUES (s.branch_id, s.branch_name, s.branch_location)"
        ).first()
    finally:
        spark.catalog.dropTempView(view)
    metrics = result.asDict() if result is not None else {}
    return int(metrics.get("num_inserted_rows", 0)), int(metrics.get("num_updated_rows", 0))


def run(
    spark: SparkSession,
    catalog: str | None,
    bronze_schema: str = "bronze",
    gold_schema: str = "gold",
    update_existing: bool = False,
) -> LoadStats:
    source_table = table_name(catalog, bronze_schema, SOURCE_TABLE)
    target_table = table_name(catalog, gold_schema, TARGET_TABLE)

    mapped = transform(read_source(spark, source_table))
    loadable, source_rows, rejected_width, rejected_dup = split_rejects(mapped)

    ensure_target(spark, target_table)
    inserted, updated = merge_into_target(spark, loadable, target_table, update_existing)
    return LoadStats(source_rows, rejected_width, rejected_dup, inserted, updated)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load gold.dim_branch (Talend Load_DimBranch)")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument(
        "--update-existing",
        default="false",
        choices=["true", "false"],
        help="Also update changed existing rows (SCD-1). Talend's INSERT mode never does.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> LoadStats:
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    stats = run(
        spark,
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        gold_schema=args.gold_schema,
        update_existing=args.update_existing == "true",
    )
    print(f"load_dim_branch: {asdict(stats)}")
    return stats


if __name__ == "__main__":
    main()
