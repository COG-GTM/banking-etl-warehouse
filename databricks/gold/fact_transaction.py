"""PySpark port of the Talend job `Load_FactTransaction`.

Legacy flow (IDX_INTERNSHIP/process/Load_FactTransaction_0.1.item):

    tMSSqlInput (sample.dbo.transaction_db, mergeOrder 1) ─┐
    tFileInputExcel (transaction_excel.xlsx, mergeOrder 2) ─┼─> tUnite -> tUniqRow
    tFileInputDelimited (transaction_csv.csv, mergeOrder 3)─┘        (key: transaction_id)
        -> tMap (pass-through rename to PascalCase) -> tMSSqlOutput (DWH.FactTransaction)

Every function here is a pure transform over DataFrames so it can be tested with a
local SparkSession; the Databricks-only pieces (table reads, Delta MERGE, widgets)
live in `databricks/gold/jobs/load_fact_transaction.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import DEFAULT_TIMESTAMP_FORMAT, GoldConfig, TransactionSource

FACT_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
)

FACT_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), nullable=False),
        StructField("account_id", IntegerType(), nullable=True),
        StructField("transaction_date", TimestampType(), nullable=True),
        StructField("amount", DecimalType(19, 4), nullable=True),
        StructField("transaction_type", StringType(), nullable=True),
        StructField("branch_id", IntegerType(), nullable=True),
    ]
)

LINEAGE_COLUMNS: tuple[str, ...] = ("_source_name", "_source_priority", "_ingest_ts", "_source_file")

MERGE_KEY = "transaction_id"

# Applied after source priority so two rows with the same transaction_id always
# resolve to the same winner regardless of partitioning or file read order.
_TIEBREAK_COLUMNS: tuple[str, ...] = (
    "_ingest_ts",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
)


@dataclass(frozen=True)
class FactLoadResult:
    """Outputs of one fact build: the rows to merge plus the two side channels."""

    fact: DataFrame
    orphans: DataFrame
    duplicates: DataFrame


def _snake(name: str) -> str:
    out: list[str] = []
    for index, char in enumerate(name):
        if char.isupper() and index > 0 and not name[index - 1].isupper():
            out.append("_")
        out.append(char.lower())
    return "".join(out).replace("__", "_")


def normalize_source(
    df: DataFrame,
    source_name: str,
    priority: int,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT,
) -> DataFrame:
    """Cast one bronze transaction source onto the gold fact schema.

    Accepts snake_case or PascalCase bronze columns, parses `dd-MM-yyyy HH:mm:ss`
    strings, and keeps the ingest metadata needed for the deduplication tiebreak.
    """
    renamed = df
    for column in df.columns:
        target = _snake(column) if not column.startswith("_") else column
        if target != column:
            renamed = renamed.withColumnRenamed(column, target)

    available = set(renamed.columns)
    missing = [c for c in FACT_COLUMNS if c not in available]
    if missing:
        raise ValueError(f"source {source_name!r} is missing columns {missing}")

    projected = renamed.select(
        F.col("transaction_id").cast(IntegerType()).alias("transaction_id"),
        F.col("account_id").cast(IntegerType()).alias("account_id"),
        parse_transaction_date(renamed, "transaction_date", timestamp_format).alias(
            "transaction_date"
        ),
        F.col("amount").cast(DecimalType(19, 4)).alias("amount"),
        F.col("transaction_type").cast(StringType()).alias("transaction_type"),
        F.col("branch_id").cast(IntegerType()).alias("branch_id"),
        F.lit(source_name).alias("_source_name"),
        F.lit(priority).alias("_source_priority"),
        (
            F.col("_ingest_ts").cast(TimestampType())
            if "_ingest_ts" in available
            else F.lit(None).cast(TimestampType())
        ).alias("_ingest_ts"),
        (
            F.col("_source_file").cast(StringType())
            if "_source_file" in available
            else F.lit(None).cast(StringType())
        ).alias("_source_file"),
    )
    return projected


def parse_transaction_date(df: DataFrame, column: str, timestamp_format: str) -> Column:
    """Parse the legacy `dd-MM-yyyy HH:mm:ss` pattern, tolerating a date-only value.

    Bronze keeps raw types, so the column may already be a timestamp (the SQL
    Server DATETIME2 extract) or a string (CSV/Excel). `try_to_timestamp`
    returns NULL instead of failing the job on an unparseable value, matching
    the Talend inputs which have `Die on error` unchecked.
    """
    field = df.schema[column]
    if not isinstance(field.dataType, StringType):
        return F.col(column).cast(TimestampType())

    as_string = F.col(column)
    return F.coalesce(
        F.try_to_timestamp(as_string, F.lit(timestamp_format)),
        F.try_to_timestamp(as_string, F.lit("dd-MM-yyyy")),
        F.try_to_timestamp(as_string),
    )


def union_sources(dataframes: Sequence[DataFrame]) -> DataFrame:
    """`tUnite`: schema-aligned union of the normalized sources."""
    if not dataframes:
        raise ValueError("at least one source DataFrame is required")
    unioned = dataframes[0]
    for df in dataframes[1:]:
        unioned = unioned.unionByName(df, allowMissingColumns=True)
    return unioned


def _dedupe_window() -> Window:
    order = [F.col("_source_priority").asc()]
    order += [F.col(c).asc_nulls_last() for c in _TIEBREAK_COLUMNS]
    return Window.partitionBy(F.col(MERGE_KEY)).orderBy(*order)


def dedupe_transactions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """`tUniqRow` on `transaction_id`: return (unique rows, duplicate rows).

    Talend emits the *first* row it sees for a key on the UNIQUE flow and the
    later ones on the DUPLICATE flow. "First" there means tUnite arrival order,
    i.e. the mergeOrder of the input branch. We reproduce that with source
    priority (mssql < excel < csv) and then a fully deterministic tiebreak on
    `_ingest_ts` followed by the payload columns, because Spark has no stable
    notion of arrival order within a source.
    """
    ranked = df.withColumn("_dedupe_rank", F.row_number().over(_dedupe_window()))
    unique = ranked.filter(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")
    duplicates = ranked.filter(F.col("_dedupe_rank") > 1).drop("_dedupe_rank")
    return unique, duplicates


def select_fact_columns(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    """`tMap`: pass-through projection onto the gold fact schema."""
    columns = list(FACT_COLUMNS) + ([c for c in LINEAGE_COLUMNS if c in df.columns] if keep_lineage else [])
    return df.select(*columns)


def split_orphans(
    fact: DataFrame,
    dim_account: DataFrame,
    dim_branch: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Referential-integrity check against the two dimensions.

    Delta foreign keys on Unity Catalog are informational (`NOT ENFORCED`), so the
    check is done here. A NULL key is not an orphan (T-SQL FKs allow NULL);
    a non-NULL key with no dimension row is quarantined with a reason instead of
    being dropped.
    """
    accounts = dim_account.select(F.col("account_id").alias("_dim_account_id")).distinct()
    branches = dim_branch.select(F.col("branch_id").alias("_dim_branch_id")).distinct()

    joined = fact.join(
        accounts, fact["account_id"] == F.col("_dim_account_id"), "left"
    ).join(branches, fact["branch_id"] == F.col("_dim_branch_id"), "left")

    missing_account = F.col("account_id").isNotNull() & F.col("_dim_account_id").isNull()
    missing_branch = F.col("branch_id").isNotNull() & F.col("_dim_branch_id").isNull()

    reason = F.concat_ws(
        ",",
        F.when(missing_account, F.lit("missing_account_id")),
        F.when(missing_branch, F.lit("missing_branch_id")),
    )

    flagged = joined.withColumn("_orphan_reason", F.when(reason == "", None).otherwise(reason)).drop(
        "_dim_account_id", "_dim_branch_id"
    )

    clean = flagged.filter(F.col("_orphan_reason").isNull()).drop("_orphan_reason")
    orphans = flagged.filter(F.col("_orphan_reason").isNotNull())
    return clean, orphans


def build_fact_transaction(
    sources: Iterable[tuple[TransactionSource, DataFrame]],
    dim_account: DataFrame,
    dim_branch: DataFrame,
    config: GoldConfig | None = None,
    quarantine_orphans: bool = True,
) -> FactLoadResult:
    """Full `Load_FactTransaction` pipeline as a pure function over DataFrames."""
    cfg = config or GoldConfig()
    normalized = [
        normalize_source(df, source.name, source.priority, cfg.timestamp_format)
        for source, df in sources
    ]
    unioned = union_sources(normalized)
    unique, duplicates = dedupe_transactions(unioned)
    candidate = select_fact_columns(unique, keep_lineage=True)
    clean, orphans = split_orphans(candidate, dim_account, dim_branch)

    fact = select_fact_columns(clean if quarantine_orphans else candidate)
    return FactLoadResult(
        fact=fact,
        orphans=orphans,
        duplicates=select_fact_columns(duplicates, keep_lineage=True),
    )


def upsert_by_key(target: DataFrame, source: DataFrame, key: str = MERGE_KEY) -> DataFrame:
    """DataFrame equivalent of the Delta `MERGE ... WHEN MATCHED UPDATE * /
    WHEN NOT MATCHED INSERT *` used by the job, for local verification.

    The Databricks job uses `DeltaTable.merge`; this keeps the same semantics
    testable without a Delta runtime.
    """
    columns = target.columns
    surviving = target.join(source.select(key), on=key, how="left_anti")
    return surviving.unionByName(source.select(*columns))
