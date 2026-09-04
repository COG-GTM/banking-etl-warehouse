"""Transformations replacing the Talend tUnite / tUniqRow / tMap components."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .readers import SOURCE_CSV, SOURCE_DB, SOURCE_EXCEL

# tUnite_1 merge order in the Talend job: row1 = SQL Server, row2 = Excel,
# row3 = CSV. tUniqRow_1 keeps the first row it sees for a duplicate key, so
# the merge order is what makes deduplication deterministic here too.
SOURCE_PRIORITY = {SOURCE_DB: 1, SOURCE_EXCEL: 2, SOURCE_CSV: 3}

FACT_SCHEMA = StructType(
    [
        StructField("TransactionID", IntegerType(), nullable=True),
        StructField("AccountID", IntegerType(), nullable=True),
        StructField("TransactionDate", TimestampType(), nullable=True),
        StructField("Amount", DecimalType(19, 4), nullable=True),
        StructField("TransactionType", StringType(), nullable=True),
        StructField("BranchID", IntegerType(), nullable=True),
    ]
)

FACT_COLUMNS = tuple(field.name for field in FACT_SCHEMA.fields)


def unify_transactions(*dfs: DataFrame) -> DataFrame:
    """Union the normalized source streams (Talend ``tUnite_1``)."""
    if not dfs:
        raise ValueError("unify_transactions requires at least one DataFrame")
    unified = dfs[0]
    for df in dfs[1:]:
        unified = unified.unionByName(df, allowMissingColumns=False)
    return unified


def dedupe_transactions(df: DataFrame) -> DataFrame:
    """Deduplicate on ``TransactionID`` (Talend ``tUniqRow_1``).

    Talend keeps the first row reaching tUniqRow, which depends on the tUnite
    merge order: SQL Server, then Excel, then CSV. The same ordering is applied
    here via :data:`SOURCE_PRIORITY`, with ``TransactionDate`` and ``_source``
    as tie-breakers so the result is deterministic regardless of partitioning.
    """
    priority = F.coalesce(
        F.create_map(
            *[x for k, v in SOURCE_PRIORITY.items() for x in (F.lit(k), F.lit(v))]
        )[F.col("_source")],
        F.lit(len(SOURCE_PRIORITY) + 1),
    )
    window = Window.partitionBy("TransactionID").orderBy(
        priority.asc(), F.col("TransactionDate").asc_nulls_last(), F.col("_source").asc()
    )
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def build_fact_transaction(df: DataFrame) -> DataFrame:
    """Project the gold ``FactTransaction`` columns (Talend ``tMap_1``)."""
    return df.select(
        *[
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in FACT_SCHEMA.fields
        ]
    )


def validate_referential_integrity(
    fact_df: DataFrame,
    dim_account_df: DataFrame,
    dim_branch_df: DataFrame,
) -> DataFrame:
    """Return fact rows whose FKs are missing from the dimensions.

    Delta Lake does not enforce foreign keys, so the constraints declared in
    ``sql_scripts/01_create_tables.sql`` are checked explicitly. The result adds
    ``_orphan_account`` / ``_orphan_branch`` flags; an empty DataFrame means the
    load is referentially clean.
    """
    accounts = dim_account_df.select(F.col("AccountID").alias("_dim_account_id")).distinct()
    branches = dim_branch_df.select(F.col("BranchID").alias("_dim_branch_id")).distinct()

    joined = fact_df.join(
        accounts, fact_df["AccountID"] == F.col("_dim_account_id"), how="left"
    ).join(branches, fact_df["BranchID"] == F.col("_dim_branch_id"), how="left")

    return (
        joined.withColumn("_orphan_account", F.col("_dim_account_id").isNull())
        .withColumn("_orphan_branch", F.col("_dim_branch_id").isNull())
        .filter(F.col("_orphan_account") | F.col("_orphan_branch"))
        .drop("_dim_account_id", "_dim_branch_id")
    )
