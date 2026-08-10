# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: `dwh.silver.dim_branch`
# MAGIC
# MAGIC Databricks replacement for the Talend job `Load_DimBranch`.
# MAGIC
# MAGIC Talend job (`IDX_INTERNSHIP/process/Load_DimBranch_0.1.item`):
# MAGIC
# MAGIC ```
# MAGIC tMSSqlInput (Sample_DB, table "branch")
# MAGIC     SELECT dbo.branch.branch_id, dbo.branch.branch_name, dbo.branch.branch_location
# MAGIC     FROM   dbo.branch
# MAGIC   -> tMap_1 (straight pass-through, no filter, no expression)
# MAGIC       BranchID       = row1.branch_id
# MAGIC       BranchName     = row1.branch_name
# MAGIC       BranchLocation = row1.branch_location
# MAGIC   -> tMSSqlOutput (DWH.DimBranch, INSERT)
# MAGIC ```
# MAGIC
# MAGIC Differences vs. Talend, by design:
# MAGIC * source is `dwh.bronze.branch` (ticket 4) instead of a JDBC read of `dbo.branch`;
# MAGIC * snake_case column names on the silver target;
# MAGIC * `MERGE INTO` on `branch_id` instead of blind `INSERT`, so re-runs are idempotent;
# MAGIC * data-quality gates before the write.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = "dwh"
BRONZE_TABLE = "dwh.bronze.branch"
TARGET_TABLE = "dwh.silver.dim_branch"
BUSINESS_KEY = "branch_id"
DEFAULT_SOURCE_SYSTEM = "sample_db"

# Ticket-3 target schema for DimBranch:
#   BranchID       INT          -> branch_id       INT      (PK / business key)
#   BranchName     VARCHAR(100) -> branch_name     STRING
#   BranchLocation VARCHAR(255) -> branch_location STRING
# `branch_id` is nullable in the DataFrame schema (a cast never yields a non-nullable
# column); NOT NULL is enforced by the DQ gate and by the Delta DDL below.
TARGET_SCHEMA = StructType(
    [
        StructField("branch_id", IntegerType(), True),
        StructField("branch_name", StringType(), True),
        StructField("branch_location", StringType(), True),
        StructField("_loaded_at", TimestampType(), True),
        StructField("_source_system", StringType(), True),
    ]
)

TARGET_COLUMNS = [field.name for field in TARGET_SCHEMA.fields]

# COMMAND ----------


class DataQualityError(Exception):
    """Raised when a silver data-quality gate fails; aborts the job before any write."""


# COMMAND ----------


def _audit_columns(df: DataFrame, source_system: str):
    """Carry `_loaded_at` / `_source_system` from bronze, deriving them when absent."""
    loaded_at = (
        F.col("_loaded_at").cast(TimestampType())
        if "_loaded_at" in df.columns
        else F.current_timestamp()
    )
    source = (
        F.coalesce(F.col("_source_system").cast(StringType()), F.lit(source_system))
        if "_source_system" in df.columns
        else F.lit(source_system)
    )
    return loaded_at.alias("_loaded_at"), source.alias("_source_system")


def transform_branch(bronze_df: DataFrame, source_system: str = DEFAULT_SOURCE_SYSTEM) -> DataFrame:
    """Explicit projection + casts from `dwh.bronze.branch` to the silver schema."""
    loaded_at, source = _audit_columns(bronze_df, source_system)
    return bronze_df.select(
        F.col("branch_id").cast(IntegerType()).alias("branch_id"),
        F.col("branch_name").cast(StringType()).alias("branch_name"),
        F.col("branch_location").cast(StringType()).alias("branch_location"),
        loaded_at,
        source,
    )


def deduplicate(df: DataFrame, key: str = BUSINESS_KEY) -> DataFrame:
    """Keep one row per business key: the most recently loaded one."""
    ordering = Window.partitionBy(F.col(key)).orderBy(
        F.col("_loaded_at").desc_nulls_last(), *[F.col(c).asc_nulls_last() for c in TARGET_COLUMNS if c != key]
    )
    return (
        df.withColumn("_row_number", F.row_number().over(ordering))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .select(*TARGET_COLUMNS)
    )


# COMMAND ----------


def run_data_quality_checks(
    df: DataFrame,
    bronze_count: int,
    key: str = BUSINESS_KEY,
    table_name: str = TARGET_TABLE,
    min_row_ratio: float = 0.9,
) -> None:
    """Fail the job with a clear message when a gate is violated.

    Gates: non-null business key, unique business key, and a row count within
    `min_row_ratio` of `bronze_count` (the number of distinct business keys in
    bronze, so legitimate deduplication does not trip the gate).
    """
    null_keys = df.filter(F.col(key).isNull()).count()
    if null_keys:
        raise DataQualityError(
            f"{table_name}: {null_keys} row(s) have a NULL business key `{key}`; refusing to write."
        )

    total = df.count()
    distinct_keys = df.select(key).distinct().count()
    if total != distinct_keys:
        raise DataQualityError(
            f"{table_name}: business key `{key}` is not unique "
            f"({total} rows vs {distinct_keys} distinct keys); refusing to write."
        )

    if bronze_count > 0 and total == 0:
        raise DataQualityError(
            f"{table_name}: bronze has {bronze_count} row(s) but the silver projection is empty; "
            "refusing to write."
        )

    if bronze_count > 0 and total < bronze_count * min_row_ratio:
        raise DataQualityError(
            f"{table_name}: row count {total} is below {min_row_ratio:.0%} of the bronze count "
            f"{bronze_count}; refusing to write."
        )


# COMMAND ----------


def ensure_target_table(spark: SparkSession, table_name: str = TARGET_TABLE) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            branch_id       INT       NOT NULL,
            branch_name     STRING,
            branch_location STRING,
            _loaded_at      TIMESTAMP,
            _source_system  STRING
        ) USING DELTA
        """
    )


def merge_dimension(
    spark: SparkSession,
    df: DataFrame,
    table_name: str = TARGET_TABLE,
    key: str = BUSINESS_KEY,
    full_overwrite: bool = False,
) -> None:
    """Idempotent load: MERGE on the business key, or a full overwrite when forced."""
    if full_overwrite:
        df.select(*TARGET_COLUMNS).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_name)
        return

    ensure_target_table(spark, table_name)
    source_view = "_src_dim_branch"
    df.select(*TARGET_COLUMNS).createOrReplaceTempView(source_view)
    update_set = ", ".join(f"t.{c} = s.{c}" for c in TARGET_COLUMNS if c != key)
    insert_cols = ", ".join(TARGET_COLUMNS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {table_name} AS t
        USING {source_view} AS s
          ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


# COMMAND ----------


def build(
    spark: SparkSession,
    source_table: str = BRONZE_TABLE,
    target_table: str = TARGET_TABLE,
    source_system: str = DEFAULT_SOURCE_SYSTEM,
    full_overwrite: bool = False,
) -> int:
    bronze_df = spark.table(source_table)
    bronze_count = bronze_df.select(F.col(BUSINESS_KEY)).distinct().count()
    silver_df = deduplicate(transform_branch(bronze_df, source_system)).cache()
    run_data_quality_checks(silver_df, bronze_count, table_name=target_table)
    merge_dimension(spark, silver_df, target_table, full_overwrite=full_overwrite)
    return silver_df.count()


# COMMAND ----------

if "dbutils" in globals():
    dbutils.widgets.text("source_table", BRONZE_TABLE, "Bronze source table")
    dbutils.widgets.text("target_table", TARGET_TABLE, "Silver target table")
    dbutils.widgets.text("source_system", DEFAULT_SOURCE_SYSTEM, "Source system tag")
    dbutils.widgets.dropdown("full_overwrite", "false", ["false", "true"], "Force full overwrite")

    rows = build(
        spark,
        source_table=dbutils.widgets.get("source_table"),
        target_table=dbutils.widgets.get("target_table"),
        source_system=dbutils.widgets.get("source_system"),
        full_overwrite=dbutils.widgets.get("full_overwrite") == "true",
    )
    print(f"dim_branch load complete: {rows} row(s)")
