"""Publish conformed silver dimensions to the gold star schema.

The Talend jobs wrote with `DATA_ACTION=INSERT` into a table created on demand
(`TABLE_ACTION=CREATE_IF_NOT_EXISTS`), i.e. a blind append that fails on a
primary-key collision, so a rerun was only safe after truncating the target.
Here the default is an idempotent Delta `MERGE` on the business key; the legacy
behaviour is still available as `full_refresh`.

`upsert_dataframes` is the pure-DataFrame expression of the same semantics: it
is what the tests assert against, since Delta is not available off-cluster.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import deduplicate_on_key


def assert_unique_key(df: DataFrame, key: str) -> DataFrame:
    """Fail fast: a MERGE with duplicate source keys is non-deterministic."""
    duplicates = df.groupBy(key).count().filter(F.col("count") > 1).limit(1).collect()
    if duplicates:
        raise ValueError(f"duplicate business key {key}={duplicates[0][key]!r} in merge source")
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"null business key {key} in merge source")
    return df


def upsert_dataframes(target: DataFrame, source: DataFrame, key: str) -> DataFrame:
    """Reference implementation of the MERGE: update on key match, else insert."""
    columns = target.columns
    kept = target.join(source.select(key).distinct(), on=key, how="left_anti")
    return kept.unionByName(source.select(*columns))


def merge_dimension(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    key: str,
    deduplicate: bool = True,
    order_by: str | None = None,
) -> None:
    """Idempotent upsert of `df` into the Delta table `target_table`."""
    source = deduplicate_on_key(df, key, order_by) if deduplicate else df
    assert_unique_key(source, key)

    if not spark.catalog.tableExists(target_table):
        source.write.format("delta").saveAsTable(target_table)
        return

    view = f"_merge_src_{target_table.replace('.', '_')}"
    source.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING {view} AS s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(view)


def full_refresh_dimension(spark: SparkSession, df: DataFrame, target_table: str) -> None:
    """Legacy truncate/insert equivalent, kept for backfills and reprocessing."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def write_silver(df: DataFrame, target_table: str) -> None:
    """Silver is a full restatement of the conformed source, so overwrite."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
