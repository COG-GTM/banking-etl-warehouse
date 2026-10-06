"""Delta writers for the ``dwh`` target schema."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame


def merge_into_delta(df: DataFrame, target_table: str, keys: Sequence[str]) -> None:
    """Upsert ``df`` into a Delta table, creating it (and its schema) on first run."""
    spark = df.sparkSession
    schema_name = target_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    df = df.dropDuplicates(list(keys))
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    view = f"_src_{target_table.replace('.', '_')}"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    spark.sql(
        f"MERGE INTO {target_table} t USING {view} s ON {on} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    spark.catalog.dropTempView(view)
