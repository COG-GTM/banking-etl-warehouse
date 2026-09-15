"""Append-only Delta writes for the bronze layer."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_bronze_batch(
    df: DataFrame,
    table: str,
    *,
    path: str | None = None,
    merge_schema: bool = True,
) -> None:
    """Append a batch frame to a Delta bronze table (managed, or ``path`` for local runs)."""
    writer = df.write.format("delta").mode("append")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if path is not None:
        writer.option("path", path).saveAsTable(table)
    else:
        writer.saveAsTable(table)


def write_bronze_stream(
    df: DataFrame,
    table: str,
    checkpoint_location: str,
    *,
    available_now: bool = True,
) -> StreamingQuery:  # pragma: no cover - Databricks only
    """Append an Auto Loader stream to a Delta bronze table."""
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.toTable(table)
