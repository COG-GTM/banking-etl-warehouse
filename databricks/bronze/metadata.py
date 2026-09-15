"""Ingest metadata columns shared by every bronze table."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import (
    BATCH_ID_COLUMN,
    INGEST_TS_COLUMN,
    SOURCE_FILE_COLUMN,
    SOURCE_TABLE_COLUMN,
)


def new_batch_id(now: datetime | None = None) -> str:
    """Batch id of the form ``<utc timestamp>-<short uuid>``."""
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def _source_file_column(explicit: str | None) -> Column:
    if explicit is not None:
        return F.lit(explicit)
    return F.col("_metadata.file_path")


def with_file_metadata(
    df: DataFrame,
    batch_id: str,
    source_file: str | None = None,
) -> DataFrame:
    """Add ``_ingest_ts`` / ``_source_file`` / ``_batch_id`` to a file-sourced frame.

    When ``source_file`` is omitted the Spark ``_metadata.file_path`` column is
    used, which works for Auto Loader and for plain file reads.
    """
    return (
        df.withColumn(INGEST_TS_COLUMN, F.current_timestamp())
        .withColumn(SOURCE_FILE_COLUMN, _source_file_column(source_file))
        .withColumn(BATCH_ID_COLUMN, F.lit(batch_id))
    )


def with_table_metadata(df: DataFrame, batch_id: str, source_table: str) -> DataFrame:
    """Add ``_ingest_ts`` / ``_source_table`` / ``_batch_id`` to a JDBC-sourced frame."""
    return (
        df.withColumn(INGEST_TS_COLUMN, F.current_timestamp())
        .withColumn(SOURCE_TABLE_COLUMN, F.lit(source_table))
        .withColumn(BATCH_ID_COLUMN, F.lit(batch_id))
    )
