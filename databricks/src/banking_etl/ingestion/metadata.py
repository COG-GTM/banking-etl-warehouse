"""Typing with rescue, plus the ingestion metadata columns every bronze table carries.

Sources are read as STRING and typed here with ``try_cast`` /
``try_to_timestamp`` (SQL expressions, so they also work under Spark
Connect on serverless).  A non-blank value that does not parse becomes NULL
in its typed column and is kept verbatim in ``_rescued_data`` (a JSON object
``{column: raw_value}``), together with any ``_corrupt_record`` /
``_unexpected_columns`` captured by the reader.  Nothing is dropped.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .sources import ColumnSpec

SOURCE_SYSTEM_COL = "_source_system"
SOURCE_FILE_COL = "_source_file"
FILE_MODIFICATION_TIME_COL = "_file_modification_time"
INGESTED_AT_COL = "_ingested_at"
INGEST_RUN_ID_COL = "_ingest_run_id"
RESCUED_DATA_COL = "_rescued_data"

CORRUPT_RECORD_COL = "_corrupt_record"
UNEXPECTED_COLUMNS_COL = "_unexpected_columns"

METADATA_COLUMNS = (
    SOURCE_SYSTEM_COL,
    SOURCE_FILE_COL,
    FILE_MODIFICATION_TIME_COL,
    INGESTED_AT_COL,
    INGEST_RUN_ID_COL,
    RESCUED_DATA_COL,
)


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def typed_column(spec: ColumnSpec, raw_col: str | None = None) -> Column:
    """Expression parsing the STRING column ``raw_col`` into ``spec.spark_type``.

    Blank strings become NULL.  Invalid values become NULL (never an error).
    """
    raw = f"nullif(trim({_ident(raw_col or spec.name)}), '')"
    stype = spec.spark_type.upper()
    if stype == "STRING":
        return F.col(raw_col or spec.name).cast("string")
    if stype == "TIMESTAMP" and spec.timestamp_formats:
        parts = [f"try_to_timestamp({raw}, {_sql_str(fmt)})" for fmt in spec.timestamp_formats]
        if spec.iso_fallback:
            parts.append(f"try_cast({raw} AS TIMESTAMP)")
        expr = parts[0] if len(parts) == 1 else f"coalesce({', '.join(parts)})"
        return F.expr(expr)
    return F.expr(f"try_cast({raw} AS {stype})")


def _failed(spec: ColumnSpec, typed: Column, raw_col: str) -> Column:
    raw = F.col(raw_col)
    return raw.isNotNull() & (F.trim(raw) != F.lit("")) & typed.isNull()


def apply_schema_with_rescue(
    df: DataFrame,
    columns: Sequence[ColumnSpec],
    passthrough: Iterable[str] = (),
    extra_rescue_columns: Iterable[str] = (),
) -> DataFrame:
    """Type the STRING columns named by ``columns`` and build ``_rescued_data``.

    ``passthrough`` columns are kept as-is; ``extra_rescue_columns`` (for
    example ``_corrupt_record``) are folded into ``_rescued_data`` under their
    own name and dropped.
    """
    typed_cols = []
    entries: list[Column] = []
    for spec in columns:
        typed = typed_column(spec)
        typed_cols.append(typed.alias(spec.name))
        if spec.spark_type.upper() != "STRING":
            entries += [F.lit(spec.name), F.when(_failed(spec, typed, spec.name), F.col(spec.name))]
    for extra in extra_rescue_columns:
        if extra in df.columns:
            entries += [F.lit(extra), F.col(extra).cast("string")]
    if entries:
        rescued_map = F.map_filter(F.create_map(*entries), lambda _k, v: v.isNotNull())
        rescued = F.when(F.size(rescued_map) > 0, F.to_json(rescued_map))
    else:
        rescued = F.lit(None).cast("string")
    return df.select(
        *typed_cols,
        *[F.col(c) for c in passthrough],
        rescued.cast("string").alias(RESCUED_DATA_COL),
    )


def add_ingestion_metadata(
    df: DataFrame,
    source_system: str,
    run_id: str,
    source_file: Column | None = None,
    file_modification_time: Column | None = None,
) -> DataFrame:
    """Append the standard metadata columns (in ``METADATA_COLUMNS`` order)."""
    data_cols = [c for c in df.columns if c not in METADATA_COLUMNS]
    rescued = F.col(RESCUED_DATA_COL) if RESCUED_DATA_COL in df.columns else F.lit(None)
    return df.select(
        *[F.col(c) for c in data_cols],
        F.lit(source_system).alias(SOURCE_SYSTEM_COL),
        (source_file if source_file is not None else F.lit(None)).cast("string").alias(SOURCE_FILE_COL),
        (file_modification_time if file_modification_time is not None else F.lit(None))
        .cast("timestamp")
        .alias(FILE_MODIFICATION_TIME_COL),
        F.current_timestamp().alias(INGESTED_AT_COL),
        F.lit(run_id).cast("string").alias(INGEST_RUN_ID_COL),
        rescued.cast("string").alias(RESCUED_DATA_COL),
    )
