"""Shared helpers for the silver dimension transforms.

Every function here is a plain function over Spark DataFrames so the whole
silver layer is importable and testable without a Databricks cluster.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_METADATA_COLUMNS = ("_ingest_ts", "_source_file", "_batch_id")

MONEY_TYPE = DecimalType(19, 4)

DATE_FORMATS = ("yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy", "MM/dd/yyyy")

TIMESTAMP_FORMATS = ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "dd-MM-yyyy HH:mm:ss")


@dataclass(frozen=True)
class DimensionConfig:
    """Table names for one dimension, so nothing is hard-coded to a workspace."""

    catalog: str = "banking"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    bronze_tables: dict[str, str] = field(default_factory=dict)
    silver_table: str = ""
    gold_table: str = ""
    business_key: str = ""

    def bronze(self, name: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{self.bronze_tables[name]}"

    @property
    def silver_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{self.silver_table}"

    @property
    def gold_fqn(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.{self.gold_table}"


def drop_ingest_metadata(df: DataFrame) -> DataFrame:
    """Drop the bronze bookkeeping columns; silver carries business data only."""
    present = [c for c in BRONZE_METADATA_COLUMNS if c in df.columns]
    return df.drop(*present) if present else df


def _blank_to_null(col: Column) -> Column:
    trimmed = F.trim(col.cast("string"))
    return F.when(trimmed == F.lit(""), F.lit(None).cast("string")).otherwise(trimmed)


def _try_cast(col: Column, data_type) -> Column:
    """Cast that yields NULL instead of failing under ANSI mode."""
    caster = getattr(col, "try_cast", None)
    return caster(data_type) if caster is not None else col.cast(data_type)


def to_int(col: Column) -> Column:
    """Bronze integers arrive as strings; tolerate padding and empty strings."""
    return _try_cast(_blank_to_null(col), "int")


def to_decimal(col: Column, decimal_type: DecimalType = MONEY_TYPE) -> Column:
    """Parse a raw money string, stripping currency symbols and thousands separators."""
    cleaned = F.regexp_replace(_blank_to_null(col), r"[$,\s]", "")
    return _try_cast(cleaned, decimal_type)


def to_date(col: Column, formats: tuple[str, ...] = DATE_FORMATS) -> Column:
    """Parse the first matching format; unparseable values become NULL."""
    cleaned = _blank_to_null(col)
    return F.coalesce(*[F.try_to_timestamp(cleaned, F.lit(fmt)).cast("date") for fmt in formats])


def to_timestamp(col: Column, formats: tuple[str, ...] = TIMESTAMP_FORMATS) -> Column:
    cleaned = _blank_to_null(col)
    return F.coalesce(*[F.try_to_timestamp(cleaned, F.lit(fmt)) for fmt in formats])


def upcase(col: Column) -> Column:
    """Equivalent of Talend `StringHandling.UPCASE`, which is null-safe."""
    return F.upper(_blank_to_null(col))


def clean_string(col: Column) -> Column:
    """Pass-through string cleansing: trim + empty-string-to-null, no case change."""
    return _blank_to_null(col)


def deduplicate_on_key(df: DataFrame, key: str, order_by: str | None = None) -> DataFrame:
    """Keep one row per business key.

    Talend's tMap lookups use `matchingMode="UNIQUE_MATCH"`, i.e. the last row
    loaded for a lookup key wins. The same rule is applied here, ordered by
    `order_by` (usually `_ingest_ts`) when available, otherwise by input order.
    """
    order_col = F.col(order_by).desc() if order_by and order_by in df.columns else F.col("_dedup_seq").desc()
    seq = df.withColumn("_dedup_seq", F.monotonically_increasing_id())
    window = Window.partitionBy(key).orderBy(order_col, F.col("_dedup_seq").desc())
    return (
        seq.withColumn("_dedup_rank", F.row_number().over(window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank", "_dedup_seq")
    )
