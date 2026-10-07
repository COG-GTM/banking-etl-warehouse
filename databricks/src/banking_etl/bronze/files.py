"""Bronze ingestion of the flat-file transaction sources via Auto Loader.

Replaces ``tFileInputExcel`` / ``tFileInputDelimited`` in the Talend
``Load_FactTransaction`` job:

* ``transaction_excel.xlsx`` -> ``bronze.file_transaction_excel`` (``cloudFiles.format=excel``)
* ``transaction_csv.csv``    -> ``bronze.file_transaction_csv``   (CSV, ``dd-MM-yyyy HH:mm:ss``)

Each source lands in its own folder of the landing volume; Auto Loader keeps
its schema and checkpoint state under ``<landing>/_autoloader/``. Values that
don't fit the schema hints are kept in ``_rescued_data`` and every row is
stamped with ``_source_file`` / ``_ingested_at``.

Only :func:`read_autoloader` is Databricks-specific. Local tests inject
:func:`read_local_standin`, which streams the same folder with the OSS
``csv`` / ``parquet`` file source and the same typed schema.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DataType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEFAULT_CATALOG = "migration_demo"
DEFAULT_SCHEMA_PREFIX = "banking_mig_"
LANDING_VOLUME = "landing"

RESCUED_DATA_COLUMN = "_rescued_data"
SOURCE_FILE_COLUMN = "_source_file"
INGESTED_AT_COLUMN = "_ingested_at"
METADATA_COLUMNS = (RESCUED_DATA_COLUMN, SOURCE_FILE_COLUMN, INGESTED_AT_COLUMN)

CSV_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"
# The Excel reader renders date cells as text in their display format when read as STRING:
# the legacy workbook shows "1/18/24 13:10"; openpyxl/ISO-formatted cells "2024-01-22 9:00:00".
EXCEL_TIMESTAMP_FORMATS = ("M/d/yy H:mm", "M/d/yy H:mm:ss", "yyyy-MM-dd H:mm:ss", "yyyy-MM-dd H:mm")

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# Legacy FactTransaction types: INT ids, DATETIME, MONEY (= DECIMAL(19,4)).
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DecimalType(19, 4), True),
        StructField("transaction_type", StringType(), True),
        StructField("branch_id", IntegerType(), True),
    ]
)
TRANSACTION_COLUMNS = tuple(f.name for f in TRANSACTION_SCHEMA.fields)


def schema_hints(schema: StructType = TRANSACTION_SCHEMA, as_strings: bool = False) -> str:
    """``cloudFiles.schemaHints`` string, e.g. ``transaction_id INT, ...``."""
    return ", ".join(
        f"{f.name} {'STRING' if as_strings else f.dataType.simpleString().upper()}"
        for f in schema.fields
    )


@dataclass(frozen=True)
class FileSource:
    name: str
    file_name: str
    table: str
    file_format: str
    reader_options: dict[str, str] = field(default_factory=dict)
    schema_evolution_mode: str = "rescue"
    # False: read every column as STRING and cast in ``to_bronze``, rescuing values that
    # fail to cast. Needed where the reader cannot rescue type mismatches itself.
    typed_read: bool = True

    @property
    def folder(self) -> str:
        return self.name


EXCEL_SOURCE = FileSource(
    name="transaction_excel",
    file_name="transaction_excel.xlsx",
    table="file_transaction_excel",
    file_format="excel",
    reader_options={"headerRows": "1", "dataAddress": "Sheet1"},
    # Auto Loader's Excel reader does not support schema evolution, and it fails the whole
    # file on a cell that does not match a typed hint instead of rescuing it.
    schema_evolution_mode="none",
    typed_read=False,
)

CSV_SOURCE = FileSource(
    name="transaction_csv",
    file_name="transaction_csv.csv",
    table="file_transaction_csv",
    file_format="csv",
    reader_options={
        "header": "true",
        "timestampFormat": CSV_TIMESTAMP_FORMAT,
        "encoding": "ISO-8859-1",
        "mode": "PERMISSIVE",
    },
)

SOURCES: dict[str, FileSource] = {s.name: s for s in (EXCEL_SOURCE, CSV_SOURCE)}


@dataclass(frozen=True)
class Locations:
    """Fully qualified names and volume paths for one ingestion target."""

    catalog: str | None = DEFAULT_CATALOG
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX
    landing_root: str | None = None
    schema: str | None = None  # overrides f"{schema_prefix}bronze", e.g. a ticket-scoped schema

    def __post_init__(self) -> None:
        for value in (self.catalog, self.schema_prefix or None, self.schema):
            if value is not None and not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"invalid Unity Catalog identifier: {value!r}")

    @property
    def bronze_schema(self) -> str:
        return self.schema or f"{self.schema_prefix}bronze"

    @property
    def _state_root(self) -> str:
        # The default landing volume already belongs to one catalog.schema; a custom
        # landing_root may be shared by several targets, so namespace Auto Loader state.
        if self.landing_root:
            target = ".".join(p for p in (self.catalog, self.bronze_schema) if p)
            return f"{self.landing}/_autoloader/{target}"
        return f"{self.landing}/_autoloader"

    @property
    def landing(self) -> str:
        if self.landing_root:
            return self.landing_root.rstrip("/")
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{LANDING_VOLUME}"

    def table_name(self, source: FileSource) -> str:
        parts = [self.catalog, self.bronze_schema, source.table]
        return ".".join(p for p in parts if p)

    def source_path(self, source: FileSource) -> str:
        return f"{self.landing}/{source.folder}"

    def schema_location(self, source: FileSource) -> str:
        return f"{self._state_root}/schemas/{source.table}"

    def checkpoint_location(self, source: FileSource) -> str:
        return f"{self._state_root}/checkpoints/{source.table}"


def autoloader_options(source: FileSource, schema_location: str) -> dict[str, str]:
    return {
        "cloudFiles.format": source.file_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaHints": schema_hints(as_strings=not source.typed_read),
        "cloudFiles.inferColumnTypes": str(source.typed_read).lower(),
        "cloudFiles.schemaEvolutionMode": source.schema_evolution_mode,
        "cloudFiles.includeExistingFiles": "true",
        "rescuedDataColumn": RESCUED_DATA_COLUMN,
        **source.reader_options,
    }


Reader = Callable[[SparkSession, FileSource, Locations], DataFrame]


def read_autoloader(spark: SparkSession, source: FileSource, loc: Locations) -> DataFrame:
    """Auto Loader stream of one landing folder. Databricks only."""
    return (
        spark.readStream.format("cloudFiles")
        .options(**autoloader_options(source, loc.schema_location(source)))
        .load(loc.source_path(source))
    )


def read_local_standin(spark: SparkSession, source: FileSource, loc: Locations) -> DataFrame:
    """OSS stand-in for ``cloudFiles``: a typed file-source stream of the folder.

    The CSV source keeps malformed records in ``_rescued_data`` via
    ``columnNameOfCorruptRecord``. The Excel source is staged as all-STRING parquet
    (OSS Spark has no Excel reader), matching what Auto Loader returns for it.
    """
    if source.file_format == "csv":
        schema = StructType(
            TRANSACTION_SCHEMA.fields + [StructField(RESCUED_DATA_COLUMN, StringType(), True)]
        )
        options = {
            k: v for k, v in source.reader_options.items() if not k.startswith("cloudFiles.")
        }
        return (
            spark.readStream.format("csv")
            .options(**options)
            .option("columnNameOfCorruptRecord", RESCUED_DATA_COLUMN)
            .schema(schema)
            .load(loc.source_path(source))
        )
    string_schema = StructType([StructField(f.name, StringType(), True) for f in TRANSACTION_SCHEMA])
    return (
        spark.readStream.format("parquet")
        .schema(string_schema)
        .load(loc.source_path(source))
        .withColumn(RESCUED_DATA_COLUMN, F.lit(None).cast(StringType()))
    )


def _safe_cast(raw: Column, name: str, data_type: DataType) -> Column:
    """Cast a STRING column to ``data_type``, yielding NULL instead of failing."""
    if isinstance(data_type, TimestampType):
        return F.coalesce(
            *[F.call_function("try_to_timestamp", raw, F.lit(fmt)) for fmt in EXCEL_TIMESTAMP_FORMATS]
        )
    return F.expr(f"try_cast(`{name}` AS {data_type.simpleString()})")


def to_bronze(df: DataFrame) -> DataFrame:
    """Project the legacy columns (cast to the hinted types) plus bronze metadata.

    Columns that arrive as STRING but are typed in the legacy schema are cast with
    ``try_*`` semantics; values that fail to cast are kept as JSON in ``_rescued_data``
    (same shape as Auto Loader's: ``{"column": "raw value", "_file_path": ...}``).
    """
    string_cols = {f.name for f in df.schema.fields if isinstance(f.dataType, StringType)}
    columns, failures = [], []
    for f in TRANSACTION_SCHEMA.fields:
        raw = F.col(f.name)
        if f.name in string_cols and not isinstance(f.dataType, StringType):
            value = _safe_cast(raw, f.name, f.dataType)
            failures.append((f.name, raw, value))
        else:
            value = raw.cast(f.dataType)
        columns.append(value.alias(f.name))

    rescued = (
        F.col(RESCUED_DATA_COLUMN)
        if RESCUED_DATA_COLUMN in df.columns
        else F.lit(None).cast(StringType())
    )
    if failures:
        failed = [raw.isNotNull() & value.isNull() for _, raw, value in failures]
        cast_rescue = F.when(
            reduce(lambda a, b: a | b, failed),
            F.to_json(
                F.struct(
                    *[F.when(c, raw).alias(name) for (name, raw, _), c in zip(failures, failed)],
                    F.col("_metadata.file_path").alias("_file_path"),
                )
            ),
        )
        rescued = F.coalesce(rescued, cast_rescue)
    return df.select(
        *columns,
        rescued.alias(RESCUED_DATA_COLUMN),
        F.col("_metadata.file_path").alias(SOURCE_FILE_COLUMN),
        F.current_timestamp().alias(INGESTED_AT_COLUMN),
    )


def start_ingestion(
    spark: SparkSession,
    source: FileSource,
    loc: Locations,
    reader: Reader = read_autoloader,
) -> StreamingQuery:
    """Start an ``availableNow`` stream from the landing folder into the bronze table."""
    return (
        to_bronze(reader(spark, source, loc))
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", loc.checkpoint_location(source))
        .queryName(f"bronze_{source.table}")
        .trigger(availableNow=True)
        .toTable(loc.table_name(source))
    )


def ingest(
    spark: SparkSession,
    loc: Locations,
    sources: list[str] | None = None,
    reader: Reader = read_autoloader,
) -> dict[str, int]:
    """Run each source to completion and return the bronze row count per table."""
    counts: dict[str, int] = {}
    for name in list(SOURCES) if sources is None else sources:
        source = SOURCES[name]
        start_ingestion(spark, source, loc, reader).awaitTermination()
        table = loc.table_name(source)
        counts[table] = spark.table(table).count()
    return counts


def ensure_objects(spark: SparkSession, loc: Locations) -> None:
    """Create the bronze schema and landing volume if missing (Unity Catalog only)."""
    schema = f"`{loc.catalog}`.`{loc.bronze_schema}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.`{LANDING_VOLUME}`")
