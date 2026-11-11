"""File ingestion (CSV and Excel) into append-only bronze Delta tables.

Each landing directory is consumed incrementally: with Auto Loader
(``cloudFiles``) on Databricks, or with Spark's plain file streaming source
elsewhere (local tests).  Either way the checkpoint makes runs idempotent per
file and the bronze table is an append-only ingestion history; a file that
lands again under a new name is ingested again.

All values are read as STRING (CSV) or rendered to STRING (Excel) and typed
by ``metadata.apply_schema_with_rescue`` so a bad value never drops a row or
fails a file.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .excel import EXCEL_ROW_COL, EXCEL_SHEET_COL, xlsx_rows_from_files
from .metadata import (
    CORRUPT_RECORD_COL,
    UNEXPECTED_COLUMNS_COL,
    add_ingestion_metadata,
    apply_schema_with_rescue,
)
from .sources import ColumnSpec, FileSource

# Talend reads the CSV as ISO-8859-15, which Spark 4 CSV does not accept; ISO-8859-1
# decodes the same bytes except for 8 symbols (e.g. the euro sign).
DEFAULT_CSV_ENCODING = "ISO-8859-1"

BINARY_FILE_SCHEMA = StructType(
    [
        StructField("path", StringType()),
        StructField("modificationTime", TimestampType()),
        StructField("length", LongType()),
        StructField("content", BinaryType()),
    ]
)


def string_schema(column_names: Sequence[str], corrupt_record: bool = True) -> StructType:
    fields = [StructField(c, StringType()) for c in column_names]
    if corrupt_record:
        fields.append(StructField(CORRUPT_RECORD_COL, StringType()))
    return StructType(fields)


def _reader(spark: SparkSession, streaming: bool, use_auto_loader: bool, file_format: str):
    if streaming and use_auto_loader:
        return spark.readStream.format("cloudFiles").option("cloudFiles.format", file_format)
    base = spark.readStream if streaming else spark.read
    return base.format(file_format)


def read_csv_as_strings(
    spark: SparkSession,
    path: str,
    column_names: Sequence[str],
    *,
    streaming: bool = True,
    use_auto_loader: bool = True,
    file_glob: str = "*.csv",
    encoding: str = DEFAULT_CSV_ENCODING,
    delimiter: str = ",",
) -> DataFrame:
    """Read CSV files with a header as all-STRING columns plus ``_corrupt_record``.

    Adds ``_src_path`` / ``_src_mtime`` from the hidden ``_metadata`` column.
    """
    reader = (
        _reader(spark, streaming, use_auto_loader, "csv")
        .schema(string_schema(column_names))
        .option("header", "true")
        .option("sep", delimiter)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", encoding)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .option("pathGlobFilter", file_glob)
        .option("recursiveFileLookup", "true")
    )
    df = reader.load(path)
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_src_path"),
        F.col("_metadata.file_modification_time").alias("_src_mtime"),
    )


def _ship_package_with_udfs() -> None:
    """Pickle banking_etl by value so Python workers need not have it installed."""
    from pyspark import cloudpickle

    package = sys.modules[__name__.split(".")[0]]
    cloudpickle.register_pickle_by_value(package)


def read_xlsx_as_strings(
    spark: SparkSession,
    path: str,
    column_names: Sequence[str],
    *,
    streaming: bool = True,
    use_auto_loader: bool = True,
    file_glob: str = "*.xlsx",
    sheet_name: str | None = None,
) -> DataFrame:
    """Read workbooks as ``binaryFile`` and parse them with openpyxl (see ``excel.py``)."""
    reader = _reader(spark, streaming, use_auto_loader, "binaryFile").option("pathGlobFilter", file_glob)
    if not (streaming and use_auto_loader):
        reader = reader.schema(BINARY_FILE_SCHEMA).option("recursiveFileLookup", "true")
    binary = reader.load(path).select("path", "modificationTime", "content")

    names = list(column_names)
    out_schema = StructType(
        [StructField(c, StringType()) for c in names]
        + [
            StructField(CORRUPT_RECORD_COL, StringType()),
            StructField(UNEXPECTED_COLUMNS_COL, StringType()),
            StructField(EXCEL_SHEET_COL, StringType()),
            StructField(EXCEL_ROW_COL, IntegerType()),
            StructField("_src_path", StringType()),
            StructField("_src_mtime", TimestampType()),
        ]
    )
    field_names = out_schema.fieldNames()

    def parse_batches(batches):
        import pandas as pd

        for pdf in batches:
            rows = list(
                xlsx_rows_from_files(
                    zip(pdf["path"], pdf["modificationTime"], pdf["content"]), names, sheet_name
                )
            )
            for r in rows:
                r["_src_path"] = r.pop("path")
                r["_src_mtime"] = r.pop("modificationTime")
            yield pd.DataFrame(rows, columns=field_names).astype({EXCEL_ROW_COL: "Int32"})

    _ship_package_with_udfs()
    return binary.mapInPandas(parse_batches, out_schema)


def to_bronze(
    raw: DataFrame,
    columns: Sequence[ColumnSpec],
    source_system: str,
    run_id: str,
    passthrough: Sequence[str] = (),
) -> DataFrame:
    """Type + rescue a raw STRING frame from ``read_*_as_strings`` and add metadata."""
    keep = [*passthrough, "_src_path", "_src_mtime"]
    typed = apply_schema_with_rescue(
        raw,
        columns,
        passthrough=keep,
        extra_rescue_columns=(CORRUPT_RECORD_COL, UNEXPECTED_COLUMNS_COL),
    )
    out = add_ingestion_metadata(
        typed,
        source_system=source_system,
        run_id=run_id,
        source_file=F.col("_src_path"),
        file_modification_time=F.col("_src_mtime"),
    )
    return out.drop("_src_path", "_src_mtime")


def file_source_system(source: FileSource) -> str:
    return f"file:{source.name}"


def bronze_file_frame(
    spark: SparkSession,
    source: FileSource,
    path: str,
    run_id: str,
    *,
    streaming: bool = True,
    use_auto_loader: bool = True,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    excel_sheet: str | None = None,
) -> DataFrame:
    """Bronze-shaped DataFrame (streaming by default) for a transaction file drop."""
    if source.file_format == "csv":
        raw = read_csv_as_strings(
            spark,
            path,
            source.column_names,
            streaming=streaming,
            use_auto_loader=use_auto_loader,
            file_glob=source.file_glob,
            encoding=csv_encoding,
        )
        return to_bronze(raw, source.columns, file_source_system(source), run_id)
    if source.file_format == "xlsx":
        raw = read_xlsx_as_strings(
            spark,
            path,
            source.column_names,
            streaming=streaming,
            use_auto_loader=use_auto_loader,
            file_glob=source.file_glob,
            sheet_name=excel_sheet,
        )
        return to_bronze(
            raw,
            source.columns,
            file_source_system(source),
            run_id,
            passthrough=(EXCEL_SHEET_COL, EXCEL_ROW_COL),
        )
    raise ValueError(f"unsupported file format: {source.file_format}")


def write_stream_append(df: DataFrame, table: str, checkpoint: str) -> None:
    """Drain everything available into ``table`` (append-only) and stop."""
    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()
