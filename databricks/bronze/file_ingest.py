"""Bronze ingestion for the flat-file sources of ``Load_FactTransaction``.

Replaces the ``tFileInputDelimited`` / ``tFileInputExcel`` components. All
Databricks-only entry points (Auto Loader, ``spark-excel``) are isolated in one
function each so tests can substitute a local reader.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField

from .config import RESCUED_DATA_COLUMN, FileSource
from .metadata import with_file_metadata

# Pattern used by tFileInputExcel_1 / tFileInputDelimited_1 in the Talend job.
EXCEL_DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss"
_PY_EXCEL_DATETIME_FORMAT = "%d-%m-%Y %H:%M:%S"
_PY_EXCEL_DATE_FORMAT = "%d-%m-%Y"

Reader = Callable[[SparkSession, str, FileSource], DataFrame]


def autoloader_options(
    schema_location: str,
    *,
    file_format: str = "csv",
    header: bool = True,
    include_existing_files: bool = True,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Auto Loader (``cloudFiles``) options for a bronze file source.

    Schema inference is disabled: bronze reads everything as ``STRING`` from an
    explicit schema, and malformed / unexpected fields land in
    ``_rescued_data`` instead of being dropped.
    """
    options = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": "false",
        "cloudFiles.schemaEvolutionMode": "rescue",
        "cloudFiles.includeExistingFiles": "true" if include_existing_files else "false",
        "rescuedDataColumn": RESCUED_DATA_COLUMN,
        "header": "true" if header else "false",
        "mode": "PERMISSIVE",
    }
    if extra:
        options.update(extra)
    return options


def read_csv_autoloader(
    spark: SparkSession,
    path: str,
    source: FileSource,
    schema_location: str,
    extra_options: dict[str, str] | None = None,
) -> DataFrame:
    """Streaming Auto Loader read of the CSV source. Databricks only."""
    options = autoloader_options(
        schema_location,
        file_format=source.file_format,
        header=source.header,
        extra=extra_options,
    )
    return (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .schema(source.schema)
        .load(path)
    )


def read_csv_batch(spark: SparkSession, path: str, source: FileSource) -> DataFrame:
    """Plain batch CSV read with the same semantics as the Auto Loader path.

    Used for local validation and as the reader injected by the unit tests. The
    corrupt-record column stands in for Auto Loader's ``rescuedDataColumn``, so
    malformed rows survive here exactly as they would on a cluster.
    """
    schema = source.schema.add(StructField(RESCUED_DATA_COLUMN, StringType(), True))
    return (
        spark.read.format("csv")
        .option("header", "true" if source.header else "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", RESCUED_DATA_COLUMN)
        .schema(schema)
        .load(path)
    )


def read_excel_spark(spark: SparkSession, path: str, source: FileSource) -> DataFrame:
    """Excel read through ``com.crealytics.spark-excel``.

    Requires the Maven coordinate
    ``com.crealytics:spark-excel_2.12:3.5.1_0.20.4`` installed on the cluster
    (see ``docs/migration/02-bronze-ingestion.md``); it is not bundled with
    Databricks Runtime and is not available in the local test environment,
    where :func:`read_excel_pandas` is used instead.
    """
    return (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true" if source.header else "false")
        .option("dataAddress", f"'{source.sheet}'!A1" if isinstance(source.sheet, str) else "A1")
        .option("inferSchema", "false")
        .option("usePlainNumberFormat", "true")
        .schema(source.schema)
        .load(path)
    )


def _stringify(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime(_PY_EXCEL_DATETIME_FORMAT)
    if isinstance(value, date):
        return value.strftime(_PY_EXCEL_DATE_FORMAT)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def read_excel_pandas(spark: SparkSession, path: str, source: FileSource) -> DataFrame:
    """Fallback Excel read via pandas/openpyxl, producing the same all-string frame.

    Excel stores ``transaction_date`` as a native datetime; it is rendered with
    the ``dd-MM-yyyy HH:mm:ss`` pattern declared on ``tFileInputExcel_1`` so the
    bronze string value matches the CSV source byte for byte.
    """
    import pandas as pd

    pdf = pd.read_excel(
        path,
        sheet_name=source.sheet,
        header=0 if source.header else None,
        engine="openpyxl",
    )
    pdf.columns = source.columns[: len(pdf.columns)]
    rows = [
        tuple(_stringify(value) for value in row)
        for row in pdf.itertuples(index=False, name=None)
    ]
    return spark.createDataFrame(rows, schema=source.schema)


def ingest_file_source(
    spark: SparkSession,
    path: str,
    source: FileSource,
    batch_id: str,
    reader: Reader,
    source_file: str | None = None,
) -> DataFrame:
    """Read ``path`` with ``reader`` and attach the bronze metadata columns.

    Columns are projected in the order declared by the Talend schema; nothing is
    cast, filtered or deduplicated here.
    """
    df = reader(spark, path, source)
    projection = [F.col(name) for name in source.columns]
    if RESCUED_DATA_COLUMN in df.columns:
        projection.append(F.col(RESCUED_DATA_COLUMN))
    return with_file_metadata(df.select(*projection), batch_id, source_file=source_file)
