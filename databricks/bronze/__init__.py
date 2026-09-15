"""Bronze ingestion layer for the Databricks migration of the Talend extract stage."""

from .config import CATALOG, FILE_SOURCES, JDBC_SOURCES, FileSource, JdbcSource
from .file_ingest import (
    autoloader_options,
    ingest_file_source,
    read_csv_autoloader,
    read_csv_batch,
    read_excel_pandas,
    read_excel_spark,
)
from .jdbc_ingest import (
    JdbcConnection,
    build_jdbc_options,
    build_source_query,
    ingest_jdbc_source,
)
from .metadata import new_batch_id, with_file_metadata, with_table_metadata
from .writer import write_bronze_batch, write_bronze_stream

__all__ = [
    "CATALOG",
    "FILE_SOURCES",
    "JDBC_SOURCES",
    "FileSource",
    "JdbcConnection",
    "JdbcSource",
    "autoloader_options",
    "build_jdbc_options",
    "build_source_query",
    "ingest_file_source",
    "ingest_jdbc_source",
    "new_batch_id",
    "read_csv_autoloader",
    "read_csv_batch",
    "read_excel_pandas",
    "read_excel_spark",
    "with_file_metadata",
    "with_table_metadata",
    "write_bronze_batch",
    "write_bronze_stream",
]
