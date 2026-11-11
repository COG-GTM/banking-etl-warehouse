"""Entry point used by the bronze ingestion notebook / job."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from pyspark.sql import SparkSession

from .config import BronzeConfig, ensure_bronze_objects
from .files import DEFAULT_CSV_ENCODING, bronze_file_frame, write_stream_append
from .sources import FILE_SOURCES_BY_NAME, SAMPLE_DB_TABLES, SAMPLE_DB_TABLES_BY_NAME
from .sqlserver import SqlServerConnection, ingest_sample_db

ALL_SOURCES = ("sample_db", *FILE_SOURCES_BY_NAME)


def parse_csv_list(value: str | None, allowed: Sequence[str]) -> list[str]:
    items = [v.strip() for v in (value or "").split(",") if v.strip()]
    if not items or items == ["all"]:
        return list(allowed)
    unknown = sorted(set(items) - set(allowed))
    if unknown:
        raise ValueError(f"unknown values {unknown}; allowed: {list(allowed)}")
    return items


def ingest_file_source(
    spark: SparkSession,
    config: BronzeConfig,
    source_name: str,
    run_id: str,
    *,
    use_auto_loader: bool = True,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    excel_sheet: str | None = None,
) -> None:
    source = FILE_SOURCES_BY_NAME[source_name]
    df = bronze_file_frame(
        spark,
        source,
        config.landing_path(source.name),
        run_id,
        use_auto_loader=use_auto_loader,
        csv_encoding=csv_encoding,
        excel_sheet=excel_sheet,
    )
    write_stream_append(df, config.table(source.name), config.checkpoint_path(source.name))


def run_bronze_ingestion(
    spark: SparkSession,
    config: BronzeConfig,
    *,
    sources: str | Sequence[str] = "all",
    sample_db_mode: str = "files",
    sample_db_tables: str | Sequence[str] = "all",
    conn: SqlServerConnection | None = None,
    foreign_catalog: str | None = None,
    use_auto_loader: bool = True,
    run_id: str | None = None,
    create_objects: bool = True,
    csv_encoding: str = DEFAULT_CSV_ENCODING,
    excel_sheet: str | None = None,
) -> dict:
    run_id = run_id or uuid.uuid4().hex
    selected = sources if not isinstance(sources, str) else parse_csv_list(sources, ALL_SOURCES)
    tables = (
        sample_db_tables
        if not isinstance(sample_db_tables, str)
        else parse_csv_list(sample_db_tables, [t.name for t in SAMPLE_DB_TABLES])
    )
    if create_objects:
        ensure_bronze_objects(spark, config)

    summary: dict = {"run_id": run_id, "sources": {}}
    for name in selected:
        if name == "sample_db":
            summary["sources"]["sample_db"] = ingest_sample_db(
                spark,
                config,
                sample_db_mode,
                run_id,
                tables=[SAMPLE_DB_TABLES_BY_NAME[t] for t in tables],
                conn=conn,
                foreign_catalog=foreign_catalog,
                use_auto_loader=use_auto_loader,
            )
        else:
            ingest_file_source(
                spark,
                config,
                name,
                run_id,
                use_auto_loader=use_auto_loader,
                csv_encoding=csv_encoding,
                excel_sheet=excel_sheet,
            )
            summary["sources"][name] = config.table(name)
    return summary
