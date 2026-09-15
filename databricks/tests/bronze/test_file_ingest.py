"""Local ingestion of the committed CSV/XLSX into Delta bronze tables."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from bronze.config import (
    BATCH_ID_COLUMN,
    FILE_SOURCES,
    INGEST_TS_COLUMN,
    RESCUED_DATA_COLUMN,
    SOURCE_FILE_COLUMN,
    TRANSACTION_FILE_COLUMNS,
)
from bronze.file_ingest import (
    autoloader_options,
    ingest_file_source,
    read_csv_batch,
    read_excel_pandas,
)
from bronze.metadata import new_batch_id
from bronze.writer import write_bronze_batch

CSV_ROWS = 12
EXCEL_ROWS = 7


@pytest.fixture(scope="module")
def csv_landing(tmp_path_factory, data_sources: Path) -> Path:
    landing = tmp_path_factory.mktemp("landing_csv")
    shutil.copy(data_sources / "transaction_csv.csv", landing / "transaction_csv.csv")
    return landing


def test_autoloader_options_force_raw_strings_and_rescue():
    options = autoloader_options("/Volumes/banking/bronze/_checkpoints/csv/_schema")

    assert options["cloudFiles.format"] == "csv"
    assert options["cloudFiles.schemaLocation"].endswith("/_schema")
    assert options["cloudFiles.inferColumnTypes"] == "false"
    assert options["rescuedDataColumn"] == RESCUED_DATA_COLUMN
    assert options["header"] == "true"


def test_csv_ingest_row_count_columns_and_metadata(spark, csv_landing: Path):
    source = FILE_SOURCES["transaction_csv"]
    batch_id = new_batch_id()

    df = ingest_file_source(
        spark,
        path=str(csv_landing),
        source=source,
        batch_id=batch_id,
        reader=read_csv_batch,
    )
    rows = df.collect()

    assert df.columns == TRANSACTION_FILE_COLUMNS + [
        RESCUED_DATA_COLUMN,
        INGEST_TS_COLUMN,
        SOURCE_FILE_COLUMN,
        BATCH_ID_COLUMN,
    ]
    assert all(field.dataType.typeName() == "string" for field in df.schema[:6])
    assert len(rows) == CSV_ROWS
    assert {row[BATCH_ID_COLUMN] for row in rows} == {batch_id}
    assert all(row[INGEST_TS_COLUMN] is not None for row in rows)
    assert all(row[SOURCE_FILE_COLUMN].endswith("transaction_csv.csv") for row in rows)
    assert rows[0]["transaction_id"] == "14"
    assert rows[0]["transaction_date"] == "21-01-2024 14:00:00"


def test_excel_ingest_matches_csv_shape_and_date_format(spark, data_sources: Path):
    source = FILE_SOURCES["transaction_excel"]
    path = str(data_sources / "transaction_excel.xlsx")
    batch_id = new_batch_id()

    df = ingest_file_source(
        spark,
        path=path,
        source=source,
        batch_id=batch_id,
        reader=read_excel_pandas,
        source_file=path,
    )
    rows = df.collect()

    assert df.columns == TRANSACTION_FILE_COLUMNS + [
        INGEST_TS_COLUMN,
        SOURCE_FILE_COLUMN,
        BATCH_ID_COLUMN,
    ]
    assert len(rows) == EXCEL_ROWS
    assert {row[SOURCE_FILE_COLUMN] for row in rows} == {path}
    assert {row[BATCH_ID_COLUMN] for row in rows} == {batch_id}
    # Excel stores a native datetime; bronze renders the Talend dd-MM-yyyy HH:mm:ss pattern.
    assert rows[0]["transaction_date"] == "18-01-2024 13:10:00"
    assert rows[0]["amount"] == "50000"


def test_bronze_delta_tables_are_append_only(spark, csv_landing: Path, tmp_path: Path):
    source = FILE_SOURCES["transaction_csv"]
    table = "file_transaction_csv_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    for _ in range(2):
        df = ingest_file_source(
            spark,
            path=str(csv_landing),
            source=source,
            batch_id=new_batch_id(),
            reader=read_csv_batch,
        )
        write_bronze_batch(df, table, path=str(tmp_path / table))

    result = spark.table(table)
    assert result.count() == CSV_ROWS * 2
    assert result.select(BATCH_ID_COLUMN).distinct().count() == 2
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_excel_lands_in_its_own_delta_table(spark, data_sources: Path, tmp_path: Path):
    source = FILE_SOURCES["transaction_excel"]
    path = str(data_sources / "transaction_excel.xlsx")
    table = "file_transaction_excel_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    df = ingest_file_source(
        spark,
        path=path,
        source=source,
        batch_id=new_batch_id(),
        reader=read_excel_pandas,
        source_file=path,
    )
    write_bronze_batch(df, table, path=str(tmp_path / table))

    result = spark.table(table)
    assert result.count() == EXCEL_ROWS
    assert result.where(f"{SOURCE_FILE_COLUMN} IS NULL OR {INGEST_TS_COLUMN} IS NULL").count() == 0
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_unexpected_column_lands_in_rescued_data(spark, tmp_path: Path):
    source = FILE_SOURCES["transaction_csv"]
    malformed = tmp_path / "malformed.csv"
    malformed.write_text(
        "transaction_id,account_id,transaction_date,amount,transaction_type,branch_id\n"
        "99,1,01-02-2024 10:00:00,100,Deposit,2,extra-value\n"
    )

    df = ingest_file_source(
        spark,
        path=str(malformed),
        source=source,
        batch_id=new_batch_id(),
        reader=read_csv_batch,
    )
    row = df.collect()[0]

    assert RESCUED_DATA_COLUMN in df.columns
    assert row["transaction_id"] == "99"
    assert row[RESCUED_DATA_COLUMN] is not None
