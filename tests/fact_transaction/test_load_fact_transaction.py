from __future__ import annotations

import pytest
from fact_transaction.load_fact_transaction import (
    JobParams,
    bronze_table_name,
    build,
    parse_args,
    read_sources,
    write,
)
from fact_transaction.transforms import FACT_SCHEMA


def test_parse_args_local_mode(csv_path, excel_path):
    params = parse_args(
        [
            "--bronze-table",
            "",
            "--csv-path",
            csv_path,
            "--excel-path",
            excel_path,
            "--format",
            "parquet",
            "--path",
            "/tmp/out",
        ]
    )
    assert (params.output_format, params.output_path) == ("parquet", "/tmp/out")
    assert params.catalog == "main" and params.schema == "gold"
    assert params.bronze_table is None


def test_bronze_table_name_qualification():
    assert bronze_table_name(JobParams()) == "main.bronze.transaction_db"
    assert (
        bronze_table_name(JobParams(bronze_table="other.raw.tx")) == "other.raw.tx"
    )


def test_read_sources_requires_a_source(spark):
    with pytest.raises(ValueError):
        read_sources(spark, JobParams(bronze_table=None))


def test_build_and_write_parquet(spark, tmp_path, csv_path, excel_path):
    out = str(tmp_path / "FactTransaction")
    params = parse_args(
        [
            "--bronze-table",
            "",
            "--csv-path",
            csv_path,
            "--excel-path",
            excel_path,
            "--format",
            "parquet",
            "--path",
            out,
        ]
    )
    fact = build(spark, params)
    write(fact, params)

    reloaded = spark.read.parquet(out)
    assert reloaded.count() == 17  # 7 excel + 12 csv, minus ids 14/15 shared by both
    assert reloaded.schema.fieldNames() == list(FACT_SCHEMA.fieldNames())
