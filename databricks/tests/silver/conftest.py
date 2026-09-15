from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-dimension-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def bronze_df(spark: SparkSession, rows: list[dict], columns: list[str]):
    """Bronze shape: every business column is a string, plus ingest metadata."""
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    fields = [StructField(c, StringType(), True) for c in columns]
    fields += [
        StructField("_ingest_ts", TimestampType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
    ]
    import datetime

    data = [
        tuple(row.get(c) for c in columns)
        + (datetime.datetime(2024, 1, 1, 0, 0, 0), "sample.bak", "batch-1")
        for row in rows
    ]
    return spark.createDataFrame(data, StructType(fields))
