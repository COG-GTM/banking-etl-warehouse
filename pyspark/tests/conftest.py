from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StructType

from banking_etl.config import WarehouseSettings
from banking_etl.spark_session import get_spark

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_SOURCES = REPO_ROOT / "data_sources"


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    os.environ.setdefault("SPARK_SESSION_TIMEZONE", "UTC")
    session = get_spark("banking-etl-tests")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture()
def warehouse(tmp_path: Path) -> WarehouseSettings:
    return WarehouseSettings(storage="path", catalog="", db_schema="dwh", base_path=str(tmp_path / "dwh"))


def shape(schema: StructType) -> list[tuple[str, DataType]]:
    """Column names and types, ignoring nullability and CHAR/VARCHAR metadata."""
    return [(f.name, f.dataType) for f in schema.fields]
