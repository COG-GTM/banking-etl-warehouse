"""Shared pytest fixtures: a local Spark + Delta Lake session and a test EtlConfig.

Spark tests need Java 17 (``JAVA_HOME``) and ``pip install -e .[dev]``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

BUNDLE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BUNDLE_ROOT.parent
SRC = BUNDLE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
_JDK17 = Path("/usr/lib/jvm/java-17-openjdk-amd64")
if "JAVA_HOME" not in os.environ and _JDK17.exists():
    os.environ["JAVA_HOME"] = str(_JDK17)

from banking_etl.config import EtlConfig  # noqa: E402


@pytest.fixture(scope="session")
def bundle_root() -> Path:
    return BUNDLE_ROOT


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def data_sources_dir() -> Path:
    return REPO_ROOT / "data_sources"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Session-wide local SparkSession with Delta enabled (catalog ``spark_catalog``)."""
    from banking_etl.spark import local_spark

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = local_spark(app_name="banking_etl_tests", warehouse_dir=str(warehouse))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def etl_config(request) -> EtlConfig:
    """Config pointing at the local ``spark_catalog`` with a per-test schema prefix."""
    safe = "".join(c if c.isalnum() else "_" for c in request.node.name.lower())[:40]
    return EtlConfig(catalog="spark_catalog", schema_prefix=f"t_{safe}_")


@pytest.fixture
def medallion_schemas(spark, etl_config):
    """Create the bronze/silver/gold schemas for ``etl_config`` locally and drop them afterwards."""
    from banking_etl.uc_setup import run_setup

    run_setup(spark, etl_config, create_catalog=False, create_volume=False)
    yield etl_config
    for schema in etl_config.schemas.values():
        spark.sql(f"DROP SCHEMA IF EXISTS {etl_config.catalog}.{schema} CASCADE")
