"""Shared pytest fixtures: a local Spark 4 + Delta session and a local EnvConfig.

Tests run with OSS Delta, so Unity Catalog-only DDL (PK/FK constraints, CLUSTER BY) is
rendered with ``unity_catalog=False``. Each ticket adds its own ``tests/test_t<N>_*.py``.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

for jdk in ("/usr/lib/jvm/java-17-openjdk-amd64", "/usr/lib/jvm/java-21-openjdk-amd64"):
    if Path(jdk).exists() and "JAVA_HOME" not in os.environ.get("BANKING_DWH_KEEP_JAVA_HOME", ""):
        os.environ["JAVA_HOME"] = jdk
        break
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from banking_dwh.common.config import EnvConfig  # noqa: E402

MAVEN_MIRROR = "https://maven-central.storage-download.googleapis.com/maven2/"


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="bdwh-warehouse-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("banking_dwh-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.sources.default", "delta")
        .config("spark.jars.repositories", MAVEN_MIRROR)
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="session")
def local_cfg() -> EnvConfig:
    """Local stand-in for the dev target: 'catalog' spark_catalog, schemas t_bronze, t_silver..."""
    return EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix="t_")


@pytest.fixture(scope="session")
def local_schemas(spark, local_cfg):
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {local_cfg.schema(layer)}")
    return local_cfg
