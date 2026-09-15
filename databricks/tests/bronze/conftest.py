from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[3]
DATABRICKS_ROOT = REPO_ROOT / "databricks"
if str(DATABRICKS_ROOT) not in sys.path:
    sys.path.insert(0, str(DATABRICKS_ROOT))


DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.appName("bronze-tests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
    )

    # DELTA_JARS lets an offline machine point at pre-downloaded Delta jars
    # (comma separated) instead of resolving the Maven coordinate.
    delta_jars = os.environ.get("DELTA_JARS")
    if delta_jars:
        builder = builder.config("spark.jars", delta_jars)
    else:
        builder = builder.config("spark.jars.packages", DELTA_PACKAGE)

    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_sources() -> Path:
    return REPO_ROOT / "data_sources"
