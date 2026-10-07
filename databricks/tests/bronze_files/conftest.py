from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = DATABRICKS_ROOT.parent
SRC = DATABRICKS_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("bronze-files-tests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    # DELTA_JARS (comma-separated local jars) skips Maven resolution, e.g. when
    # Maven Central rate-limits the machine.
    jars = os.environ.get("DELTA_JARS")
    if jars:
        session = builder.config("spark.jars", jars).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_sources() -> Path:
    return REPO_ROOT / "data_sources"
