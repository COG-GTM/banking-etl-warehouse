import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

REPO_ROOT = ROOT.parent
FIXTURES = ROOT / "fixtures"
DATA_SOURCES = REPO_ROOT / "data_sources"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("banking-etl-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # Maven Central rate-limits CI/VMs; resolve Delta/JDBC jars through the GCS mirror first.
        .config(
            "spark.jars.repositories",
            os.environ.get("SPARK_JARS_REPOSITORIES", "https://maven-central.storage-download.googleapis.com/maven2/"),
        )
    )
    extra = [p for p in os.environ.get("SPARK_EXTRA_PACKAGES", "").split(",") if p]
    session = configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
    for layer in ("bronze", "silver", "gold", "ops"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def settings():
    from banking_etl.config import Settings

    return Settings(catalog=None)
