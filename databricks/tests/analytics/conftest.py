import os
import sys
from pathlib import Path

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[2]
if str(DATABRICKS_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(DATABRICKS_ROOT / "src"))
# Python workers must match the driver's minor version (PYTHON_VERSION_MISMATCH otherwise).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark 4 + Delta session (reuses an existing session if a shared conftest already made one)."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("banking-etl-analytics-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.repositories",
            os.environ.get("SPARK_JARS_REPOSITORIES", "https://maven-central.storage-download.googleapis.com/maven2/"),
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
