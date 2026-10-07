import os
import sys
from pathlib import Path

import pytest

MAVEN_MIRROR = "https://maven-central.storage-download.googleapis.com/maven2/"
SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(scope="session")
def ddl_spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    warehouse = tmp_path_factory.mktemp("ddl_warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("banking_etl_ddl_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # Maven Central rate-limits (HTTP 429) shared CI/VM egress; resolve delta-spark via a mirror too.
        .config("spark.jars.repositories", os.environ.get("SPARK_JARS_REPOSITORIES", MAVEN_MIRROR))
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()
