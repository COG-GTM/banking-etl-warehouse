import os
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Spark 4 needs JDK 17/21; newer JDKs fail at startup ("getSubject is not supported").
JDK17 = Path("/usr/lib/jvm/java-17-openjdk-amd64")
if JDK17.exists():
    os.environ["JAVA_HOME"] = str(JDK17)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    wh = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("banking-etl-validation-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(wh))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={wh}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Maven Central rate-limits CI/VM IPs (HTTP 429); the GCS mirror is tried as a fallback.
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sources(spark):
    from banking_etl.validation import fixtures

    return fixtures.load_sources(spark)


@pytest.fixture(scope="session")
def legacy(spark):
    from banking_etl.validation import fixtures

    return fixtures.load_legacy_dwh(spark)


@pytest.fixture(scope="session")
def built(sources):
    from banking_etl.validation import reference_transform

    gold, rejects = reference_transform.build_gold(sources)
    return {k: v.cache() for k, v in gold.items()}, rejects.cache()
