import os
import sys
from pathlib import Path

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[2]
SRC = DATABRICKS_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PARITY_CSV = DATABRICKS_ROOT / "fixtures" / "parity" / "dim_customer.csv"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if not os.environ.get("JAVA_HOME") and os.path.isdir("/usr/lib/jvm/java-17-openjdk-amd64"):
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dims_customer_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
        # Extra Maven mirror for the delta-spark jar (repo1.maven.org rate-limits CI/VM IPs).
        .config(
            "spark.jars.repositories",
            os.environ.get("SPARK_JARS_REPOSITORIES", "https://maven-central.storage-download.googleapis.com/maven2"),
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
