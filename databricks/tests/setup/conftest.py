import os
import sys

import pytest

_DATABRICKS = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for _p in (os.path.join(_DATABRICKS, "src"), os.path.join(_DATABRICKS, "setup")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

if not os.environ.get("JAVA_HOME") and os.path.isdir("/usr/lib/jvm/java-17-openjdk-amd64"):
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

DATABRICKS_ROOT = _DATABRICKS


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    """Local Spark + Delta; reuses a session another conftest already started."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return

    from delta import configure_spark_with_delta_pip

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("banking-etl-setup-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
