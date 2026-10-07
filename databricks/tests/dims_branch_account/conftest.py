import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[2]
SRC = DATABRICKS_ROOT / "src"
for _p in (SRC, Path(__file__).resolve().parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


_JAVA17 = "/usr/lib/jvm/java-17-openjdk-amd64"
# Spark 4.0 supports Java 17/21 only; the box default JAVA_HOME may be 11 or 25.
_current = os.environ.get("JAVA_HOME", "")
if os.path.isdir(_JAVA17) and not any(v in _current for v in ("-17-", "-21-")):
    os.environ["JAVA_HOME"] = _JAVA17


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="t5_dims_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tests-dims-branch-account")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
