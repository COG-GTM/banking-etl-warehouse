import os
import sys
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "databricks" / "src"
DATA_SOURCES = REPO_ROOT / "data_sources"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

MSSQL_JDBC = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
MAVEN_MIRROR = "https://maven-central.storage-download.googleapis.com/maven2/"


@pytest.fixture(scope="session")
def data_sources() -> Path:
    return DATA_SOURCES


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("bronze-ingestion-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.jars.repositories", MAVEN_MIRROR)
        .config("spark.ui.enabled", "false")
    )
    extra = [MSSQL_JDBC] if os.environ.get("SAMPLE_DB_JDBC_URL") else []
    session = delta.configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze_config(spark, tmp_path):
    from banking_etl.ingestion.config import BronzeConfig

    schema = f"bronze_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{schema}")
    cfg = BronzeConfig(
        catalog="spark_catalog",
        bronze_schema=schema,
        landing_root=str(tmp_path / "landing"),
        checkpoint_root=str(tmp_path / "checkpoints"),
    )
    yield cfg
    spark.sql(f"DROP DATABASE IF EXISTS spark_catalog.{schema} CASCADE")
