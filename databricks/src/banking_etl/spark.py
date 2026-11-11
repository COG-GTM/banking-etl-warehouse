"""SparkSession helpers that work on Databricks and locally (tests)."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

MAVEN_MIRROR = "https://maven-central.storage-download.googleapis.com/maven2/"


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "banking_etl") -> "SparkSession":
    """Return the active session (Databricks job/notebook), else a local Delta session."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if on_databricks():
        return SparkSession.builder.getOrCreate()
    return local_spark(app_name=app_name)


def local_spark(
    app_name: str = "banking_etl_local",
    warehouse_dir: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> "SparkSession":
    """Local SparkSession with Delta Lake enabled (requires ``delta-spark``)."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("BANKING_ETL_SPARK_MASTER", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.jars.repositories", os.environ.get("BANKING_ETL_MAVEN_REPO", MAVEN_MIRROR))
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()
