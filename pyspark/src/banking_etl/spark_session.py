"""SparkSession factory for Databricks, EKS (spark-submit) and local runs."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

DEFAULT_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.1",
    "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11",
)


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str) -> SparkSession:
    """Return a Delta-enabled SparkSession.

    On Databricks the runtime session is reused as-is. Elsewhere Delta Lake is enabled
    explicitly; jars come from ``BANKING_ETL_JARS`` (comma-separated local jars), from the
    image classpath when ``BANKING_ETL_RESOLVE_PACKAGES=false`` (container), or are
    resolved from Maven via ``spark.jars.packages``.
    """
    if is_databricks():
        return SparkSession.builder.appName(app_name).getOrCreate()

    session_tz = os.environ.get("SPARK_SESSION_TIMEZONE", "UTC")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", session_tz)
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={session_tz}")
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={session_tz}")
    )
    jars = os.environ.get("BANKING_ETL_JARS", "")
    if jars:
        builder = builder.config("spark.jars", jars)
    elif os.environ.get("BANKING_ETL_RESOLVE_PACKAGES", "true").lower() == "true":
        packages = os.environ.get("BANKING_ETL_SPARK_PACKAGES", ",".join(DEFAULT_PACKAGES))
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()
