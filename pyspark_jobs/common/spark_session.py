"""Shared SparkSession factory configured for the SQL Server JDBC driver."""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import MSSQL_JDBC_DRIVER, EtlConfig


def _require_jdbc_driver(spark: SparkSession) -> None:
    jvm = spark._jvm
    try:
        jvm.java.lang.Class.forName(
            MSSQL_JDBC_DRIVER, True, jvm.java.lang.Thread.currentThread().getContextClassLoader()
        )
    except Exception as exc:  # py4j wraps the ClassNotFoundException
        raise RuntimeError(
            f"{MSSQL_JDBC_DRIVER} is not on the Spark classpath. Under spark-submit pass "
            "--packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 (or --jars <mssql-jdbc.jar>), "
            "or use pyspark_jobs/bin/submit.sh which adds it for you."
        ) from exc


def get_spark(app_name: str, config: EtlConfig | None = None) -> SparkSession:
    """Builds (or reuses) a SparkSession with the SQL Server JDBC driver on the classpath.

    If the driver is already supplied via ``spark-submit --jars/--packages`` the builder
    settings below are ignored by Spark, so the same code works under ``spark-submit`` and
    plain ``python``.
    """
    config = config or EtlConfig.from_env()
    settings = config.spark
    builder = SparkSession.builder.appName(f"{settings.app_name_prefix}-{app_name}")
    if settings.master:
        builder = builder.master(settings.master)

    packages = []
    if settings.jars:
        builder = builder.config("spark.jars", settings.jars)
    elif settings.jdbc_packages:
        packages.append(settings.jdbc_packages)
    if config.files.excel_reader == "crealytics":
        packages.append(settings.excel_package)
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    if settings.repositories:
        builder = builder.config("spark.jars.repositories", settings.repositories)

    spark = builder.getOrCreate()
    _require_jdbc_driver(spark)

    # JDBC timestamps are converted with the JVM default zone; keep Spark's session zone
    # aligned with it so to_date(TransactionDate) matches CAST(... AS DATE) in SQL Server.
    timezone = settings.session_timezone or spark._jvm.java.util.TimeZone.getDefault().getID()
    spark.conf.set("spark.sql.session.timeZone", timezone)
    return spark
