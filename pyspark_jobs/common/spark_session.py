"""SparkSession factory with the SQL Server JDBC driver on the classpath."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

DEFAULT_MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
DEFAULT_SPARK_EXCEL_PACKAGE = "com.crealytics:spark-excel_2.12:3.5.1_0.20.4"


def _packages() -> list[str]:
    packages = [os.environ.get("MSSQL_JDBC_PACKAGE", DEFAULT_MSSQL_JDBC_PACKAGE)]
    if os.environ.get("EXCEL_READER", "pandas").lower() == "crealytics":
        packages.append(os.environ.get("SPARK_EXCEL_PACKAGE", DEFAULT_SPARK_EXCEL_PACKAGE))
    extra = os.environ.get("SPARK_EXTRA_PACKAGES")
    if extra:
        packages.extend(p.strip() for p in extra.split(",") if p.strip())
    return [p for p in packages if p]


def get_spark(app_name: str = "banking-etl-warehouse") -> SparkSession:
    """Return a SparkSession configured for SQL Server JDBC access.

    Driver resolution, in order of precedence:
      * ``MSSQL_JDBC_JAR`` – path(s) to a local mssql-jdbc jar (comma separated),
        added via ``spark.jars``; no network access required.
      * otherwise ``spark.jars.packages`` resolves ``MSSQL_JDBC_PACKAGE`` from
        Maven (``SPARK_JARS_REPOSITORIES`` adds mirror repositories).

    These only take effect when the JVM is started from Python (``python job.py``).
    Under ``spark-submit`` pass ``--jars``/``--packages`` on the command line instead.
    """
    builder = SparkSession.builder.appName(app_name)

    master = os.environ.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)

    local_jars = os.environ.get("MSSQL_JDBC_JAR")
    if local_jars:
        builder = builder.config("spark.jars", local_jars)
        packages = [p for p in _packages() if not p.startswith("com.microsoft.sqlserver:")]
    else:
        packages = _packages()
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    repositories = os.environ.get("SPARK_JARS_REPOSITORIES")
    if repositories:
        builder = builder.config("spark.jars.repositories", repositories)

    builder = (
        builder.config("spark.sql.session.timeZone", os.environ.get("SPARK_TIMEZONE", "UTC"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
