"""Shared SparkSession builder with the SQL Server JDBC driver and spark-excel on the classpath."""

from pyspark.sql import SparkSession

from etl.config import SparkConfig


def build_spark_session(cfg: SparkConfig) -> SparkSession:
    builder = (
        SparkSession.builder.appName(cfg.app_name)
        .master(cfg.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    if cfg.jars:
        builder = builder.config("spark.jars", cfg.jars)
    elif cfg.packages:
        builder = builder.config("spark.jars.packages", cfg.packages)
    if cfg.repositories:
        builder = builder.config("spark.jars.repositories", cfg.repositories)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
