"""JDBC read/write helpers shared by the ETL jobs."""

import logging

from pyspark.sql import DataFrame, SparkSession

from etl.config import JdbcConfig

log = logging.getLogger(__name__)


def read_table(spark: SparkSession, db: JdbcConfig, table: str) -> DataFrame:
    return spark.read.format("jdbc").options(**db.options()).option("dbtable", table).load()


def read_query(spark: SparkSession, db: JdbcConfig, query: str) -> DataFrame:
    return spark.read.format("jdbc").options(**db.options()).option("query", query).load()


def execute(spark: SparkSession, db: JdbcConfig, sql: str) -> int:
    """Run a single DML/DDL statement on the database via the JVM JDBC driver."""
    jvm = spark.sparkContext._jvm
    # Registers the driver from Spark's classloader (spark.jars / spark.jars.packages).
    jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(db.driver)
    conn = jvm.java.sql.DriverManager.getConnection(db.url, db.user, db.password)
    try:
        stmt = conn.createStatement()
        try:
            return stmt.executeUpdate(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def clear_table(spark: SparkSession, db: JdbcConfig, table: str) -> None:
    """Empty a DWH table while keeping its DDL (PK/FK constraints).

    TRUNCATE is not allowed on tables referenced by a foreign key (DimAccount, DimBranch),
    and Spark's JDBC overwrite would drop and recreate the table without constraints.
    """
    deleted = execute(spark, db, f"DELETE FROM {table}")
    log.info("Cleared %s (%d rows deleted)", table, deleted)


def write_table(spark: SparkSession, df: DataFrame, db: JdbcConfig, table: str, mode: str) -> int:
    """Write df to table. mode='overwrite' empties the table first, then appends."""
    if mode == "overwrite":
        clear_table(spark, db, table)
    rows = df.count()
    df.write.format("jdbc").options(**db.options()).option("dbtable", table).mode("append").save()
    log.info("Wrote %d rows to %s (%s)", rows, table, mode)
    return rows
