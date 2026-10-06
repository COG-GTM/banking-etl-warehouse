"""Shared helpers for the banking DWH Databricks jobs.

Parameters resolve in this order: Databricks widget (notebook / job parameter),
``--name value`` command-line argument (spark_python_task), environment variable
(upper-cased name, for local runs), then the default.

Credentials are read from a Databricks secret scope. When ``dbutils`` is not available
(local runs) the secret falls back to an environment variable of the same name upper-cased
(e.g. key ``sqlserver-user`` -> ``SQLSERVER_USER``). Nothing is hard-coded.
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULTS = {
    "catalog": "",
    "target_schema": "dwh",
    "secret_scope": "banking-etl",
    "jdbc_host_key": "sqlserver-host",
    "jdbc_user_key": "sqlserver-user",
    "jdbc_password_key": "sqlserver-password",
    "jdbc_port": "1433",
    "jdbc_database": "sample",
    "jdbc_source_schema": "dbo",
    "jdbc_extra_options": "encrypt=true;trustServerCertificate=true",
    "write_mode": "merge",
}


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(spark)
    except Exception:
        pass
    try:
        import IPython  # type: ignore

        return IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        return None


def _cli_arg(name: str):
    flag = f"--{name}"
    argv = sys.argv[1:]
    for i, arg in enumerate(argv):
        if arg == flag and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith(flag + "="):
            return arg.split("=", 1)[1]
    return None


def get_param(name: str, default=None, spark: SparkSession = None) -> str:
    if default is None:
        default = DEFAULTS.get(name, "")
    dbutils = get_dbutils(spark or get_spark())
    if dbutils is not None:
        try:
            dbutils.widgets.text(name, str(default))
            value = dbutils.widgets.get(name)
            if value != "":
                return value
        except Exception:
            pass
    value = _cli_arg(name)
    if value is not None:
        return value
    return os.environ.get(name.upper(), default)


def get_secret(key: str, spark: SparkSession = None) -> str:
    spark = spark or get_spark()
    scope = get_param("secret_scope", spark=spark)
    dbutils = get_dbutils(spark)
    if dbutils is not None:
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            pass
    env_name = key.upper().replace("-", "_")
    value = os.environ.get(env_name)
    if value is None:
        raise RuntimeError(f"Secret '{key}' not found in scope '{scope}' and env var {env_name} is not set")
    return value


def qualified(table: str, spark: SparkSession = None) -> str:
    catalog = get_param("catalog", spark=spark)
    schema = get_param("target_schema", spark=spark)
    return ".".join(p for p in (catalog, schema, table) if p)


def jdbc_url(spark: SparkSession = None) -> str:
    spark = spark or get_spark()
    host = get_param("jdbc_host", spark=spark) or get_secret(get_param("jdbc_host_key", spark=spark), spark)
    port = get_param("jdbc_port", spark=spark)
    database = get_param("jdbc_database", spark=spark)
    extra = get_param("jdbc_extra_options", spark=spark)
    url = f"jdbc:sqlserver://{host}:{port};databaseName={database}"
    return f"{url};{extra}" if extra else url


def read_jdbc_table(table: str, spark: SparkSession = None) -> DataFrame:
    """Read ``<jdbc_source_schema>.<table>`` from the source SQL Server (Talend tMSSqlInput)."""
    spark = spark or get_spark()
    source_schema = get_param("jdbc_source_schema", spark=spark)
    dbtable = table if "." in table else f"{source_schema}.{table}"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url(spark))
        .option("dbtable", dbtable)
        .option("user", get_secret(get_param("jdbc_user_key", spark=spark), spark))
        .option("password", get_secret(get_param("jdbc_password_key", spark=spark), spark))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )


def align_to_target(df: DataFrame, target: str, spark: SparkSession = None) -> DataFrame:
    """Select and cast ``df`` columns to the target Delta table's column order and types."""
    spark = spark or get_spark()
    schema = spark.table(target).schema
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema])


def write_delta(df: DataFrame, target: str, key: str, spark: SparkSession = None) -> None:
    """Write to an existing Delta table: ``merge`` upserts on ``key``, ``overwrite`` replaces rows."""
    spark = spark or get_spark()
    mode = get_param("write_mode", spark=spark).lower()
    df = align_to_target(df, target, spark)
    if mode == "overwrite":
        df.write.insertInto(target, overwrite=True)
    elif mode == "merge":
        from delta.tables import DeltaTable

        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        raise ValueError(f"write_mode must be 'merge' or 'overwrite', got '{mode}'")
    print(f"{target}: wrote {df.count()} rows ({mode}); table now has {spark.table(target).count()} rows")
