# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — SQL Server ingestion
# MAGIC Replaces the `tMSSqlInput` components of `Load_FactTransaction`,
# MAGIC `Load_DimAccount`, `Load_DimCustomer` and `Load_DimBranch`.
# MAGIC
# MAGIC One append-only Delta table per source table in the `sample` database.
# MAGIC Credentials come from `dbutils.secrets`; nothing is hard-coded.

# COMMAND ----------

import sys

sys.path.append("../..")

from bronze.config import BRONZE_SCHEMA, CATALOG, JDBC_SOURCES  # noqa: E402
from bronze.jdbc_ingest import (  # noqa: E402
    JdbcConnection,
    build_bounds_query,
    build_jdbc_options,
    dbutils_secret_resolver,
    ingest_jdbc_source,
    read_jdbc,
)
from bronze.metadata import new_batch_id  # noqa: E402
from bronze.writer import write_bronze_batch  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("sql_host", "", "SQL Server host")
dbutils.widgets.text("database", "sample", "Source database")
dbutils.widgets.text("secret_scope", "banking-etl", "Secret scope")
dbutils.widgets.text("tables", ",".join(JDBC_SOURCES), "Tables (comma separated)")
dbutils.widgets.dropdown("load_mode", "full_refresh", ["full_refresh", "incremental"], "Load mode")
dbutils.widgets.text("watermark_value", "", "Watermark (incremental only)")
dbutils.widgets.text("num_partitions", "4", "Partitions for keyed reads")

connection = JdbcConnection(
    host=dbutils.widgets.get("sql_host"),
    database=dbutils.widgets.get("database"),
    secret_scope=dbutils.widgets.get("secret_scope"),
)
tables = [name.strip() for name in dbutils.widgets.get("tables").split(",") if name.strip()]
incremental = dbutils.widgets.get("load_mode") == "incremental"
watermark_value = dbutils.widgets.get("watermark_value").strip() or None
num_partitions = int(dbutils.widgets.get("num_partitions"))
secret_resolver = dbutils_secret_resolver(dbutils)
batch_id = new_batch_id()

# COMMAND ----------


def partition_bounds(source, watermark):
    """Fetch MIN/MAX of the partition key so the read can be split."""
    bounds_options = build_jdbc_options(connection, source, secret_resolver)
    bounds_options["dbtable"] = (
        f"({build_bounds_query(source, watermark_value=watermark)}) AS bounds"
    )
    row = read_jdbc(spark, bounds_options).first()
    if row is None or row["lower_bound"] is None:
        return None, None
    return int(row["lower_bound"]), int(row["upper_bound"])


for name in tables:
    source = JDBC_SOURCES[name]
    watermark = watermark_value if incremental and source.watermark_column else None

    lower_bound = upper_bound = partitions = None
    if source.partition_column:
        lower_bound, upper_bound = partition_bounds(source, watermark)
        if lower_bound is not None and upper_bound > lower_bound:
            partitions = num_partitions
        else:
            lower_bound = upper_bound = None

    options = build_jdbc_options(
        connection,
        source,
        secret_resolver,
        watermark_value=watermark,
        num_partitions=partitions,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )
    df = ingest_jdbc_source(spark, source, options, batch_id)
    write_bronze_batch(df, table=source.full_table_name(CATALOG, BRONZE_SCHEMA))
    print(f"ingested {source.qualified_source} -> {source.target_table}")

# COMMAND ----------

print(f"batch_id={batch_id}")
