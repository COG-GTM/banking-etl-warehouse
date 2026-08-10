# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: SQL Server `sample` database (TICKET-4)
# MAGIC
# MAGIC Reads the operational source tables of the SQL Server `sample` database
# MAGIC (formerly restored from `data_sources/sample.bak`) over JDBC and lands them
# MAGIC **as-is** as Delta tables in `dwh.bronze`.
# MAGIC
# MAGIC This replaces the source (`tMSSqlInput`) side of the Talend jobs
# MAGIC `Load_DimBranch`, `Load_DimAccount`, `Load_DimCustomer` and
# MAGIC `Load_FactTransaction`. No cleansing, joining, deduplication or type
# MAGIC coercion happens here - those belong to the silver and gold layers.
# MAGIC
# MAGIC Connection details are supplied through widgets; the password is read from
# MAGIC a Databricks secret scope so that no credential is stored in the notebook.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md ## Parameters

dbutils.widgets.text("jdbc_host", "", "SQL Server host")
dbutils.widgets.text("jdbc_port", "1433", "SQL Server port")
dbutils.widgets.text("jdbc_database", "sample", "Source database")
dbutils.widgets.text("source_schema", "dbo", "Source schema")
dbutils.widgets.text("jdbc_user", "", "SQL Server user")
dbutils.widgets.text("secret_scope", "banking-etl", "Secret scope")
dbutils.widgets.text("password_secret_key", "sqlserver-password", "Password secret key")
dbutils.widgets.text("catalog", "dwh", "Unity Catalog catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")

JDBC_HOST = dbutils.widgets.get("jdbc_host").strip()
JDBC_PORT = dbutils.widgets.get("jdbc_port").strip()
JDBC_DATABASE = dbutils.widgets.get("jdbc_database").strip()
SOURCE_SCHEMA = dbutils.widgets.get("source_schema").strip()
JDBC_USER = dbutils.widgets.get("jdbc_user").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
PASSWORD_SECRET_KEY = dbutils.widgets.get("password_secret_key").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()

if not JDBC_HOST:
    raise ValueError("Widget 'jdbc_host' is required")
if not JDBC_USER:
    raise ValueError("Widget 'jdbc_user' is required")

JDBC_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key=PASSWORD_SECRET_KEY)

JDBC_URL = (
    f"jdbc:sqlserver://{JDBC_HOST}:{JDBC_PORT};"
    f"databaseName={JDBC_DATABASE};encrypt=true;trustServerCertificate=true"
)

JDBC_OPTIONS = {
    "url": JDBC_URL,
    "user": JDBC_USER,
    "password": JDBC_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Source tables of the `sample` database consumed by the Talend jobs.
SOURCE_TABLES = [
    "account",
    "branch",
    "customer",
    "city",
    "state",
    "transaction_db",
]

# COMMAND ----------

# MAGIC %md ## Helpers


def read_source_table(table: str) -> DataFrame:
    """Read a source table over JDBC exactly as it is stored in SQL Server."""
    return (
        spark.read.format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", f"{SOURCE_SCHEMA}.{table}")
        .load()
    )


def land_to_bronze(table: str) -> int:
    """Overwrite the bronze Delta table for `table` and return the row count."""
    df = read_source_table(table).withColumn(
        "_ingested_at", F.current_timestamp()
    ).withColumn(
        "_source_system", F.lit(f"sqlserver:{JDBC_DATABASE}.{SOURCE_SCHEMA}.{table}")
    )

    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    return spark.table(target).count()


# COMMAND ----------

# MAGIC %md ## Ingest

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

results = {table: land_to_bronze(table) for table in SOURCE_TABLES}

for table, row_count in results.items():
    print(f"{CATALOG}.{BRONZE_SCHEMA}.{table}: {row_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Landed tables: `dwh.bronze.account`, `dwh.bronze.branch`,
# MAGIC `dwh.bronze.customer`, `dwh.bronze.city`, `dwh.bronze.state`,
# MAGIC `dwh.bronze.transaction_db`.
# MAGIC
# MAGIC The CSV and Excel transaction feeds are ingested by a separate bronze job
# MAGIC and joined into the fact table in a later ticket.

dbutils.notebook.exit(str(results))
