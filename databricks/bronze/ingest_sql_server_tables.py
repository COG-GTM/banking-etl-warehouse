# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest SQL Server Source Tables
# MAGIC
# MAGIC **Source:** SQL Server `sample` database (restored from `sample.bak`)
# MAGIC
# MAGIC **Target:** Bronze Delta tables for each source table:
# MAGIC - `bronze.src_customer`
# MAGIC - `bronze.src_city`
# MAGIC - `bronze.src_state`
# MAGIC - `bronze.src_account`
# MAGIC - `bronze.src_branch`
# MAGIC - `bronze.src_transaction`
# MAGIC
# MAGIC In the legacy system, these tables lived in SQL Server and were accessed directly
# MAGIC by Talend ETL jobs. For the Databricks migration, we simulate this by reading
# MAGIC from exported CSV files placed in a Unity Catalog Volume.
# MAGIC
# MAGIC **Note:** In a production migration, this notebook could be replaced with a
# MAGIC direct JDBC connection to SQL Server using Databricks' built-in connector.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

VOLUME_PATH = "/Volumes/banking_dwh/raw_data/source_files"
BRONZE_CATALOG = "banking_dwh"
BRONZE_SCHEMA = "bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType


def ingest_source_table(file_name, table_name, schema=None):
    """Read a CSV export of a SQL Server table and write it as a Bronze Delta table."""
    reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")

    if schema:
        reader = reader.schema(schema)

    df = reader.load(f"{VOLUME_PATH}/{file_name}")

    # Add ingestion metadata
    df_bronze = (
        df.withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_source_file", lit(file_name))
    )

    df_bronze.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}")

    row_count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    ).collect()[0]["cnt"]
    print(f"  -> {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}: {row_count} rows ingested")
    return df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customer Table
# MAGIC Columns: customer_id, customer_name, address, city_id, age, gender, email

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city_id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
])

print("Ingesting customer table...")
ingest_source_table("src_customer.csv", "src_customer", customer_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest City Table
# MAGIC Columns: city_id, city_name, state_id

# COMMAND ----------

city_schema = StructType([
    StructField("city_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("state_id", IntegerType(), True),
])

print("Ingesting city table...")
ingest_source_table("src_city.csv", "src_city", city_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest State Table
# MAGIC Columns: state_id, state_name

# COMMAND ----------

state_schema = StructType([
    StructField("state_id", IntegerType(), True),
    StructField("state_name", StringType(), True),
])

print("Ingesting state table...")
ingest_source_table("src_state.csv", "src_state", state_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Account Table
# MAGIC Columns: account_id, customer_id, account_type, balance, date_opened, status

# COMMAND ----------

account_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("date_opened", DateType(), True),
    StructField("status", StringType(), True),
])

print("Ingesting account table...")
ingest_source_table("src_account.csv", "src_account", account_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Branch Table
# MAGIC Columns: branch_id, branch_name, branch_location

# COMMAND ----------

branch_schema = StructType([
    StructField("branch_id", IntegerType(), True),
    StructField("branch_name", StringType(), True),
    StructField("branch_location", StringType(), True),
])

print("Ingesting branch table...")
ingest_source_table("src_branch.csv", "src_branch", branch_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Transaction Table (SQL Server source)
# MAGIC Columns: transaction_id, account_id, transaction_date, amount, transaction_type, branch_id

# COMMAND ----------

transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("branch_id", IntegerType(), True),
])

print("Ingesting transaction table (SQL Server source)...")
ingest_source_table("src_transaction.csv", "src_transaction", transaction_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n=== Bronze Layer Ingestion Complete ===")
tables = [
    "src_customer", "src_city", "src_state",
    "src_account", "src_branch", "src_transaction",
]
for t in tables:
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{t}").collect()[0]["cnt"]
    print(f"  {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{t}: {cnt} rows")
