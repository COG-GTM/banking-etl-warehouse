# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest Transactions from CSV
# MAGIC
# MAGIC **Source:** `transaction_csv.csv` (legacy flat-file feed)
# MAGIC
# MAGIC **Target:** `bronze.transactions_csv` Delta table
# MAGIC
# MAGIC This notebook reads the raw CSV transaction file and persists it as a Delta table
# MAGIC in the Bronze layer, preserving the original schema and data exactly as-is.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configurable paths -- update these to match your workspace
VOLUME_PATH = "/Volumes/banking_dwh/raw_data/source_files"
BRONZE_CATALOG = "banking_dwh"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = "transactions_csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw CSV

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

# Define schema explicitly to match legacy source
csv_schema = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("branch_id", IntegerType(), True),
    ]
)

df_csv = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(csv_schema)
    .load(f"{VOLUME_PATH}/transaction_csv.csv")
)

print(f"CSV records loaded: {df_csv.count()}")
df_csv.printSchema()
df_csv.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import col, count, when, lit
from datetime import datetime

# Null checks on key columns
null_counts = df_csv.select(
    count(when(col("transaction_id").isNull(), True)).alias("null_transaction_id"),
    count(when(col("account_id").isNull(), True)).alias("null_account_id"),
    count(when(col("amount").isNull(), True)).alias("null_amount"),
)
null_counts.show()

# Record metadata for lineage
total_records = df_csv.count()
print(f"Total records from CSV source: {total_records}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

# Add ingestion metadata columns
df_bronze = df_csv.withColumn("_ingestion_timestamp", current_timestamp()).withColumn(
    "_source_file", lit("transaction_csv.csv")
)

# Write as Delta table (overwrite for full refresh; switch to merge for incremental)
df_bronze.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}")

print(f"Bronze table {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME} written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Table

# COMMAND ----------

spark.sql(
    f"SELECT COUNT(*) AS row_count FROM {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"
).show()
spark.sql(
    f"SELECT * FROM {BRONZE_CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME} LIMIT 5"
).show(truncate=False)
