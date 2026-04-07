# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Build DimAccount
# MAGIC
# MAGIC **Replaces:** Talend job `Load_DimAccount`
# MAGIC
# MAGIC **Logic:** Direct load from source account table with schema alignment to
# MAGIC the DWH DimAccount structure.
# MAGIC
# MAGIC **Source (Bronze):** `bronze.src_account`
# MAGIC
# MAGIC **Target:** `silver.dim_account` Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BRONZE_CATALOG = "banking_dwh"
BRONZE_SCHEMA = "bronze"
SILVER_CATALOG = "banking_dwh"
SILVER_SCHEMA = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Source

# COMMAND ----------

df_account = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.src_account")
print(f"Source accounts: {df_account.count()}")
df_account.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Align to DWH Schema
# MAGIC
# MAGIC Maps source columns to the DimAccount target schema defined in
# MAGIC `sql_scripts/01_create_tables.sql`. The Talend job `Load_DimAccount`
# MAGIC performs a simple passthrough mapping.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df_dim_account = df_account.select(
    col("account_id").alias("AccountID"),
    col("customer_id").alias("CustomerID"),
    col("account_type").alias("AccountType"),
    col("balance").cast("double").alias("Balance"),
    col("date_opened").alias("DateOpened"),
    col("status").alias("Status"),
)

print("DimAccount preview:")
df_dim_account.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import count, when

quality_checks = df_dim_account.select(
    count("*").alias("total_rows"),
    count(when(col("AccountID").isNull(), True)).alias("null_account_id"),
    count(when(col("CustomerID").isNull(), True)).alias("null_customer_id"),
    count(when(col("Balance").isNull(), True)).alias("null_balance"),
    count(when(col("Status").isNull(), True)).alias("null_status"),
)
quality_checks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

df_silver = df_dim_account.withColumn("_transform_timestamp", current_timestamp())

df_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_account")

print(f"Silver table {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_account written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS row_count FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_account").show()
spark.sql(f"SELECT * FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_account LIMIT 5").show(truncate=False)
