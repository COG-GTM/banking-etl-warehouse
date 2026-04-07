# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Build DimBranch
# MAGIC
# MAGIC **Replaces:** Talend job `Load_DimBranch`
# MAGIC
# MAGIC **Logic:** Direct load from source branch table with schema alignment to
# MAGIC the DWH DimBranch structure.
# MAGIC
# MAGIC **Source (Bronze):** `bronze.src_branch`
# MAGIC
# MAGIC **Target:** `silver.dim_branch` Delta table

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

df_branch = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.src_branch")
print(f"Source branches: {df_branch.count()}")
df_branch.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Align to DWH Schema
# MAGIC
# MAGIC Maps source columns to the DimBranch target schema defined in
# MAGIC `sql_scripts/01_create_tables.sql`. The Talend job `Load_DimBranch`
# MAGIC performs a simple passthrough mapping.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df_dim_branch = df_branch.select(
    col("branch_id").alias("BranchID"),
    col("branch_name").alias("BranchName"),
    col("branch_location").alias("BranchLocation"),
)

print("DimBranch preview:")
df_dim_branch.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import count, when

quality_checks = df_dim_branch.select(
    count("*").alias("total_rows"),
    count(when(col("BranchID").isNull(), True)).alias("null_branch_id"),
    count(when(col("BranchName").isNull(), True)).alias("null_branch_name"),
)
quality_checks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

df_silver = df_dim_branch.withColumn("_transform_timestamp", current_timestamp())

df_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_branch")

print(f"Silver table {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_branch written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS row_count FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_branch").show()
spark.sql(f"SELECT * FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_branch LIMIT 5").show(truncate=False)
