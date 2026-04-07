# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Build DimCustomer
# MAGIC
# MAGIC **Replaces:** Talend job `Load_DimCustomer`
# MAGIC
# MAGIC **Logic:** LEFT OUTER JOIN of customer + city + state tables, with UPPERCASE
# MAGIC normalization on CustomerName, CityName, and StateName.
# MAGIC
# MAGIC **Source (Bronze):**
# MAGIC - `bronze.src_customer`
# MAGIC - `bronze.src_city`
# MAGIC - `bronze.src_state`
# MAGIC
# MAGIC **Target:** `silver.dim_customer` Delta table

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
# MAGIC ## Read Bronze Source Tables

# COMMAND ----------

df_customer = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.src_customer")
df_city = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.src_city")
df_state = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.src_state")

print(f"Customers: {df_customer.count()}")
print(f"Cities:    {df_city.count()}")
print(f"States:    {df_state.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Join + Uppercase Normalization
# MAGIC
# MAGIC This replicates the Talend `tMap` component in `Load_DimCustomer`:
# MAGIC - LEFT OUTER JOIN customer -> city (on city_id)
# MAGIC - LEFT OUTER JOIN city -> state (on state_id)
# MAGIC - UPPER() on customer_name, city_name, state_name

# COMMAND ----------

from pyspark.sql.functions import col, upper, current_timestamp

# Replicate the Talend tMap: customer LEFT JOIN city LEFT JOIN state
df_dim_customer = (
    df_customer.alias("c")
    .join(df_city.alias("ci"), col("c.city_id") == col("ci.city_id"), "left")
    .join(df_state.alias("s"), col("ci.state_id") == col("s.state_id"), "left")
    .select(
        col("c.customer_id").alias("CustomerID"),
        upper(col("c.customer_name")).alias("CustomerName"),
        col("c.address").alias("Address"),
        upper(col("ci.city_name")).alias("CityName"),
        upper(col("s.state_name")).alias("StateName"),
        col("c.age").alias("Age"),
        col("c.gender").alias("Gender"),
        col("c.email").alias("Email"),
    )
)

print("DimCustomer preview (after join + uppercase normalization):")
df_dim_customer.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import count, when

quality_checks = df_dim_customer.select(
    count("*").alias("total_rows"),
    count(when(col("CustomerID").isNull(), True)).alias("null_customer_id"),
    count(when(col("CustomerName").isNull(), True)).alias("null_customer_name"),
    count(when(col("CityName").isNull(), True)).alias("null_city_name"),
    count(when(col("StateName").isNull(), True)).alias("null_state_name"),
)
quality_checks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

df_silver = df_dim_customer.withColumn("_transform_timestamp", current_timestamp())

df_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_customer")

print(f"Silver table {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_customer written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS row_count FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_customer").show()
spark.sql(f"SELECT * FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.dim_customer LIMIT 5").show(truncate=False)
