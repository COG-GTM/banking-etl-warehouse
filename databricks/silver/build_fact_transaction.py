# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Build FactTransaction
# MAGIC
# MAGIC **Replaces:** Talend job `Load_FactTransaction`
# MAGIC
# MAGIC **Logic:**
# MAGIC 1. **Union** (tUnite) three transaction streams: SQL Server, CSV, and Excel
# MAGIC 2. **Deduplicate** (tUniqRow) by `transaction_id` — keep first occurrence
# MAGIC 3. **Parse** transaction_date to proper TimestampType
# MAGIC
# MAGIC **Source (Bronze):**
# MAGIC - `bronze.src_transaction`  (SQL Server export)
# MAGIC - `bronze.transactions_csv` (CSV file)
# MAGIC - `bronze.transactions_excel` (Excel file)
# MAGIC
# MAGIC **Target:** `silver.fact_transaction` Delta table

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
# MAGIC ## Read All Three Transaction Sources

# COMMAND ----------

from pyspark.sql.functions import col, lit

# Source 1: SQL Server transactions (exported to CSV)
df_sql = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.src_transaction").select(
    col("transaction_id"),
    col("account_id"),
    col("transaction_date"),
    col("amount"),
    col("transaction_type"),
    col("branch_id"),
).withColumn("_source_system", lit("sql_server"))

# Source 2: CSV transactions
df_csv = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.transactions_csv").select(
    col("transaction_id"),
    col("account_id"),
    col("transaction_date"),
    col("amount"),
    col("transaction_type"),
    col("branch_id"),
).withColumn("_source_system", lit("csv_file"))

# Source 3: Excel transactions
df_excel = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.transactions_excel").select(
    col("transaction_id"),
    col("account_id"),
    col("transaction_date").cast("string").alias("transaction_date"),
    col("amount"),
    col("transaction_type"),
    col("branch_id"),
).withColumn("_source_system", lit("excel_file"))

print(f"SQL Server records:  {df_sql.count()}")
print(f"CSV records:         {df_csv.count()}")
print(f"Excel records:       {df_excel.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Union All Sources (replaces Talend tUnite)
# MAGIC
# MAGIC The Talend `tUnite` component merges rows from all three input streams
# MAGIC into a single data flow. PySpark `unionByName` is the equivalent.

# COMMAND ----------

df_union = df_sql.unionByName(df_csv).unionByName(df_excel)

print(f"Total records after union: {df_union.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplicate by transaction_id (replaces Talend tUniqRow)
# MAGIC
# MAGIC The Talend `tUniqRow` component keeps only the first occurrence of each
# MAGIC `transaction_id`. We replicate this using `dropDuplicates`.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Use window function to keep first occurrence (deterministic ordering)
window_spec = Window.partitionBy("transaction_id").orderBy(col("_source_system").desc())

df_dedup = (
    df_union.withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

duplicates_removed = df_union.count() - df_dedup.count()
print(f"Records after deduplication: {df_dedup.count()}")
print(f"Duplicates removed: {duplicates_removed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse Transaction Date and Align Schema

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, coalesce, current_timestamp

# The CSV source uses "dd-MM-yyyy HH:mm:ss" format, while SQL/Excel may vary
# Try multiple parse formats to handle all sources
df_fact = df_dedup.withColumn(
    "TransactionDate",
    coalesce(
        to_timestamp(col("transaction_date"), "dd-MM-yyyy HH:mm:ss"),
        to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("transaction_date"), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col("transaction_date")),
    ),
).select(
    col("transaction_id").alias("TransactionID"),
    col("account_id").alias("AccountID"),
    col("TransactionDate"),
    col("amount").alias("Amount"),
    col("transaction_type").alias("TransactionType"),
    col("branch_id").alias("BranchID"),
    col("_source_system"),
)

print("FactTransaction preview:")
df_fact.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import count, when

quality_checks = df_fact.select(
    count("*").alias("total_rows"),
    count(when(col("TransactionID").isNull(), True)).alias("null_txn_id"),
    count(when(col("AccountID").isNull(), True)).alias("null_account_id"),
    count(when(col("TransactionDate").isNull(), True)).alias("null_txn_date"),
    count(when(col("Amount").isNull(), True)).alias("null_amount"),
    count(when(col("BranchID").isNull(), True)).alias("null_branch_id"),
)
quality_checks.show()

# Check for any remaining duplicates
dup_check = df_fact.groupBy("TransactionID").count().filter(col("count") > 1)
print(f"Remaining duplicate transaction_ids: {dup_check.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

df_silver = df_fact.withColumn("_transform_timestamp", current_timestamp())

df_silver.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction")

print(f"Silver table {SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS row_count FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction").show()
spark.sql(f"SELECT * FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction LIMIT 5").show(truncate=False)

# Source breakdown
spark.sql(f"""
    SELECT _source_system, COUNT(*) AS record_count
    FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction
    GROUP BY _source_system
    ORDER BY _source_system
""").show()
