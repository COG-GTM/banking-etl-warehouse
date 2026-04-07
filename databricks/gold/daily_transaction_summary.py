# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Daily Transaction Summary
# MAGIC
# MAGIC **Replaces:** Stored Procedure `sp_DailyTransaction`
# MAGIC
# MAGIC **Logic:** Aggregates transactions by date with count and sum of amounts.
# MAGIC The legacy stored procedure accepted `@start_date` and `@end_date` parameters;
# MAGIC this Gold table materializes the full summary and consumers can filter by date
# MAGIC at query time using standard SQL WHERE clauses.
# MAGIC
# MAGIC **Source (Silver):** `silver.fact_transaction`
# MAGIC
# MAGIC **Target:** `gold.daily_transaction_summary` Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SILVER_CATALOG = "banking_dwh"
SILVER_SCHEMA = "silver"
GOLD_CATALOG = "banking_dwh"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Fact Table

# COMMAND ----------

df_fact = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction")
print(f"Total transactions: {df_fact.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Aggregate by Date
# MAGIC
# MAGIC Replicates the core logic of `sp_DailyTransaction`:
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     CAST(TransactionDate AS DATE) AS [Date],
# MAGIC     COUNT(TransactionID) AS TotalTransactions,
# MAGIC     SUM(Amount) AS TotalAmount
# MAGIC FROM FactTransaction
# MAGIC GROUP BY CAST(TransactionDate AS DATE)
# MAGIC ORDER BY [Date]
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, to_date, current_timestamp

df_daily_summary = (
    df_fact
    .withColumn("TransactionDateOnly", to_date(col("TransactionDate")))
    .groupBy("TransactionDateOnly")
    .agg(
        count("TransactionID").alias("TotalTransactions"),
        spark_sum("Amount").alias("TotalAmount"),
    )
    .withColumnRenamed("TransactionDateOnly", "Date")
    .orderBy("Date")
)

print("Daily Transaction Summary preview:")
df_daily_summary.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import when

quality_checks = df_daily_summary.select(
    count("*").alias("total_days"),
    count(when(col("Date").isNull(), True)).alias("null_dates"),
    count(when(col("TotalTransactions") <= 0, True)).alias("zero_txn_days"),
    spark_sum("TotalTransactions").alias("grand_total_txns"),
    spark_sum("TotalAmount").alias("grand_total_amount"),
)
quality_checks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Delta Table

# COMMAND ----------

df_gold = df_daily_summary.withColumn("_refresh_timestamp", current_timestamp())

df_gold.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.daily_transaction_summary")

print(f"Gold table {GOLD_CATALOG}.{GOLD_SCHEMA}.daily_transaction_summary written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Example usage (equivalent to legacy stored procedure call):
# MAGIC ```sql
# MAGIC -- Legacy: EXEC sp_DailyTransaction @start_date='2024-01-18', @end_date='2024-01-20'
# MAGIC -- Databricks equivalent:
# MAGIC SELECT * FROM banking_dwh.gold.daily_transaction_summary
# MAGIC WHERE Date BETWEEN '2024-01-18' AND '2024-01-20'
# MAGIC ORDER BY Date
# MAGIC ```

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS row_count FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.daily_transaction_summary").show()
spark.sql(f"SELECT * FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.daily_transaction_summary ORDER BY Date").show(truncate=False)
