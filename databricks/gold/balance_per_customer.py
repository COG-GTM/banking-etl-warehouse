# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Balance Per Customer
# MAGIC
# MAGIC **Replaces:** Stored Procedure `sp_BalancePerCustomer`
# MAGIC
# MAGIC **Logic:** Reconciles initial balance (from DimAccount) with transaction
# MAGIC activity to compute each active account's current balance. Deposits add to
# MAGIC the balance; all other transaction types (Withdrawal, Transfer, Payment)
# MAGIC subtract from it.
# MAGIC
# MAGIC The legacy stored procedure accepted `@customer_name` as a parameter;
# MAGIC this Gold table materializes the full result set and consumers can filter
# MAGIC by customer name at query time.
# MAGIC
# MAGIC **Source (Silver):**
# MAGIC - `silver.dim_customer`
# MAGIC - `silver.dim_account`
# MAGIC - `silver.fact_transaction`
# MAGIC
# MAGIC **Target:** `gold.balance_per_customer` Delta table

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
# MAGIC ## Read Silver Tables

# COMMAND ----------

df_customer = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_customer")
df_account = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_account")
df_fact = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.fact_transaction")

print(f"Customers:    {df_customer.count()}")
print(f"Accounts:     {df_account.count()}")
print(f"Transactions: {df_fact.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Compute Current Balance
# MAGIC
# MAGIC Replicates the core CTE logic of `sp_BalancePerCustomer`:
# MAGIC ```sql
# MAGIC WITH TransactionSummary AS (
# MAGIC     SELECT AccountID,
# MAGIC            SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount
# MAGIC                     ELSE -Amount END) AS TotalTransactionAmount
# MAGIC     FROM FactTransaction
# MAGIC     GROUP BY AccountID
# MAGIC )
# MAGIC SELECT c.CustomerName, a.AccountType, a.Balance AS InitialBalance,
# MAGIC        a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
# MAGIC FROM DimCustomer c
# MAGIC JOIN DimAccount a ON c.CustomerID = a.CustomerID
# MAGIC LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
# MAGIC WHERE a.Status = 'active'
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when, coalesce, lit, current_timestamp, lower

# Step 1: CTE equivalent — compute net transaction amount per account
# Deposit adds, everything else subtracts (matches legacy CASE WHEN logic)
df_txn_summary = (
    df_fact
    .withColumn(
        "signed_amount",
        when(lower(col("TransactionType")) == "deposit", col("Amount"))
        .otherwise(-col("Amount"))
    )
    .groupBy("AccountID")
    .agg(spark_sum("signed_amount").alias("TotalTransactionAmount"))
)

print("Transaction summary per account:")
df_txn_summary.show(10, truncate=False)

# COMMAND ----------

# Step 2: Join customer + account + transaction summary
# Filter to active accounts only (matches legacy WHERE a.Status = 'active')
df_balance = (
    df_customer.alias("c")
    .join(df_account.alias("a"), col("c.CustomerID") == col("a.CustomerID"), "inner")
    .join(df_txn_summary.alias("ts"), col("a.AccountID") == col("ts.AccountID"), "left")
    .filter(lower(col("a.Status")) == "active")
    .select(
        col("c.CustomerID"),
        col("c.CustomerName"),
        col("a.AccountID"),
        col("a.AccountType"),
        col("a.Balance").alias("InitialBalance"),
        # CurrentBalance = InitialBalance + ISNULL(TotalTransactionAmount, 0)
        (col("a.Balance") + coalesce(col("ts.TotalTransactionAmount"), lit(0))).alias(
            "CurrentBalance"
        ),
    )
)

print("Balance per customer preview:")
df_balance.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import count, min as spark_min, max as spark_max

quality_checks = df_balance.select(
    count("*").alias("total_active_accounts"),
    count(when(col("CustomerName").isNull(), True)).alias("null_customer_name"),
    count(when(col("InitialBalance").isNull(), True)).alias("null_initial_balance"),
    count(when(col("CurrentBalance").isNull(), True)).alias("null_current_balance"),
    spark_min("CurrentBalance").alias("min_balance"),
    spark_max("CurrentBalance").alias("max_balance"),
)
quality_checks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Delta Table

# COMMAND ----------

df_gold = df_balance.withColumn("_refresh_timestamp", current_timestamp())

df_gold.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.balance_per_customer")

print(f"Gold table {GOLD_CATALOG}.{GOLD_SCHEMA}.balance_per_customer written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Example usage (equivalent to legacy stored procedure call):
# MAGIC ```sql
# MAGIC -- Legacy: EXEC sp_BalancePerCustomer @customer_name = 'John'
# MAGIC -- Databricks equivalent:
# MAGIC SELECT * FROM banking_dwh.gold.balance_per_customer
# MAGIC WHERE CustomerName LIKE '%JOHN%'
# MAGIC ORDER BY CustomerName, AccountType
# MAGIC ```

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS row_count FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.balance_per_customer").show()
spark.sql(f"""
    SELECT * FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.balance_per_customer
    ORDER BY CustomerName, AccountType
""").show(truncate=False)
