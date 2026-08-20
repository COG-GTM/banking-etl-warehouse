# Databricks notebook source
# MAGIC %md
# MAGIC # Balance Per Customer (replaces `sp_BalancePerCustomer`)
# MAGIC
# MAGIC Current balance of every **active** account belonging to customers whose name
# MAGIC matches the search term. Deposits increase the balance, every other transaction
# MAGIC type decreases it.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dwh", "Schema")
dbutils.widgets.text("customer_name", "", "Customer name (substring match)")

# COMMAND ----------

from dwh_analytics import balance_per_customer

result = balance_per_customer(
    spark,
    customer_name=dbutils.widgets.get("customer_name"),
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pure Spark SQL equivalent
# MAGIC
# MAGIC ```sql
# MAGIC WITH TransactionSummary AS (
# MAGIC   SELECT
# MAGIC     AccountID,
# MAGIC     SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
# MAGIC       AS TotalTransactionAmount
# MAGIC   FROM ${catalog}.${schema}.FactTransaction
# MAGIC   GROUP BY AccountID
# MAGIC )
# MAGIC SELECT
# MAGIC   c.CustomerName,
# MAGIC   a.AccountType,
# MAGIC   a.Balance AS InitialBalance,
# MAGIC   CAST(a.Balance + COALESCE(ts.TotalTransactionAmount, 0) AS DECIMAL(19,4)) AS CurrentBalance
# MAGIC FROM ${catalog}.${schema}.DimCustomer c
# MAGIC JOIN ${catalog}.${schema}.DimAccount a ON c.CustomerID = a.CustomerID
# MAGIC LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
# MAGIC WHERE c.CustomerName LIKE '%${customer_name}%'
# MAGIC   AND a.Status = 'active';
# MAGIC ```
