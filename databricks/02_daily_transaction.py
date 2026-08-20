# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Transaction Summary (replaces `sp_DailyTransaction`)
# MAGIC
# MAGIC Aggregates `FactTransaction` by transaction day between `start_date` and
# MAGIC `end_date` (inclusive) and adds a configurable moving average that smooths the
# MAGIC daily `TotalAmount` series (default: trailing 7 days).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dwh", "Schema")
dbutils.widgets.text("start_date", "2024-01-18", "Start date (yyyy-MM-dd)")
dbutils.widgets.text("end_date", "2024-01-20", "End date (yyyy-MM-dd)")
dbutils.widgets.text("window_size", "7", "Moving average window (days)")

# COMMAND ----------

from dwh_analytics import daily_transaction

result = daily_transaction(
    spark,
    start_date=dbutils.widgets.get("start_date"),
    end_date=dbutils.widgets.get("end_date"),
    window_size=int(dbutils.widgets.get("window_size")),
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pure Spark SQL equivalent
# MAGIC
# MAGIC ```sql
# MAGIC WITH daily AS (
# MAGIC   SELECT
# MAGIC     CAST(TransactionDate AS DATE) AS Date,
# MAGIC     COUNT(TransactionID)          AS TotalTransactions,
# MAGIC     SUM(Amount)                   AS TotalAmount
# MAGIC   FROM ${catalog}.${schema}.FactTransaction
# MAGIC   WHERE CAST(TransactionDate AS DATE) BETWEEN DATE('${start_date}') AND DATE('${end_date}')
# MAGIC   GROUP BY CAST(TransactionDate AS DATE)
# MAGIC )
# MAGIC SELECT
# MAGIC   Date,
# MAGIC   TotalTransactions,
# MAGIC   TotalAmount,
# MAGIC   CAST(AVG(TotalAmount) OVER (
# MAGIC     ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC   ) AS DECIMAL(19,4)) AS SmoothedTotalAmount
# MAGIC FROM daily
# MAGIC ORDER BY Date;
# MAGIC ```
