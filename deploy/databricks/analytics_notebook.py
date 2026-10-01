# Databricks notebook source
# MAGIC %md
# MAGIC # Banking DWH analytics
# MAGIC Interactive replacements for the legacy `sp_DailyTransaction` and `sp_BalancePerCustomer`
# MAGIC stored procedures. Install the `banking_etl` wheel on the cluster (deployed by the bundle).

# COMMAND ----------

dbutils.widgets.text("catalog", "banking")  # noqa: F821
dbutils.widgets.text("schema", "dwh")  # noqa: F821
dbutils.widgets.text("start_date", "2024-01-18")  # noqa: F821
dbutils.widgets.text("end_date", "2024-01-22")  # noqa: F821
dbutils.widgets.text("customer_name", "shelly")  # noqa: F821

# COMMAND ----------

from banking_etl.analytics import balance_per_customer, daily_transaction  # noqa: E402

prefix = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"  # noqa: F821
fact = spark.table(f"{prefix}.fact_transaction")  # noqa: F821
accounts = spark.table(f"{prefix}.dim_account")  # noqa: F821
customers = spark.table(f"{prefix}.dim_customer")  # noqa: F821

# COMMAND ----------

# EXEC sp_DailyTransaction @start_date, @end_date
display(  # noqa: F821
    daily_transaction(fact, dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date"))  # noqa: F821
)

# COMMAND ----------

# EXEC sp_BalancePerCustomer @customer_name
display(  # noqa: F821
    balance_per_customer(fact, accounts, customers, dbutils.widgets.get("customer_name"))  # noqa: F821
)
