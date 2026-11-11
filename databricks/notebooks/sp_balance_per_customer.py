# Databricks notebook source
# MAGIC %md
# MAGIC # sp_BalancePerCustomer
# MAGIC
# MAGIC Databricks port of the SQL Server procedure `sp_BalancePerCustomer`
# MAGIC (`sql_scripts/02_create_procedures.sql`).
# MAGIC
# MAGIC 1. Creates or replaces the Unity Catalog SQL table function
# MAGIC    `<catalog>.<gold_schema>.sp_balance_per_customer(p_customer_name)` and its helper
# MAGIC    `<catalog>.<gold_schema>.tsql_like_to_regex(pattern)`.
# MAGIC 2. Runs it for `customer_name`, cross-checks the result against the PySpark
# MAGIC    implementation, and displays it.
# MAGIC
# MAGIC Ad-hoc use after registration:
# MAGIC `SELECT * FROM banking_dwh.gold.sp_balance_per_customer('shelly')`

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from banking_etl.sp_balance_per_customer import (  # noqa: E402
    balance_per_customer,
    call_table_function,
    register_functions,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "banking_dwh")  # noqa: F821
dbutils.widgets.text("gold_schema", "gold")  # noqa: F821
dbutils.widgets.text("customer_name", "")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
gold_schema = dbutils.widgets.get("gold_schema")  # noqa: F821
customer_name = dbutils.widgets.get("customer_name")  # noqa: F821

# COMMAND ----------

function_name = register_functions(spark, catalog, gold_schema)  # noqa: F821
print(f"Registered {function_name}")

sql_result = call_table_function(spark, customer_name, catalog, gold_schema)  # noqa: F821
pyspark_result = balance_per_customer(spark, customer_name, catalog, gold_schema)  # noqa: F821

mismatches = sql_result.exceptAll(pyspark_result).count() + pyspark_result.exceptAll(sql_result).count()
if mismatches:
    raise AssertionError(f"SQL table function and PySpark results differ by {mismatches} rows")

display(sql_result)  # noqa: F821

# COMMAND ----------

dbutils.notebook.exit(json.dumps({  # noqa: F821
    "function": function_name,
    "customer_name": customer_name,
    "rows": sql_result.count(),
}))
