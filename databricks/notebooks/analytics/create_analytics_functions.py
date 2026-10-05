# Databricks notebook source
# MAGIC %md
# MAGIC # Create analytics table functions (ticket 9)
# MAGIC Replaces the DWH stored procedures (`sql_scripts/02_create_procedures.sql`) with Unity Catalog SQL
# MAGIC table-valued functions in the gold schema, from `sql/analytics/*.sql` (idempotent `CREATE OR REPLACE`):
# MAGIC
# MAGIC | T-SQL | Databricks SQL |
# MAGIC |---|---|
# MAGIC | `EXEC sp_DailyTransaction '2024-01-18', '2024-01-20'` | `SELECT * FROM <catalog>.<prefix>gold.fn_daily_transaction(DATE'2024-01-18', DATE'2024-01-20')` |
# MAGIC | `EXEC sp_BalancePerCustomer 'shelly'` | `SELECT * FROM <catalog>.<prefix>gold.fn_balance_per_customer('shelly')` |
# MAGIC
# MAGIC Widgets: `catalog`, `schema_prefix`. Requires the gold tables (ticket 4).

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.analytics.functions import FUNCTIONS, apply_analytics_functions, function_name
from banking_etl.config import settings_from_widgets

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
apply_analytics_functions(spark, settings)  # noqa: F821
names = [function_name(settings, fn) for fn in FUNCTIONS]

# COMMAND ----------

for name in names:
    spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}").show(100, truncate=False)  # noqa: F821

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"functions": names}))  # noqa: F821
