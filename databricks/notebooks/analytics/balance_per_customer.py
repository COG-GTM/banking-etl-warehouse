# Databricks notebook source
# MAGIC %md
# MAGIC # Balance per customer (ticket 9) - `EXEC sp_BalancePerCustomer @customer_name`
# MAGIC Ad-hoc front end for `gold.fn_balance_per_customer`: initial and current balance of every active
# MAGIC account whose customer name contains `customer_name` (case-insensitive; `%` / `_` are LIKE wildcards;
# MAGIC empty = every customer). Widgets: `catalog`, `schema_prefix`, `customer_name`.
# MAGIC Run `create_analytics_functions` first.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.analytics.functions import balance_per_customer
from banking_etl.config import settings_from_widgets

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
dbutils.widgets.text("customer_name", "shelly", "Customer name contains")  # noqa: F821
customer_name = dbutils.widgets.get("customer_name")  # noqa: F821

result = balance_per_customer(spark, settings, customer_name)  # noqa: F821
display(result)  # noqa: F821

# COMMAND ----------

rows = [r.asDict() for r in result.collect()]
dbutils.notebook.exit(json.dumps({"rows": rows}, default=str))  # noqa: F821
