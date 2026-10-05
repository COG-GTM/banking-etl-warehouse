# Databricks notebook source
# MAGIC %md
# MAGIC # bronze.sqlserver_account -> silver.account (ticket 6, Talend `Load_DimAccount`)
# MAGIC Typed, conformed account snapshot: `balance` int -> DECIMAL(19,4), `date_opened` datetime2 -> DATE,
# MAGIC strings passed through unchanged (no trim / re-casing, as in the Talend tMap). Full overwrite.
# MAGIC Fails on NULL or duplicate `account_id`.
# MAGIC
# MAGIC Widgets: `catalog`, `schema_prefix`. Logic: `src/banking_etl/dims/account.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.dims.account import load_silver_account

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
print(settings)

# COMMAND ----------

result = load_silver_account(spark, settings)  # noqa: F821
print(f"{result['target']}: {result['rows']} rows <- {result['source']}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))  # noqa: F821
