# Databricks notebook source
# MAGIC %md
# MAGIC # silver.transaction -> gold.fact_transaction (ticket 8, Talend `Load_FactTransaction`)
# MAGIC Full refresh (Talend `TRUNCATE` + `INSERT`). `AccountKey` / `BranchKey` are looked up from
# MAGIC `gold.dim_account` / `gold.dim_branch`; rows whose AccountID/BranchID has no dim match (the legacy
# MAGIC FKs rejected them) replace the FK rows of `ops.fact_transaction_rejects`. Run after the dim loads.
# MAGIC
# MAGIC Widgets: `catalog`, `schema_prefix`. Logic: `src/banking_etl/facts/transaction.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.facts.transaction import load_fact_transaction

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
print(settings)

# COMMAND ----------

result = load_fact_transaction(spark, settings)  # noqa: F821
print(json.dumps(result, indent=2))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))  # noqa: F821
