# Databricks notebook source
# MAGIC %md
# MAGIC # silver.branch (ticket 5, Talend `Load_DimBranch`)
# MAGIC Rebuilds `silver.branch` from the `bronze.sqlserver_branch` snapshot (overwrite). Values pass
# MAGIC through unchanged: the Talend tMap applies no trim/case change.
# MAGIC Widgets: `catalog`, `schema_prefix`. Logic: `src/banking_etl/dims/branch.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.dims.branch import load_silver_branch

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
result = load_silver_branch(spark, settings)  # noqa: F821
print(f"{result['target']} <- {result['source']}: {result['rows']} rows")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))  # noqa: F821
