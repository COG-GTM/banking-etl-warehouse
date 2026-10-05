# Databricks notebook source
# MAGIC %md
# MAGIC # gold.dim_branch (ticket 5, Talend `Load_DimBranch`)
# MAGIC SCD-1 merge of `silver.branch` into `gold.dim_branch` on `BranchID` via
# MAGIC `banking_etl.gold.merge.merge_scd1`: inserts new branches, updates changed
# MAGIC `BranchName`/`BranchLocation` in place, never deletes, never writes `BranchKey`.
# MAGIC Creates the gold star schema first if `dim_branch` is missing (Talend `CREATE_IF_NOT_EXISTS`).
# MAGIC Widgets: `catalog`, `schema_prefix`. Logic: `src/banking_etl/dims/branch.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.dims.branch import load_dim_branch

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
result = load_dim_branch(spark, settings)  # noqa: F821
print(json.dumps(result, indent=2))

# COMMAND ----------

spark.table(result["target"]).orderBy("BranchID").show(truncate=False)  # noqa: F821
dbutils.notebook.exit(json.dumps(result))  # noqa: F821
