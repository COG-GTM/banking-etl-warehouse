# Databricks notebook source
# MAGIC %md
# MAGIC # Load_DimBranch -> silver.branch -> gold.dim_branch (SCD-1 MERGE)
# MAGIC Port of the Talend job `Load_DimBranch`. Reads `bronze.sqlserver_branch`, writes the typed/trimmed/deduped
# MAGIC `silver.branch` snapshot and MERGEs it on the business key into `gold.dim_branch`.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("catalog", "migration_demo")
dbutils.widgets.text("schema_prefix", "banking_mig_")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("steps", "silver,gold")
dbutils.widgets.text("src_path", "")

src_path = dbutils.widgets.get("src_path") or os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

from banking_etl.dims import branch as dim  # noqa: E402
from banking_etl.dims.branch import resolve_layers  # noqa: E402

layers = resolve_layers(
    catalog=dbutils.widgets.get("catalog"),
    schema_prefix=dbutils.widgets.get("schema_prefix"),
    bronze_schema=dbutils.widgets.get("bronze_schema") or None,
    silver_schema=dbutils.widgets.get("silver_schema") or None,
    gold_schema=dbutils.widgets.get("gold_schema") or None,
)
steps = [s.strip() for s in dbutils.widgets.get("steps").split(",") if s.strip()]
result = dim.run(spark, layers, steps)
result["layers"] = layers.__dict__
print(json.dumps(result, indent=2))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))
