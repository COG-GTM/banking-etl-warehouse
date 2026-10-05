# Databricks notebook source
# MAGIC %md
# MAGIC # gold.dim_customer (ticket 7, Talend `Load_DimCustomer` -> DWH.DimCustomer)
# MAGIC SCD-1 `MERGE` of `silver.customer` into `gold.dim_customer` on `CustomerID` via
# MAGIC `banking_etl.gold.merge.merge_scd1` (changed rows updated, new rows inserted, no deletes;
# MAGIC `CustomerKey` identity never written, so it is stable). Creates the gold star schema first if
# MAGIC `gold.dim_customer` is missing (Talend `CREATE_IF_NOT_EXISTS`).
# MAGIC Widgets: `catalog`, `schema_prefix`. Run `silver_customer` first.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.dims.customer import load_dim_customer

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
result = load_dim_customer(spark, settings)  # noqa: F821
print(result)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))  # noqa: F821
