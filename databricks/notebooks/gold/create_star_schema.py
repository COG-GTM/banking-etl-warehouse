# Databricks notebook source
# MAGIC %md
# MAGIC # Create gold star schema (ticket 4)
# MAGIC Creates `gold.dim_branch`, `gold.dim_account`, `gold.dim_customer`, `gold.fact_transaction` and
# MAGIC `ops.fact_transaction_rejects` from `sql/gold/*.sql` (idempotent `CREATE TABLE IF NOT EXISTS`).
# MAGIC Widgets: `catalog`, `schema_prefix`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.gold.ddl import apply_star_schema, ddl_parameters

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
executed = apply_star_schema(spark, settings)  # noqa: F821
print(f"Executed {len(executed)} statements")

# COMMAND ----------

for name in ddl_parameters(settings).values():
    spark.sql(f"DESCRIBE TABLE {name}").show(truncate=False)  # noqa: F821
