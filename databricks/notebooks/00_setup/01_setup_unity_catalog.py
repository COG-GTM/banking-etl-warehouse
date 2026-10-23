# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 1: Unity Catalog namespace setup
# MAGIC Creates (idempotently) the catalog (prod only), the `bronze` / `silver` / `gold` / `ops`
# MAGIC schemas, the `bronze.landing` and `ops.checkpoints` volumes and the landing folders
# MAGIC for CSV, Excel and SQL Server extracts. Optionally grants read access on silver/gold.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json

from banking_dwh.common.uc_setup import landing_folders, setup_statements

dbutils.widgets.dropdown("apply_grants", "false", ["true", "false"], "Apply grants")  # noqa: F821
apply_grants = dbutils.widgets.get("apply_grants") == "true"  # noqa: F821

executed = []
for stmt in setup_statements(cfg, apply_grants=apply_grants):
    print(stmt)
    spark.sql(stmt)  # noqa: F821
    executed.append(stmt)

for folder in landing_folders(cfg):
    dbutils.fs.mkdirs(folder)  # noqa: F821
    print("mkdirs", folder)

summary = {
    **cfg.as_dict(),
    "statements": len(executed),
    "schemas_present": [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}").collect()  # noqa: F821
                        if r[0].startswith(cfg.schema_prefix)],
    "volumes": {layer: [r[1] for r in spark.sql(f"SHOW VOLUMES IN {cfg.schema(layer)}").collect()]  # noqa: F821
                for layer in ("bronze", "ops")},
    "landing_folders": landing_folders(cfg),
}
dbutils.notebook.exit(json.dumps(summary))  # noqa: F821
