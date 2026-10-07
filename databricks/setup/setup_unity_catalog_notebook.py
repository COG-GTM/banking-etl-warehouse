# Databricks notebook source
# MAGIC %md
# MAGIC # Banking ETL - Unity Catalog setup
# MAGIC Idempotently creates the bronze/silver/gold/ops schemas, the landing volume (and its
# MAGIC standard folders) and the `account users` grants. Parameters come from widgets / job
# MAGIC `base_parameters` (`catalog`, `schema_prefix`, `landing_volume`, `grant_principal`).

# COMMAND ----------

import json
import os
import sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
_setup_dir = os.path.dirname(_nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path)
for _p in (_setup_dir, os.path.join(os.path.dirname(_setup_dir), "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from banking_etl.common.config import EnvConfig  # noqa: E402
from setup_unity_catalog import setup_with_spark  # noqa: E402

cfg = EnvConfig.from_widgets(dbutils)  # noqa: F821
report = setup_with_spark(spark, cfg, unity_catalog=True, mkdirs=dbutils.fs.mkdirs)  # noqa: F821
print(json.dumps(report, indent=2, default=str))

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{cfg.catalog}` LIKE '{cfg.schema_prefix}*'"))  # noqa: F821

# COMMAND ----------

expected = {cfg.schema_name(layer) for layer in cfg.layers}
missing = expected - set(report["schemas"])
assert not missing, f"Missing schemas: {sorted(missing)}"
dbutils.notebook.exit(json.dumps({"schemas": report["schemas"], "landing": cfg.landing_path}))  # noqa: F821
