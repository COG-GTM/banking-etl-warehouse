# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 8: seed a scratch namespace for the fact_transaction live run
# MAGIC Loads the `fixtures/t8_*.csv` exports of the real sources into `<prefix>bronze` (SQL 10, Excel 7,
# MAGIC CSV 12 rows) and the `dim_account` (21) / `dim_branch` (5) rows needed for RI into `<prefix>silver`.
# MAGIC Only prefixes ending in `t8_` are accepted (see `t8_seed.check_seed_target`); never touches gold.

# COMMAND ----------

import json
import os
import sys

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
nb_path = nb_path if nb_path.startswith("/Workspace") else "/Workspace" + nb_path
FIXTURES = nb_path.rsplit("/", 1)[0]
ROOT = FIXTURES.rsplit("/fixtures", 1)[0]
for p in (os.path.join(ROOT, "src"), FIXTURES):
    if p not in sys.path:
        sys.path.insert(0, p)

dbutils.widgets.dropdown("target", "dev", ["dev", "prod"], "Target")  # noqa: F821
dbutils.widgets.text("catalog", "", "Catalog override")  # noqa: F821
dbutils.widgets.text("schema_prefix", "bdwh_t8_", "Schema prefix (must end in t8_)")  # noqa: F821

from banking_dwh.common.config import EnvConfig  # noqa: E402
import t8_seed  # noqa: E402

cfg = EnvConfig.from_widgets(dbutils)  # noqa: F821
t8_seed.check_seed_target(cfg)
counts = t8_seed.seed_scratch(spark, cfg, fixtures_dir=FIXTURES)  # noqa: F821
print(counts)
dbutils.notebook.exit(json.dumps({**cfg.as_dict(), "seeded": counts}))  # noqa: F821
