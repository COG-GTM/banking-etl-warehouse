# Databricks notebook source
# MAGIC %md
# MAGIC # banking_dwh: shared configuration (Ticket 1)
# MAGIC
# MAGIC `%run ../00_setup/00_config` from any notebook to get:
# MAGIC * `cfg`: `banking_dwh.common.config.EnvConfig` for the selected target.
# MAGIC * The standard widgets: `target` (dev | prod), `catalog` and `schema_prefix` (overrides; `-` = no prefix).
# MAGIC
# MAGIC Naming conventions are documented in `src/banking_dwh/common/config.py` and `databricks/README.md`.

# COMMAND ----------

import os
import sys


def _bundle_root() -> str:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
    # .../databricks/notebooks/<group>/<notebook> -> .../databricks
    root = "/Workspace" + nb_path if not nb_path.startswith("/Workspace") else nb_path
    return root.rsplit("/notebooks/", 1)[0]


BUNDLE_ROOT = _bundle_root()
SRC = os.path.join(BUNDLE_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# COMMAND ----------

dbutils.widgets.dropdown("target", "dev", ["dev", "prod"], "Target")  # noqa: F821
dbutils.widgets.text("catalog", "", "Catalog override")  # noqa: F821
dbutils.widgets.text("schema_prefix", "", "Schema prefix override ('-' = none)")  # noqa: F821

from banking_dwh.common.config import EnvConfig  # noqa: E402

cfg = EnvConfig.from_widgets(dbutils)  # noqa: F821
print(cfg.as_dict())
