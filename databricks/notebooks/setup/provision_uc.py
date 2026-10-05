# Databricks notebook source
# MAGIC %md
# MAGIC # Provision Unity Catalog (ticket 1)
# MAGIC Idempotently creates the `bronze`/`silver`/`gold`/`ops` schemas (honouring `schema_prefix`), the
# MAGIC `bronze.landing` and `ops.checkpoints` volumes, landing folders, and grants. `CREATE CATALOG` runs
# MAGIC only when `create_catalog=true` and degrades to a warning without metastore admin rights.
# MAGIC SQL lives in `sql/setup/*.sql`; logic in `src/banking_etl/setup/provision.py`.

# COMMAND ----------

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath("../../src"))

from banking_etl.config import settings_from_widgets
from banking_etl.setup.provision import ProvisionOptions, landing_dirs, parse_bool, provision, summarize

settings = settings_from_widgets(dbutils)  # noqa: F821
for name in ("create_catalog", "data_engineers", "jobs_principal"):
    dbutils.widgets.text(name, "false" if name == "create_catalog" else "")  # noqa: F821
options = ProvisionOptions(
    create_catalog=parse_bool(dbutils.widgets.get("create_catalog")),  # noqa: F821
    data_engineers=dbutils.widgets.get("data_engineers").strip(),  # noqa: F821
    jobs_principal=dbutils.widgets.get("jobs_principal").strip(),  # noqa: F821
)
print(settings, options)

# COMMAND ----------

results = provision(spark.sql, settings, options, sql_dir=Path(os.path.abspath("../../sql/setup")))  # noqa: F821
print(summarize(results))

for path in landing_dirs(settings):
    os.makedirs(path, exist_ok=True)
    print("landing dir", path)

# COMMAND ----------

for layer in ("bronze", "silver", "gold", "ops"):
    display(spark.sql(f"DESCRIBE SCHEMA {settings.schema(layer)}"))  # noqa: F821
display(spark.sql(f"SHOW VOLUMES IN {settings.schema('bronze')}"))  # noqa: F821
display(spark.sql(f"SHOW VOLUMES IN {settings.schema('ops')}"))  # noqa: F821
