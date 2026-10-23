# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 10: seed a scratch namespace for reconciliation validation
# MAGIC Live-validation harness only (not part of the pipeline job). Loads the Silver tables of a
# MAGIC ticket scratch namespace (e.g. `schema_prefix=bdwh_t10_`) from `fixtures/parity/`, builds T9
# MAGIC stand-in Gold objects from that Silver data, then optionally applies `break` scenarios
# MAGIC (`fact_row_missing`, `balance_drift`, `customer_not_upcased`) so `01_reconcile` must fail.
# MAGIC Refuses prod and the shared `bdwh_` dev namespace.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.common.config import validate_identifier
from banking_dwh.orchestration.parity_seed import assert_scratch, break_scenarios, create_gold_standins, seed_silver

dbutils.widgets.text("break", "", "Comma-separated break scenarios (blank = clean)")  # noqa: F821
scenarios = [validate_identifier(x.strip(), "scenario") for x in dbutils.widgets.get("break").split(",") if x.strip()]  # noqa: F821

assert_scratch(cfg)  # noqa: F821
counts = seed_silver(spark, cfg)  # noqa: F821
gold_objects = create_gold_standins(spark, cfg)  # noqa: F821
applied = break_scenarios(spark, cfg, scenarios)  # noqa: F821
out = {"silver_rows": counts, "gold_standins": gold_objects, "break": applied, "schemas": cfg.as_dict()["schemas"]}
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))  # noqa: F821
