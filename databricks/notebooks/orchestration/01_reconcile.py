# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 10: reconciliation against the legacy SQL Server DWH
# MAGIC Compares every Silver table and Gold object with the legacy output captured in
# MAGIC `fixtures/parity/` (row counts, distinct keys, SUM of every money column, full-row diff),
# MAGIC appends one row per metric to `ops.reconciliation_results` and one row per object to
# MAGIC `ops.dq_results`, and fails the task on any mismatch (`fail_on_error=true`).
# MAGIC Last task of the `banking_dwh_pipeline` job.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json
import uuid

from banking_dwh.common.config import validate_identifier
from banking_dwh.dq import checks
from banking_dwh.dq.reconciliation import (
    FIXTURES_DIR, as_dq_results, assert_reconciled, default_specs, run_reconciliation, summarize, write_results,
)

dbutils.widgets.text("run_id", "", "Run id (defaults to a uuid)")  # noqa: F821
dbutils.widgets.text("layers", "silver,gold", "Layers to reconcile")  # noqa: F821
dbutils.widgets.dropdown("fail_on_error", "true", ["true", "false"], "Fail on mismatch")  # noqa: F821

run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())  # noqa: F821
layers = [validate_identifier(x.strip(), "layer") for x in dbutils.widgets.get("layers").split(",") if x.strip()]  # noqa: F821
if not layers:
    raise ValueError("At least one reconciliation layer (silver, gold) is required")

results = run_reconciliation(spark, cfg, default_specs(layers), FIXTURES_DIR)  # noqa: F821
write_results(spark, cfg, results, run_id)  # noqa: F821
checks.write_results(spark, cfg, as_dq_results(results), run_id)  # noqa: F821

for r in results:
    print("PASS" if r.passed else "FAIL", r.check_name, "expected", r.expected, "actual", r.actual)
summary = {"run_id": run_id, "results_table": cfg.table("ops", "reconciliation_results"), **summarize(results)}
print(json.dumps(summary, indent=2))

# COMMAND ----------

if dbutils.widgets.get("fail_on_error") == "true":  # noqa: F821
    assert_reconciled(results)
dbutils.notebook.exit(json.dumps(summary))  # noqa: F821
