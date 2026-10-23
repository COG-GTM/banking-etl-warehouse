# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 6: Silver `dim_branch` (Talend `Load_DimBranch`)
# MAGIC Reads Bronze `sqlserver_branch`, keeps the latest row per natural key and runs an SCD-1 Delta `MERGE INTO`
# MAGIC Silver `dim_branch` (update changed rows, insert new keys, leave unchanged rows alone), then the
# MAGIC Ticket 2 primary-key checks. Talend did a plain INSERT with `DIE_ON_ERROR=false`; the MERGE is a
# MAGIC deliberate improvement so reruns are idempotent and source changes reach the DWH.
# MAGIC
# MAGIC Widgets: `target` / `catalog` / `schema_prefix` (from `00_config`), `run_id` (DQ run id, defaults
# MAGIC to a uuid), `write_dq_results` (append to `ops.dq_results`), `fail_on_error` (fail the task on a DQ failure).

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json
import uuid

from banking_dwh.silver import dim_branch

dbutils.widgets.text("run_id", "", "Run id (defaults to a uuid)")  # noqa: F821
dbutils.widgets.dropdown("write_dq_results", "true", ["true", "false"], "Write DQ results")  # noqa: F821
dbutils.widgets.dropdown("fail_on_error", "true", ["true", "false"], "Fail on DQ error")  # noqa: F821
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())  # noqa: F821

summary = dim_branch.load(
    spark,  # noqa: F821
    cfg,  # noqa: F821
    dq_run_id=run_id if dbutils.widgets.get("write_dq_results") == "true" else None,  # noqa: F821
    fail_on_error=dbutils.widgets.get("fail_on_error") == "true",  # noqa: F821
)
for r in summary["dq"]:
    print(("PASS" if r["passed"] else "FAIL"), r["check_name"], r["failed_rows"])
print({k: v for k, v in summary.items() if k != "dq"})
dbutils.notebook.exit(json.dumps({"run_id": run_id, **summary}, default=str))  # noqa: F821
