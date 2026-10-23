# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 2: star schema data-quality gate
# MAGIC Runs PK not-null / uniqueness checks and the referential-integrity checks that replace the
# MAGIC unenforced foreign keys, appends results to `ops.dq_results`, and fails the task if any check
# MAGIC fails (`fail_on_error=true`). Ticket 10 wires this in after `Load_FactTransaction`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json
import uuid

from banking_dwh.dq.checks import assert_all_passed, results_as_dicts, run_star_schema_checks, write_results

dbutils.widgets.text("run_id", "", "Run id (defaults to a uuid)")  # noqa: F821
dbutils.widgets.dropdown("fail_on_error", "true", ["true", "false"], "Fail on error")  # noqa: F821
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())  # noqa: F821

results = run_star_schema_checks(spark, cfg)  # noqa: F821
write_results(spark, cfg, results, run_id)  # noqa: F821
for r in results:
    print(("PASS" if r.passed else "FAIL"), r.check_name, r.table_name, r.failed_rows)
if dbutils.widgets.get("fail_on_error") == "true":  # noqa: F821
    assert_all_passed(results)
dbutils.notebook.exit(json.dumps({"run_id": run_id, "results": results_as_dicts(results)}))  # noqa: F821
