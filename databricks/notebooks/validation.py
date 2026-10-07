# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 10 — Legacy DWH vs Delta gold reconciliation + dry-run cutover
# MAGIC
# MAGIC Runs `docs/cutover_runbook.md` end to end against a ticket-scoped schema:
# MAGIC seed legacy snapshots + sources -> historical load -> freeze -> final incremental ->
# MAGIC **reconcile gate (100% match)** -> switch consumers (or rollback) -> rollback rehearsal ->
# MAGIC Talend decommission checklist.
# MAGIC
# MAGIC Results: `<catalog>.<prefix>ops.reconciliation_results` and `<catalog>.<prefix>ops.cutover_log`.
# MAGIC Set `mode=reconcile_only` to reconcile already-loaded gold tables (e.g. `banking_mig_gold`) without
# MAGIC re-running the cutover steps.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("catalog", "migration_demo")  # noqa: F821
dbutils.widgets.text("schema_prefix", "banking_mig_")  # noqa: F821
dbutils.widgets.text("work_suffix", "t10")  # noqa: F821
dbutils.widgets.text("ops_suffix", "ops")  # noqa: F821
dbutils.widgets.text("fixtures_path", "/Volumes/migration_demo/banking_mig_t10/fixtures/legacy_dwh")  # noqa: F821
dbutils.widgets.text("src_path", "")  # noqa: F821
dbutils.widgets.dropdown("mode", "dry_run_cutover", ["dry_run_cutover", "reconcile_only"])  # noqa: F821

w = {k: dbutils.widgets.get(k) for k in  # noqa: F821
     ("catalog", "schema_prefix", "work_suffix", "ops_suffix", "fixtures_path", "src_path", "mode")}

src_path = w["src_path"] or os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from banking_etl.validation import cutover  # noqa: E402
from banking_etl.validation.tables import Schemas  # noqa: E402

names = Schemas(catalog=w["catalog"] or None, schema_prefix=w["schema_prefix"],
                work_suffix=w["work_suffix"], ops_suffix=w["ops_suffix"])
print(json.dumps({**w, "src_path": src_path, "work_schema": names.work, "ops_schema": names.ops}, indent=2))

# COMMAND ----------

if w["mode"] == "dry_run_cutover":
    report = cutover.dry_run_cutover(spark, names, w["fixtures_path"])  # noqa: F821
    summary = report.summary()
    recon = report.reconciliation
else:
    recon = cutover.run_reconciliation(spark, names, run_label="reconcile_only")  # noqa: F821
    summary = {"reconciliation": recon.summary()}
print(json.dumps(summary, indent=2, default=str))

# COMMAND ----------

# MAGIC %md ## Reconciliation report (this run)

# COMMAND ----------

results = spark.table(names.ops_table(cutover.RESULTS_TABLE)).filter(f"run_id = '{recon.run_id}'")  # noqa: F821
display(results.groupBy("table_name", "check_type", "status").count()  # noqa: F821
        .orderBy("table_name", "check_type"))

# COMMAND ----------

display(results.filter("record_type = 'mismatch' OR status != 'PASS'"))  # noqa: F821

# COMMAND ----------

if w["mode"] == "dry_run_cutover":
    display(spark.table(names.ops_table(cutover.CUTOVER_LOG_TABLE))  # noqa: F821
            .filter(f"cutover_id = '{report.cutover_id}'").orderBy("step_no"))

# COMMAND ----------

if not recon.green:
    raise AssertionError(f"Reconcile gate RED: {len(recon.failed)} failed checks, run_id={recon.run_id}")
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
