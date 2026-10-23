# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 8: `silver.fact_transaction` (Talend `Load_FactTransaction`)
# MAGIC Bronze `sqlserver_transaction_db` (latest batch) + `file_transaction_excel` + `file_transaction_csv`
# MAGIC → conform → `unionByName` (tUnite) → dedup on `transaction_id`, first in SQL / Excel / CSV order
# MAGIC (tUniqRow) → Delta MERGE (inserts, changed rows, deletes keys gone from the sources, i.e. the
# MAGIC TRUNCATE + INSERT result) → Ticket 2 PK / FK checks into `ops.dq_results`.
# MAGIC
# MAGIC * `orphan_policy=quarantine` (default): rows violating `FK_FactTransaction_DimAccount` /
# MAGIC   `FK_FactTransaction_DimBranch` go to `ops.fact_transaction_rejects` (legacy SQL Server rejected them
# MAGIC   on INSERT) and are reported as `<FK>.quarantined` DQ results. `load`: load them, RI check fails.
# MAGIC   `fail`: abort before writing.
# MAGIC * `fail_on_dq=true`: fail the task if a post-load PK/FK check fails. `fail_on_quarantine=true`: also
# MAGIC   fail when any row was quarantined.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json
import uuid

from banking_dwh.dq.checks import DataQualityError
from banking_dwh.silver.fact_transaction import ORPHAN_POLICIES, dq_failures, load_fact_transaction

dbutils.widgets.text("run_id", "", "Run id (defaults to a uuid)")  # noqa: F821
dbutils.widgets.dropdown("orphan_policy", "quarantine", list(ORPHAN_POLICIES), "Orphan FK rows")  # noqa: F821
dbutils.widgets.dropdown("fail_on_dq", "true", ["true", "false"], "Fail on PK/FK check failure")  # noqa: F821
dbutils.widgets.dropdown("fail_on_quarantine", "false", ["true", "false"], "Fail if rows were quarantined")  # noqa: F821

run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())  # noqa: F821
summary = load_fact_transaction(
    spark, cfg, run_id=run_id, orphan_policy=dbutils.widgets.get("orphan_policy")  # noqa: F821
)
print(json.dumps(summary, indent=2, default=str))

# COMMAND ----------

failed = dq_failures(summary, include_quarantine=dbutils.widgets.get("fail_on_quarantine") == "true")  # noqa: F821
if failed and (dbutils.widgets.get("fail_on_dq") == "true" or dbutils.widgets.get("fail_on_quarantine") == "true"):  # noqa: F821
    raise DataQualityError(f"fact_transaction DQ failed: {failed}")
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
