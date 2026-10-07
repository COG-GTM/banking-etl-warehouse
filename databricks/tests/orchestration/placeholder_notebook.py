# Databricks notebook source
# Lightweight stand-in for a banking_etl_pipeline task notebook, used by live_submit.py to exercise the
# Workflow DAG on serverless before the real task notebooks (owned by other tickets) are merged.
import json
import time
from datetime import datetime, timezone

dbutils.widgets.text("task_name", "unknown")  # noqa: F821
dbutils.widgets.text("catalog", "migration_demo")  # noqa: F821
dbutils.widgets.text("schema_prefix", "banking_mig_")  # noqa: F821
dbutils.widgets.text("run_date", "")  # noqa: F821
dbutils.widgets.text("sleep_seconds", "5")  # noqa: F821

task_name = dbutils.widgets.get("task_name")  # noqa: F821
params = {k: dbutils.widgets.get(k) for k in ("catalog", "schema_prefix", "run_date")}  # noqa: F821

started = datetime.now(timezone.utc)
print(f"[banking_etl_pipeline] task={task_name} started={started.isoformat()} params={params}")
spark.sql("SELECT 1").collect()  # noqa: F821 - touch the serverless Spark session
time.sleep(int(dbutils.widgets.get("sleep_seconds")))  # noqa: F821
finished = datetime.now(timezone.utc)
print(f"[banking_etl_pipeline] task={task_name} finished={finished.isoformat()}")

dbutils.notebook.exit(  # noqa: F821
    json.dumps({"task": task_name, "started": started.isoformat(), "finished": finished.isoformat(), **params})
)
