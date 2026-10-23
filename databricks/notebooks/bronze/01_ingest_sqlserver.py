# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 3: SQL Server `sample` DB (JDBC) → Bronze
# MAGIC Replaces the Talend `tMSSqlInput` components. Reads `branch`, `account`, `customer`, `city`, `state`,
# MAGIC `transaction_db` (never `sysdiagrams`) and full-refreshes `bronze.sqlserver_<table>` per batch with the
# MAGIC contract lineage columns `_ingested_at` / `_batch_id`.
# MAGIC
# MAGIC * `source=jdbc`: Spark JDBC (`com.microsoft.sqlserver.jdbc.SQLServerDriver`), credentials from secret scope
# MAGIC   `banking-etl-sqlserver`; with `stage=true` each extract is also written as parquet to
# MAGIC   `bronze/landing/sqlserver/<table>/`.
# MAGIC * `source=staged`: reads previously staged parquet from that landing path (for compute that cannot reach
# MAGIC   SQL Server; stage locally with `python -m banking_dwh.bronze.sqlserver extract`).
# MAGIC * `assert_counts=true` fails the run if row counts differ from the `sample.bak` baseline
# MAGIC   (branch 5, state 9, city 52, customer 20, account 21, transaction_db 10).

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.bronze.sqlserver import EXPECTED_ROW_COUNTS, SOURCES, ingest, new_batch_id, resolve_tables
from banking_dwh.common.config import jdbc_options

dbutils.widgets.dropdown("source", "jdbc", list(SOURCES), "Source")  # noqa: F821
dbutils.widgets.text("tables", "all", "Tables (comma list or 'all')")  # noqa: F821
dbutils.widgets.dropdown("stage", "true", ["true", "false"], "Stage JDBC extract as parquet")  # noqa: F821
dbutils.widgets.dropdown("assert_counts", "false", ["true", "false"], "Assert sample.bak row counts")  # noqa: F821
dbutils.widgets.text("batch_id", "", "Batch id (default: generated)")  # noqa: F821

source = dbutils.widgets.get("source")  # noqa: F821
tables = resolve_tables(cfg, dbutils.widgets.get("tables"))  # noqa: F821
batch_id = dbutils.widgets.get("batch_id") or new_batch_id()  # noqa: F821

# COMMAND ----------

summary = ingest(
    spark,  # noqa: F821
    cfg,
    source=source,
    jdbc=jdbc_options(cfg, dbutils) if source == "jdbc" else None,  # noqa: F821
    tables=tables,
    batch_id=batch_id,
    stage=dbutils.widgets.get("stage") == "true",  # noqa: F821
    expected_counts=EXPECTED_ROW_COUNTS,
    assert_counts=dbutils.widgets.get("assert_counts") == "true",  # noqa: F821
)

# COMMAND ----------

# Evidence for the run output: Bronze schemas and a sample of the fact source.
summary["bronze_schemas"] = {
    t["bronze_table"]: [f"{f.name}:{f.dataType.simpleString()}" for f in spark.table(t["bronze_table"]).schema.fields]  # noqa: F821
    for t in summary["tables"]
}
txn = next((t["bronze_table"] for t in summary["tables"] if t["source_table"] == "transaction_db"), None)
if txn:
    summary["transaction_db_sample"] = [
        {k: str(v) for k, v in r.asDict().items()}
        for r in spark.table(txn).orderBy("transaction_id").limit(3).collect()  # noqa: F821
    ]
summary["config"] = cfg.as_dict()
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))  # noqa: F821
