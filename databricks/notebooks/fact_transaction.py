# Databricks notebook source
# MAGIC %md
# MAGIC # Load_FactTransaction -> `silver.transaction` + `gold.fact_transaction`
# MAGIC
# MAGIC Port of the Talend job `Load_FactTransaction` (tUnite -> tUniqRow -> tMap -> tDBOutput).
# MAGIC
# MAGIC * **silver**: normalize `bronze.sqlserver_transaction_db`, `bronze.file_transaction_excel` and
# MAGIC   `bronze.file_transaction_csv`, quarantine unparseable rows, `unionByName`, and dedupe on
# MAGIC   `transaction_id` keeping the first row in Talend merge order (sqlserver > excel > csv).
# MAGIC * **gold**: send FK violations against `gold.dim_account` / `gold.dim_branch` to quarantine, then MERGE into
# MAGIC   `gold.fact_transaction`. With `delete_missing=true`, rows not in silver are deleted, which matches TRUNCATE + INSERT.
# MAGIC * Quarantine table: `ops.fact_transaction_rejects`. Each stage replaces only its own rows.
# MAGIC * Optional `parity_table`: the run fails unless gold equals it exactly.
# MAGIC
# MAGIC Set the `*_schema` widgets to point any layer at a different schema, e.g. the ticket-scoped `banking_mig_t7`.

# COMMAND ----------

import json
import os
import sys
import uuid

dbutils.widgets.text("catalog", "migration_demo")
dbutils.widgets.text("schema_prefix", "banking_mig_")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("ops_schema", "")
dbutils.widgets.text("dim_schema", "")
dbutils.widgets.text("steps", "silver,gold")
dbutils.widgets.text("delete_missing", "true")
dbutils.widgets.text("parity_table", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("src_path", "")

src_path = dbutils.widgets.get("src_path") or os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from banking_etl.facts import transaction as ft

w = {
    k: dbutils.widgets.get(k).strip()
    for k in (
        "catalog",
        "schema_prefix",
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "ops_schema",
        "dim_schema",
        "steps",
        "delete_missing",
        "parity_table",
        "run_id",
    )
}
run_id = w["run_id"] or uuid.uuid4().hex
tables = ft.resolve_tables(
    w["catalog"] or None,
    w["schema_prefix"],
    bronze_schema=w["bronze_schema"] or None,
    silver_schema=w["silver_schema"] or None,
    gold_schema=w["gold_schema"] or None,
    ops_schema=w["ops_schema"] or None,
    dim_schema=w["dim_schema"] or None,
)
steps = {s.strip() for s in w["steps"].split(",") if s.strip()}
print(json.dumps({"run_id": run_id, "steps": sorted(steps), **tables.__dict__}, indent=2))

# COMMAND ----------

result = {"run_id": run_id}
if "silver" in steps:
    result["silver"] = ft.run_silver(spark, tables, run_id=run_id)
    print("silver:", result["silver"])
if "gold" in steps:
    result["gold"] = ft.run_gold(spark, tables, run_id=run_id, delete_missing=w["delete_missing"].lower() == "true")
    print("gold:", result["gold"])

display(spark.table(tables.rejects).groupBy("stage", "reject_reason").count().orderBy("stage", "reject_reason"))

# COMMAND ----------

if w["parity_table"]:
    missing, unexpected = ft.parity_diff(spark.table(tables.gold), spark.table(w["parity_table"]))
    result["parity"] = {
        "table": w["parity_table"],
        "missing": missing,
        "unexpected": unexpected,
        "expected_rows": spark.table(w["parity_table"]).count(),
        "gold_rows": spark.table(tables.gold).count(),
    }
    print("parity:", result["parity"])
    assert missing == 0 and unexpected == 0, (
        f"gold.fact_transaction differs from {w['parity_table']}: {result['parity']}"
    )

dbutils.notebook.exit(json.dumps(result))
