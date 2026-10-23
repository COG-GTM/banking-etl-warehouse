# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 9: Gold analytics objects (replace the stored procedures)
# MAGIC Creates `GOLD_OBJECTS` from `banking_dwh.contracts.tables` in the gold schema of the selected target:
# MAGIC * `v_daily_transaction` view (unfiltered `sp_DailyTransaction`)
# MAGIC * `fn_daily_transaction(start_date DATE, end_date DATE)` SQL table function (`sp_DailyTransaction`)
# MAGIC * `fn_balance_per_customer(customer_name STRING)` SQL table function (`sp_BalancePerCustomer`)
# MAGIC
# MAGIC DDL comes from `banking_dwh.gold.analytics` (same code that renders `ddl/04_gold_objects.sql`).
# MAGIC Reads silver `fact_transaction`, `dim_account`, `dim_customer` (Ticket 2 schema; created by `00_setup/02_create_star_schema`).

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.contracts.tables import GOLD_OBJECTS
from banking_dwh.gold.analytics import gold_object_statements

stmts = gold_object_statements(cfg)  # noqa: F821
for s in stmts:
    print(s, end=";\n\n")
    spark.sql(s)  # noqa: F821

# information_schema, not SHOW VIEWS/FUNCTIONS IN <catalog>.<schema> (cross-catalog SHOW is rejected on serverless).
gold, info = cfg.schema("gold"), f"{cfg.catalog}.information_schema"  # noqa: F821
schema_name = cfg.schema_name("gold")  # noqa: F821
views = {r[0] for r in spark.sql(f"SELECT table_name FROM {info}.views WHERE table_schema = :s", args={"s": schema_name}).collect()}  # noqa: F821,E501
functions = {r[0] for r in spark.sql(f"SELECT routine_name FROM {info}.routines WHERE routine_schema = :s", args={"s": schema_name}).collect()}  # noqa: F821,E501
out = {
    "schema": gold,
    "objects": [
        {"name": o.name, "kind": o.kind, "exists": o.name in (views if o.kind == "view" else functions)}
        for o in GOLD_OBJECTS
    ],
}
print(out)
if not all(o["exists"] for o in out["objects"]):
    raise RuntimeError(f"Gold objects missing after create: {out}")
dbutils.notebook.exit(json.dumps(out))  # noqa: F821
