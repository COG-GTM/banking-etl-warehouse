# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 9: analytics table functions (sp_DailyTransaction, sp_BalancePerCustomer)
# MAGIC Replaces the DWH stored procedures (`sql_scripts/02_create_procedures.sql`) with Unity Catalog SQL
# MAGIC table functions, created from `sql/analytics/*.sql` (idempotent `CREATE OR REPLACE`):
# MAGIC
# MAGIC | T-SQL | Databricks SQL |
# MAGIC |---|---|
# MAGIC | `EXEC sp_DailyTransaction '2024-01-18', '2024-01-20'` | `SELECT * FROM migration_demo.banking_mig_gold.fn_daily_transaction(DATE'2024-01-18', DATE'2024-01-20')` |
# MAGIC | `EXEC sp_BalancePerCustomer 'shelly'` | `SELECT * FROM migration_demo.banking_mig_gold.fn_balance_per_customer('shelly')` |
# MAGIC
# MAGIC Widgets:
# MAGIC * `catalog` (`migration_demo`), `schema_prefix` (`banking_mig_`): shared gold schema `<catalog>.<prefix>gold`.
# MAGIC * `mode`:
# MAGIC   * `create`: create the functions in shared gold (needs `gold.fact_transaction`, `gold.dim_account`, `gold.dim_customer`).
# MAGIC   * `parity`: seed the SQL Server parity datasets into the ticket-scoped schema `<catalog>.<prefix>t9`
# MAGIC     (`dwh` tables unprefixed, `dwh_edge` tables/functions prefixed `edge_`), create the functions there and assert
# MAGIC     every SQL function and PySpark result equals the original procedures' SQL Server 2022 output. Also creates the
# MAGIC     functions in shared gold if its tables exist.
# MAGIC * `ticket_layer` (`t9`): layer suffix of the ticket-scoped schema.

# COMMAND ----------

import json
import os
import sys

DATABRICKS_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(DATABRICKS_ROOT, "src"))

from banking_etl.analytics.functions import FUNCTIONS, apply_analytics_functions, function_name  # noqa: E402
from banking_etl.analytics.parity import SCENARIOS, load_expected, run_cases, seed_gold  # noqa: E402
from banking_etl.analytics.target import GOLD_TABLES, AnalyticsTarget, target_from_widgets  # noqa: E402

gold = target_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
dbutils.widgets.dropdown("mode", "create", ["create", "parity"])  # noqa: F821
dbutils.widgets.text("ticket_layer", "t9")  # noqa: F821
mode = dbutils.widgets.get("mode")  # noqa: F821
ticket_layer = dbutils.widgets.get("ticket_layer")  # noqa: F821
expected_dir = os.path.join(DATABRICKS_ROOT, "tests", "analytics", "expected")
report = {"mode": mode, "gold_schema": gold.schema}

# COMMAND ----------


def gold_tables_exist(target: AnalyticsTarget) -> bool:
    return all(spark.catalog.tableExists(target.table(t)) for t in GOLD_TABLES)  # noqa: F821


if gold_tables_exist(gold):
    apply_analytics_functions(spark, gold)  # noqa: F821
    report["gold_functions"] = [function_name(gold, fn) for fn in FUNCTIONS]
    report["gold_row_counts"] = {t: spark.table(gold.table(t)).count() for t in GOLD_TABLES}  # noqa: F821
    report["gold_smoke"] = {
        "fn_daily_transaction(2024-01-18, 2024-01-20)": [
            r.asDict() for r in spark.sql(  # noqa: F821
                f"SELECT * FROM {function_name(gold, 'fn_daily_transaction')}(DATE'2024-01-18', DATE'2024-01-20')"
            ).collect()
        ],
        "fn_balance_per_customer('shelly')": spark.sql(  # noqa: F821
            f"SELECT * FROM {function_name(gold, 'fn_balance_per_customer')}('shelly')"
        ).count(),
    }
elif mode == "create":
    raise RuntimeError(f"{gold.schema} is missing one of {GOLD_TABLES}; run the gold loads first or use mode=parity")
else:
    report["gold_functions"] = f"skipped: {gold.schema} does not have {GOLD_TABLES} yet"
print(json.dumps(report, indent=1, default=str))

# COMMAND ----------

failures = []
if mode == "parity" and ticket_layer == "gold":
    raise ValueError("ticket_layer must not be 'gold': parity mode overwrites the seeded tables")
if mode == "parity":
    report["parity"] = {}
    for scenario in SCENARIOS:
        target = AnalyticsTarget(
            catalog=gold.catalog,
            schema_prefix=gold.schema_prefix,
            layer=ticket_layer,
            name_prefix="" if scenario == "dwh" else "edge_",
        )
        data = load_expected(scenario, expected_dir)
        seeded = seed_gold(spark, target, data["tables"])  # noqa: F821
        results = run_cases(spark, target, data["cases"])  # noqa: F821
        for r in results:
            print(f"{scenario}: {r.summary()}")
            if not r.ok:
                failures.append({"scenario": scenario, "procedure": r.procedure, "params": r.params,
                                 "expected": r.expected, "sql": r.sql_rows, "dataframe": r.df_rows})
        report["parity"][scenario] = {
            "source": data["source"],
            "seeded": seeded,
            "functions": [function_name(target, fn) for fn in FUNCTIONS],
            "cases": len(results),
            "passed": sum(r.ok for r in results),
        }
    report["mismatches"] = failures
print(json.dumps(report, indent=1, default=str))
if failures:
    raise AssertionError(f"{len(failures)} parity mismatches: {json.dumps(failures, default=str)[:4000]}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(report, default=str))  # noqa: F821
