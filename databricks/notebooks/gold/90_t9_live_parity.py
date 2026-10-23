# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 9: live parity check against the original stored procedures
# MAGIC **Scratch namespaces only.** Seeds silver of a Ticket 9 scratch prefix (e.g. `schema_prefix=bdwh_t9_`) with the
# MAGIC legacy DWH fixtures in `fixtures/t9_legacy_parity/<dataset>/`, (re)creates the gold objects there, calls both
# MAGIC table functions for every case in `expected.json` (results of the original procs on SQL Server 2022) and
# MAGIC compares. `seed_silver` refuses prod and any prefix not ending in `t9_`; gold is never seeded.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.gold import parity
from banking_dwh.gold.analytics import (
    V_DAILY_TRANSACTION,
    call_balance_per_customer_sql,
    call_daily_transaction_sql,
    gold_object_statements,
)

dbutils.widgets.dropdown("dataset", "legacy", list(parity.DATASETS), "Fixture dataset")  # noqa: F821
dataset = dbutils.widgets.get("dataset")  # noqa: F821

parity.assert_t9_scratch(cfg)  # noqa: F821
seeded = parity.seed_silver(spark, cfg, dataset)  # noqa: F821
for s in gold_object_statements(cfg):  # noqa: F821
    spark.sql(s)  # noqa: F821

results = []
for case in parity.expected_cases(dataset):
    sql = (call_daily_transaction_sql(cfg) if case["proc"] == "sp_DailyTransaction"  # noqa: F821
           else call_balance_per_customer_sql(cfg))  # noqa: F821
    rows = spark.sql(sql, args=parity.case_args(case)).collect()  # noqa: F821
    ok, msg = parity.compare(parity.actual_rows(rows, case), case)
    results.append({"proc": case["proc"], "params": case["params"], "expected_rows": len(case["rows"]),
                    "actual_rows": len(rows), "match": ok, **({} if ok else {"diff": msg})})

view_rows = spark.table(cfg.table("gold", V_DAILY_TRANSACTION)).count()  # noqa: F821
samples = {
    "fn_daily_transaction('2024-01-01','2024-01-31')": [
        {k: str(v) for k, v in r.asDict().items()}
        for r in spark.sql(f"SELECT * FROM {cfg.table('gold', 'fn_daily_transaction')}(DATE'2024-01-01', DATE'2024-01-31')").collect()  # noqa: F821,E501
    ],
    "fn_balance_per_customer('SHELLY')": [
        {k: str(v) for k, v in r.asDict().items()}
        for r in spark.sql(f"SELECT * FROM {cfg.table('gold', 'fn_balance_per_customer')}('SHELLY')").collect()  # noqa: F821
    ],
}
passed = sum(r["match"] for r in results)
out = {"dataset": dataset, "silver_schema": cfg.schema("silver"), "gold_schema": cfg.schema("gold"),  # noqa: F821
       "seeded_silver_rows": seeded, "v_daily_transaction_rows": view_rows,
       "parity": f"{passed}/{len(results)}", "samples": samples,
       "failures": [r for r in results if not r["match"]], "cases": results}
print(json.dumps(out, indent=1, default=str))
if passed != len(results):
    raise AssertionError(f"Gold parity {passed}/{len(results)}: {out['failures']}")
dbutils.notebook.exit(json.dumps(out, default=str))  # noqa: F821
