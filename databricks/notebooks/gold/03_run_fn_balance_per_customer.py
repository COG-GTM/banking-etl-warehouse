# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 9: `EXEC sp_BalancePerCustomer @customer_name` replacement
# MAGIC Parameterized notebook: set the `customer_name` widget (substring, case-insensitive like the legacy
# MAGIC `SQL_Latin1_General_CP1_CI_AS` collation; `%` / `_` are wildcards as in T-SQL; empty = all customers).
# MAGIC Calls `<gold>.fn_balance_per_customer(:customer_name)` with a named parameter marker and returns JSON.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.gold.analytics import call_balance_per_customer_sql

dbutils.widgets.text("customer_name", "shelly", "Customer name contains")  # noqa: F821
args = {"customer_name": dbutils.widgets.get("customer_name")}  # noqa: F821

sql = call_balance_per_customer_sql(cfg)  # noqa: F821
print(sql, args)
df = spark.sql(sql, args=args)  # noqa: F821
display(df)  # noqa: F821
rows = [{k: str(v) if v is not None else None for k, v in r.asDict().items()} for r in df.collect()]
out = {"function": sql.split(" FROM ")[1].split("(")[0], "params": args, "row_count": len(rows), "rows": rows}
dbutils.notebook.exit(json.dumps(out))  # noqa: F821
