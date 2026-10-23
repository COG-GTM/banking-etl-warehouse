# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 9: `EXEC sp_DailyTransaction @start_date, @end_date` replacement
# MAGIC Parameterized notebook: set the `start_date` / `end_date` widgets (ISO dates, inclusive) or pass them
# MAGIC as job `base_parameters`. Calls `<gold>.fn_daily_transaction(:start_date, :end_date)` with named
# MAGIC parameter markers (no string interpolation of user input) and returns the rows as JSON.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json
from datetime import date

from banking_dwh.gold.analytics import call_daily_transaction_sql

dbutils.widgets.text("start_date", "2024-01-01", "Start date (inclusive, YYYY-MM-DD)")  # noqa: F821
dbutils.widgets.text("end_date", "2024-01-31", "End date (inclusive, YYYY-MM-DD)")  # noqa: F821
args = {k: date.fromisoformat(dbutils.widgets.get(k).strip()) for k in ("start_date", "end_date")}  # noqa: F821

sql = call_daily_transaction_sql(cfg)  # noqa: F821
print(sql, args)
df = spark.sql(sql, args=args)  # noqa: F821
display(df)  # noqa: F821
rows = [{k: str(v) if v is not None else None for k, v in r.asDict().items()} for r in df.collect()]
out = {"function": sql.split(" FROM ")[1].split("(")[0], "params": {k: str(v) for k, v in args.items()},
       "row_count": len(rows), "rows": rows}
dbutils.notebook.exit(json.dumps(out))  # noqa: F821
