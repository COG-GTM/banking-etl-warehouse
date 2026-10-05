# Databricks notebook source
# MAGIC %md
# MAGIC # Daily transactions (ticket 9) - `EXEC sp_DailyTransaction @start_date, @end_date`
# MAGIC Ad-hoc front end for `gold.fn_daily_transaction`: count and total amount per day between two dates
# MAGIC (inclusive). Widgets: `catalog`, `schema_prefix`, `start_date`, `end_date` (`YYYY-MM-DD`; empty = NULL,
# MAGIC which returns no rows like the procedure). Run `create_analytics_functions` first.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.analytics.functions import daily_transaction
from banking_etl.config import settings_from_widgets

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
dbutils.widgets.text("start_date", "2024-01-18", "Start date (inclusive)")  # noqa: F821
dbutils.widgets.text("end_date", "2024-01-20", "End date (inclusive)")  # noqa: F821
start_date = dbutils.widgets.get("start_date").strip() or None  # noqa: F821
end_date = dbutils.widgets.get("end_date").strip() or None  # noqa: F821

result = daily_transaction(spark, settings, start_date, end_date)  # noqa: F821
display(result)  # noqa: F821

# COMMAND ----------

rows = [r.asDict() for r in result.collect()]
dbutils.notebook.exit(json.dumps({"rows": rows}, default=str))  # noqa: F821
