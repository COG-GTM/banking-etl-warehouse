# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest transaction CSV + Excel files -> bronze (ticket 3)
# MAGIC Auto Loader (`cloudFiles`, `trigger(availableNow=True)`) from the landing volume:
# MAGIC - `transactions/csv/`   -> `bronze.file_transaction_csv`   (schema inference + `addNewColumns` evolution, `transaction_date` kept as raw STRING)
# MAGIC - `transactions/excel/` -> `bronze.file_transaction_excel` (native Excel reader, `Sheet1`, 1 header row; schema evolution `none`)
# MAGIC
# MAGIC Each file is ingested exactly once (Auto Loader checkpoint under the ops checkpoints volume), so re-running is a no-op.
# MAGIC Widgets: `catalog`, `schema_prefix`, `sources` (`csv,excel`), `land_from` (optional folder holding
# MAGIC `transaction_csv.csv` / `transaction_excel.xlsx` to copy into the landing volume first).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.bronze.files import ingest_transaction_files, land_transaction_files, parse_sources
from banking_etl.config import settings_from_widgets

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
dbutils.widgets.text("sources", "csv,excel")  # noqa: F821
dbutils.widgets.text("land_from", "")  # noqa: F821
sources = parse_sources(dbutils.widgets.get("sources"))  # noqa: F821
land_from = dbutils.widgets.get("land_from").strip()  # noqa: F821

# COMMAND ----------

if land_from:
    for target, status in land_transaction_files(settings, land_from, sources=sources):
        print(f"{status:<18} {target}")

# COMMAND ----------

# A Databricks notebook command fails if any stream it started failed, even when Python catches the error,
# so the addNewColumns restart is left to the job's task retry (max_retries in the resource yml).
results = ingest_transaction_files(spark, settings, sources, schema_change_retries=0)  # noqa: F821
for r in results:
    print(f"{r['source']:<6} {r['table']}: +{r['rows_added']} rows ({r['rows_before']} -> {r['rows_after']})")

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(results))  # noqa: F821
