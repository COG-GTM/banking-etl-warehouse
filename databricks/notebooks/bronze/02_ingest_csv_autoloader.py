# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 4: transaction CSV -> Bronze (Auto Loader)
# MAGIC Incrementally ingests `*.csv` files from the landing volume (`cfg.landing_path("csv_transaction")`)
# MAGIC into `bronze.file_transaction_csv` with `cloudFiles` (explicit schema, `timestampFormat=dd-MM-yyyy HH:mm:ss`,
# MAGIC `_rescued_data`, `trigger(availableNow=True)`). Files already in the checkpoint are skipped, so a rerun
# MAGIC with no new files appends 0 rows. Replaces `tFileInputDelimited_1` in Talend `Load_FactTransaction`.
# MAGIC
# MAGIC Widgets: `land_sample=true` first copies the sample CSV (`sample_source`, default the repo's
# MAGIC `data_sources/transaction_csv.csv`) into the landing folder.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.bronze import csv_autoloader as t4

dbutils.widgets.dropdown("land_sample", "false", ["true", "false"], "Land sample CSV first")  # noqa: F821
dbutils.widgets.text("sample_source", "", "Sample CSV path (blank = repo data_sources/)")  # noqa: F821

landing = cfg.landing_path(t4.LANDING_SOURCE)  # noqa: F821
landed = None
if dbutils.widgets.get("land_sample") == "true":  # noqa: F821
    landed = t4.land_sample_csv(landing, dbutils.widgets.get("sample_source") or t4.SAMPLE_CSV)  # noqa: F821
    print("landed", landed)

# COMMAND ----------

summary = t4.ingest(spark, cfg)  # noqa: F821
summary["landed"] = landed
summary["landing_files"] = sorted(f.name for f in dbutils.fs.ls(landing))  # noqa: F821
print(json.dumps(summary, indent=2, default=str))

# COMMAND ----------

display(spark.table(summary["table"]).orderBy("transaction_id"))  # noqa: F821

# COMMAND ----------

dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
