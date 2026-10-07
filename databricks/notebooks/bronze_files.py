# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Excel / CSV file sources (Auto Loader)
# MAGIC Replaces `tFileInputExcel` / `tFileInputDelimited` of the Talend `Load_FactTransaction` job.
# MAGIC
# MAGIC | Landing folder | Format | Bronze table |
# MAGIC |---|---|---|
# MAGIC | `<landing>/transaction_excel/` | `cloudFiles.format=excel` | `bronze.file_transaction_excel` |
# MAGIC | `<landing>/transaction_csv/` | CSV, `dd-MM-yyyy HH:mm:ss` | `bronze.file_transaction_csv` |
# MAGIC
# MAGIC Schema/checkpoint state: `<landing>/_autoloader/{schemas,checkpoints}/<table>`. Runs with
# MAGIC `trigger(availableNow=True)`, so each run ingests only files not seen before. Logic lives in
# MAGIC `banking_etl.bronze.files`.

# COMMAND ----------

dbutils.widgets.text("catalog", "migration_demo")
dbutils.widgets.text("schema_prefix", "banking_mig_")
dbutils.widgets.text("landing_root", "", "Landing root (default: bronze landing volume)")
dbutils.widgets.text("sources", "transaction_excel,transaction_csv")
dbutils.widgets.text("src_root", "", "Path to databricks/src (default: ../src)")

# COMMAND ----------

import json
import os
import sys

src_root = dbutils.widgets.get("src_root") or os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from banking_etl.bronze import files  # noqa: E402

loc = files.Locations(
    catalog=dbutils.widgets.get("catalog"),
    schema_prefix=dbutils.widgets.get("schema_prefix"),
    landing_root=dbutils.widgets.get("landing_root") or None,
)
sources = [s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()]
files.ensure_objects(spark, loc)

# COMMAND ----------

counts = files.ingest(spark, loc, sources)
for table, n in counts.items():
    print(f"{table}: {n} rows")

# COMMAND ----------

for name in sources:
    table = loc.table_name(files.SOURCES[name])
    display(spark.sql(f"""
        SELECT _source_file, count(*) AS rows, count(_rescued_data) AS rescued_rows,
               min(_ingested_at) AS first_ingested_at, max(_ingested_at) AS last_ingested_at
        FROM {table} GROUP BY _source_file ORDER BY _source_file
    """))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(counts))
