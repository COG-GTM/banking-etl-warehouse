# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 5: Excel transactions → Bronze `file_transaction_excel`
# MAGIC Ports the `tFileInputExcel_1` branch of Talend `Load_FactTransaction` (Sheet1, 1 header row,
# MAGIC `DIE_ON_ERROR=false`). Reads every `.xlsx` under `cfg.landing_path("excel_transaction")`
# MAGIC (`/Volumes/<catalog>/<prefix>bronze/landing/excel/transaction/`) and writes the
# MAGIC `BRONZE_FILE_TRANSACTION_EXCEL` contract: `transaction_date TIMESTAMP`, `amount DECIMAL(19,4)`,
# MAGIC unparseable cells → `_rescued_data`, plus `_source_file` / `_ingested_at`.
# MAGIC
# MAGIC * `reader=openpyxl` (default): portable openpyxl path. Needs `openpyxl` in the environment, installed
# MAGIC   below (or add it to the job environment's `dependencies`).
# MAGIC * `reader=native`: Databricks built-in Excel source (`spark.read.format("excel")`, `dataAddress=Sheet1`),
# MAGIC   read as strings and typed by the same code. On classic clusters `com.crealytics.spark.excel` is the
# MAGIC   library alternative; it is not installable on serverless.
# MAGIC
# MAGIC Idempotent: rows from a file replace earlier rows from the same `_source_file` in one Delta commit.

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json

from banking_dwh.bronze.excel import DEFAULT_SHEET, ingest_excel

dbutils.widgets.dropdown("reader", "openpyxl", ["openpyxl", "native"], "Excel reader")  # noqa: F821
dbutils.widgets.text("sheet", DEFAULT_SHEET, "Sheet name")  # noqa: F821
reader = dbutils.widgets.get("reader")  # noqa: F821
sheet = dbutils.widgets.get("sheet") or DEFAULT_SHEET  # noqa: F821
if reader not in ("openpyxl", "native"):
    raise ValueError(f"Invalid reader {reader!r}")
if len(sheet) > 31 or any(ch in sheet for ch in "[]:*?/\\'"):
    raise ValueError(f"Invalid Excel sheet name {sheet!r}")

summary = ingest_excel(spark, cfg, reader=reader, sheet=sheet)  # noqa: F821
print(json.dumps(summary, indent=2))

# COMMAND ----------

table = summary["table"]
schema = [{"name": f.name, "type": f.dataType.simpleString()} for f in spark.table(table).schema]  # noqa: F821
from pyspark.sql import functions as F  # noqa: E402

loaded = spark.table(table).where(F.col("_source_file").isin(summary["files"]))  # noqa: F821
sample = [
    {k: (str(v) if v is not None else None) for k, v in r.asDict().items()}
    for r in loaded.orderBy("transaction_id").limit(20).collect()
]
display(spark.table(table).orderBy("transaction_id"))  # noqa: F821
dbutils.notebook.exit(json.dumps({**summary, "schema": schema, "rows": sample}))  # noqa: F821
