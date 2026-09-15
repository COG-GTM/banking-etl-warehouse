# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — file ingestion
# MAGIC Replaces `tFileInputDelimited_1` / `tFileInputExcel_1` of `Load_FactTransaction`.
# MAGIC
# MAGIC * CSV -> `banking.bronze.file_transaction_csv` via Auto Loader (`cloudFiles`).
# MAGIC * XLSX -> `banking.bronze.file_transaction_excel` via `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`
# MAGIC   (cluster library), with a pandas/openpyxl fallback.
# MAGIC
# MAGIC All logic lives in `databricks/bronze/`; this notebook only wires widgets to it.

# COMMAND ----------

import sys

sys.path.append("../..")

from bronze.config import CATALOG, BRONZE_SCHEMA, FILE_SOURCES  # noqa: E402
from bronze.file_ingest import (  # noqa: E402
    ingest_file_source,
    read_csv_autoloader,
    read_excel_pandas,
    read_excel_spark,
)
from bronze.metadata import new_batch_id  # noqa: E402
from bronze.writer import write_bronze_batch, write_bronze_stream  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("landing_path", "/Volumes/banking/bronze/landing", "Landing volume")
dbutils.widgets.text("checkpoint_root", "/Volumes/banking/bronze/_checkpoints", "Checkpoint root")
dbutils.widgets.dropdown("excel_reader", "spark_excel", ["spark_excel", "pandas"], "Excel reader")

landing_path = dbutils.widgets.get("landing_path").rstrip("/")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")
excel_reader = dbutils.widgets.get("excel_reader")
batch_id = new_batch_id()

# COMMAND ----------

csv_source = FILE_SOURCES["transaction_csv"]
csv_checkpoint = f"{checkpoint_root}/{csv_source.target_table}"

csv_df = read_csv_autoloader(
    spark,
    path=f"{landing_path}/transaction_csv",
    source=csv_source,
    schema_location=f"{csv_checkpoint}/_schema",
)
csv_bronze = ingest_file_source(
    spark,
    path=f"{landing_path}/transaction_csv",
    source=csv_source,
    batch_id=batch_id,
    reader=lambda _spark, _path, _source: csv_df,
)

query = write_bronze_stream(
    csv_bronze,
    table=csv_source.full_table_name(CATALOG, BRONZE_SCHEMA),
    checkpoint_location=csv_checkpoint,
)
query.awaitTermination()

# COMMAND ----------

excel_source = FILE_SOURCES["transaction_excel"]
excel_path = f"{landing_path}/transaction_excel/transaction_excel.xlsx"
reader = read_excel_spark if excel_reader == "spark_excel" else read_excel_pandas

excel_bronze = ingest_file_source(
    spark,
    path=excel_path,
    source=excel_source,
    batch_id=batch_id,
    reader=reader,
    source_file=excel_path if excel_reader == "pandas" else None,
)

write_bronze_batch(excel_bronze, table=excel_source.full_table_name(CATALOG, BRONZE_SCHEMA))

# COMMAND ----------

print(f"batch_id={batch_id}")
