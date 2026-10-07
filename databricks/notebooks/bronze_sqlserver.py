# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: SQL Server `sample` DB -> `bronze.sqlserver_<table>`
# MAGIC
# MAGIC Config-driven ingestion (`banking_etl/bronze/sources.yml`) of every source table of the
# MAGIC legacy SQL Server `sample` database into Delta, with `_ingested_at`, `_source`, `_batch_id`.
# MAGIC
# MAGIC * `source=jdbc`   - live JDBC read. Credentials come from the secret scope in `sources.yml`
# MAGIC   (`banking-etl-sqlserver`: `jdbc-host`, `jdbc-port`, `jdbc-database`, `jdbc-user`, `jdbc-password`),
# MAGIC   falling back to `SQLSERVER_*` env vars.
# MAGIC * `source=staged` - replay a parquet extract produced by
# MAGIC   `python -m banking_etl.bronze.sqlserver extract` and uploaded to the landing volume
# MAGIC   (stand-in for a JDBC endpoint the workspace cannot reach). Same bronze writer.
# MAGIC
# MAGIC `mode` blank = per-table mode from `sources.yml` (dims `full`, `transaction_db` `incremental`
# MAGIC on `transaction_id`). `mode=incremental` applies only to tables with a `watermark_column`
# MAGIC (`account`, `transaction_db`), so set `tables` accordingly.
# MAGIC
# MAGIC Staged runs require `_manifest.json` and check every selected table's parquet row count against it
# MAGIC before anything is written.

# COMMAND ----------

dbutils.widgets.text("catalog", "migration_demo")
dbutils.widgets.text("schema_prefix", "banking_mig_")
dbutils.widgets.dropdown("source", "jdbc", ["jdbc", "staged"])
dbutils.widgets.text("staged_path", "/Volumes/migration_demo/banking_mig_bronze/landing/sqlserver/latest")
dbutils.widgets.dropdown("mode", "", ["", "full", "incremental"])
dbutils.widgets.text("tables", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("src_path", "")

# COMMAND ----------

import json
import os
import sys

src_path = dbutils.widgets.get("src_path").strip() or os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from banking_etl.bronze import sqlserver as bronze_sqlserver

catalog = dbutils.widgets.get("catalog").strip() or None
schema_prefix = dbutils.widgets.get("schema_prefix").strip()
source = dbutils.widgets.get("source")
mode = dbutils.widgets.get("mode") or None
tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()] or None
batch_id = dbutils.widgets.get("batch_id").strip() or bronze_sqlserver.new_batch_id()

config = bronze_sqlserver.load_config()
print(f"batch_id={batch_id} source={source} target={bronze_sqlserver.bronze_schema(catalog, schema_prefix)}")

# COMMAND ----------

manifest = {}
if source == "jdbc":
    conn = bronze_sqlserver.resolve_connection(config, dbutils=dbutils)
    print(f"source coverage: {bronze_sqlserver.assert_source_coverage(spark, conn, config)}")
    reader = bronze_sqlserver.jdbc_reader(spark, conn, config)
    label = bronze_sqlserver.jdbc_source_label(conn, config)
else:
    staged_path = dbutils.widgets.get("staged_path").strip()
    manifest = bronze_sqlserver.load_manifest(staged_path)
    print(f"staged extract: {staged_path} extracted_at={manifest.get('extracted_at')}")
    problems = bronze_sqlserver.validate_staged_extract(spark, staged_path, config, manifest, tables)
    if problems:
        raise AssertionError("staged extract incomplete, nothing written: " + "; ".join(problems))
    reader = bronze_sqlserver.staged_reader(spark, staged_path)
    label = bronze_sqlserver.staged_source_label(staged_path, manifest, config)

# COMMAND ----------

results = bronze_sqlserver.ingest_all(
    spark, config, reader, label,
    catalog=catalog, schema_prefix=schema_prefix, tables=tables, mode=mode, batch_id=batch_id,
)
for r in results:
    print(json.dumps(r.as_dict()))

problems = bronze_sqlserver.verify_against_manifest(results, manifest)
if problems:
    raise AssertionError("row-count check against extract manifest failed: " + "; ".join(problems))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"batch_id": batch_id, "results": [r.as_dict() for r in results]}))
