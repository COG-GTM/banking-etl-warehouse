# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server -> bronze (ticket 2)
# MAGIC Full-snapshot extract of the Talend `tMSSqlInput` sources into `bronze.sqlserver_<table>`
# MAGIC (overwrite, plus `_ingested_at` / `_source`).
# MAGIC
# MAGIC Widgets: `catalog`, `schema_prefix`, `secret_scope`, `source_mode` (`jdbc` | `fixture`),
# MAGIC `tables` (comma-separated, default all six: customer, city, state, account, branch, transaction_db),
# MAGIC `num_partitions` (JDBC partitioning hint on the integer key, default 1),
# MAGIC `stage_fixtures` (fixture mode: copy `fixtures/sample_db/*.csv` into the landing volume first, default false).
# MAGIC Logic: `src/banking_etl/bronze/sqlserver.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.bronze.fixtures import landing_root, stage_fixtures
from banking_etl.bronze.sqlserver import ingest_sqlserver, parse_tables
from banking_etl.config import SQLSERVER_SOURCE_TABLES, settings_from_widgets
from banking_etl.setup.provision import parse_bool

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
dbutils.widgets.text("tables", ",".join(SQLSERVER_SOURCE_TABLES))  # noqa: F821
dbutils.widgets.text("num_partitions", "1")  # noqa: F821
dbutils.widgets.text("stage_fixtures", "false")  # noqa: F821
tables = parse_tables(dbutils.widgets.get("tables"))  # noqa: F821
num_partitions = int(dbutils.widgets.get("num_partitions") or 1)  # noqa: F821
print(settings, tables, f"num_partitions={num_partitions}")

# COMMAND ----------

if settings.source_mode == "fixture" and parse_bool(dbutils.widgets.get("stage_fixtures")):  # noqa: F821
    for path in stage_fixtures(landing_root(settings), tables):
        print("staged", path)

# COMMAND ----------

results = ingest_sqlserver(spark, settings, tables, dbutils=dbutils, num_partitions=num_partitions)  # noqa: F821
for r in results:
    print(f"{r['target']:<60} {r['rows']:>6} rows  <- {r['source']}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({r["table"]: r["rows"] for r in results}))  # noqa: F821
