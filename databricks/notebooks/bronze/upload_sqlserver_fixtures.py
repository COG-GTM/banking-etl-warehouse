# Databricks notebook source
# MAGIC %md
# MAGIC # Upload SQL Server fixtures to the landing volume (ticket 2, dev fixture mode)
# MAGIC Copies `fixtures/sample_db/<table>.csv` (exports of `data_sources/sample.bak`) to
# MAGIC `/Volumes/<catalog>/<schema_prefix>bronze/landing/sample_db/<table>/<table>.csv`.
# MAGIC Widgets: `catalog`, `schema_prefix`, `tables` (default all six).
# MAGIC From a laptop use `scripts/bronze/upload_fixtures.py` instead.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.bronze.fixtures import landing_root, stage_fixtures
from banking_etl.bronze.sqlserver import parse_tables
from banking_etl.config import SQLSERVER_SOURCE_TABLES, settings_from_widgets

settings = settings_from_widgets(dbutils)  # noqa: F821
dbutils.widgets.text("tables", ",".join(SQLSERVER_SOURCE_TABLES))  # noqa: F821
for path in stage_fixtures(landing_root(settings), parse_tables(dbutils.widgets.get("tables"))):  # noqa: F821
    print("staged", path)
