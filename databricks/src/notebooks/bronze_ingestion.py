# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC Lands the sample.bak tables and the transaction CSV / Excel drops in
# MAGIC `<catalog>.<bronze_schema>.*` (append-only, with ingestion metadata).
# MAGIC See `databricks/docs/bronze_ingestion.md`.

# COMMAND ----------

# MAGIC %pip install --quiet openpyxl==3.1.5

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from banking_etl.ingestion.config import BronzeConfig
from banking_etl.ingestion.run import run_bronze_ingestion
from banking_etl.ingestion.sqlserver import SqlServerConnection, federation_ddl

WIDGETS = {
    "catalog": "banking_dwh",
    "bronze_schema": "bronze",
    "sources": "all",
    "sample_db_mode": "files",
    "sample_db_tables": "all",
    "landing_root": "",
    "checkpoint_root": "",
    "jdbc_secret_scope": "banking_sample_db",
    "foreign_catalog": "sample_sqlserver",
    "create_foreign_catalog": "false",
    "use_auto_loader": "true",
    "run_id": "",
}
for name, default in WIDGETS.items():
    dbutils.widgets.text(name, default)  # noqa: F821
arg = {name: dbutils.widgets.get(name).strip() for name in WIDGETS}  # noqa: F821

# COMMAND ----------

config = BronzeConfig(
    catalog=arg["catalog"],
    bronze_schema=arg["bronze_schema"],
    landing_root=arg["landing_root"] or None,
    checkpoint_root=arg["checkpoint_root"] or None,
)


def _secret(key, default=None):
    try:
        return dbutils.secrets.get(arg["jdbc_secret_scope"], key)  # noqa: F821
    except Exception:
        if default is None:
            raise
        return default


conn = None
if arg["sample_db_mode"] == "jdbc":
    conn = SqlServerConnection(
        host=_secret("host"),
        port=int(_secret("port", "1433")),
        database=_secret("database", "sample"),
        user=_secret("user"),
        password=_secret("password"),
        trust_server_certificate=_secret("trust_server_certificate", "false").lower() == "true",
    )
elif arg["sample_db_mode"] == "federation" and arg["create_foreign_catalog"].lower() == "true":
    for statement in federation_ddl(
        connection_name=f"{arg['foreign_catalog']}_conn",
        foreign_catalog=arg["foreign_catalog"],
        host=_secret("host"),
        port=int(_secret("port", "1433")),
        database=_secret("database", "sample"),
        secret_scope=arg["jdbc_secret_scope"],
    ):
        spark.sql(statement)  # noqa: F821

summary = run_bronze_ingestion(
    spark,  # noqa: F821
    config,
    sources=arg["sources"],
    sample_db_mode=arg["sample_db_mode"],
    sample_db_tables=arg["sample_db_tables"],
    conn=conn,
    foreign_catalog=arg["foreign_catalog"],
    use_auto_loader=arg["use_auto_loader"].lower() == "true",
    run_id=arg["run_id"] or None,
)
print(summary)
