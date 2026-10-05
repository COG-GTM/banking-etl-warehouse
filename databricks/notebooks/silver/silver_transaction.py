# Databricks notebook source
# MAGIC %md
# MAGIC # bronze transactions -> silver.transaction (ticket 8, Talend `Load_FactTransaction`)
# MAGIC Conforms `bronze.sqlserver_transaction_db`, `bronze.file_transaction_excel` and `bronze.file_transaction_csv`
# MAGIC (CSV dates `dd-MM-yyyy HH:mm:ss`, amount -> DECIMAL(19,4)), unions them in tUnite order
# MAGIC (SQL Server, Excel, CSV) and keeps the first row per `transaction_id` (tUniqRow). Full overwrite.
# MAGIC Unparseable input rows (null id, bad date, `_rescued_data`) replace the input-level rows of
# MAGIC `ops.fact_transaction_rejects`.
# MAGIC
# MAGIC Widgets: `catalog`, `schema_prefix`. Logic: `src/banking_etl/facts/transaction.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.facts.transaction import load_silver_transaction

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
print(settings)

# COMMAND ----------

result = load_silver_transaction(spark, settings)  # noqa: F821
print(json.dumps(result, indent=2))

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))  # noqa: F821
