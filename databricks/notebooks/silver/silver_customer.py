# Databricks notebook source
# MAGIC %md
# MAGIC # silver.customer (ticket 7, Talend `Load_DimCustomer` tMap)
# MAGIC `bronze.sqlserver_customer` LEFT JOIN `sqlserver_city` LEFT JOIN `sqlserver_state`, UPPER on
# MAGIC customer_name / address / gender, `age` varchar(3) -> INT. Overwrites `silver.customer`; rows
# MAGIC SQL Server would have refused (NULL/duplicate id, non-integer or out-of-range age, over-long strings)
# MAGIC go to `ops.dim_customer_rejects` (overwritten each run).
# MAGIC Widgets: `catalog`, `schema_prefix`. Logic: `src/banking_etl/dims/customer.py`.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from banking_etl.config import settings_from_widgets
from banking_etl.dims.customer import load_silver_customer

settings = settings_from_widgets(dbutils)  # noqa: F821 - provided by Databricks
result = load_silver_customer(spark, settings)  # noqa: F821
print(result)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))  # noqa: F821
