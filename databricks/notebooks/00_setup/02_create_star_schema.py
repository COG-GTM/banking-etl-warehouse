# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 2: Delta star schema (Silver) + ops tables
# MAGIC Creates `dim_branch`, `dim_account`, `dim_customer`, `fact_transaction` and `ops.dq_results`
# MAGIC from `banking_dwh.contracts.tables`, with informational PRIMARY KEY / FOREIGN KEY constraints
# MAGIC (`FK_FactTransaction_DimAccount`, `FK_FactTransaction_DimBranch`). Set `create_bronze=true`
# MAGIC to also pre-create the Bronze tables from their contracts.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json

from banking_dwh.contracts.ddl import bronze_statements, star_schema_statements

dbutils.widgets.dropdown("create_bronze", "false", ["true", "false"], "Pre-create Bronze tables")  # noqa: F821

stmts = star_schema_statements(cfg)
if dbutils.widgets.get("create_bronze") == "true":  # noqa: F821
    stmts = bronze_statements(cfg) + stmts
for s in stmts:
    print(s, end=";\n\n")
    spark.sql(s)  # noqa: F821

constraints = spark.sql(f"""  
  SELECT tc.table_name, tc.constraint_name, tc.constraint_type
  FROM {cfg.catalog}.information_schema.table_constraints tc
  WHERE tc.table_schema = '{cfg.schema_name("silver")}'
  ORDER BY tc.table_name, tc.constraint_type
""").collect()  # noqa: F821
out = {"tables": len(stmts), "constraints": [r.asDict() for r in constraints]}
print(out)
dbutils.notebook.exit(json.dumps(out))  # noqa: F821
