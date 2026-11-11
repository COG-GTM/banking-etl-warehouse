# Databricks notebook source
# MAGIC %md
# MAGIC # sp_DailyTransaction
# MAGIC Databricks port of the SQL Server stored procedure `sp_DailyTransaction`
# MAGIC (`sql_scripts/02_create_procedures.sql`): daily transaction count and total amount
# MAGIC from `<catalog>.<gold_schema>.fact_transaction` between `start_date` and `end_date`
# MAGIC (inclusive). Creates/refreshes the Unity Catalog table function
# MAGIC `<catalog>.<gold_schema>.sp_daily_transaction` and the SQL stored procedure
# MAGIC `<catalog>.<gold_schema>.sp_daily_transaction_proc`, runs them and the PySpark
# MAGIC implementation, and fails if any of them disagree.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("catalog", "banking_dwh")  # noqa: F821
dbutils.widgets.text("gold_schema", "gold")  # noqa: F821
dbutils.widgets.text("start_date", "")  # noqa: F821
dbutils.widgets.text("end_date", "")  # noqa: F821
dbutils.widgets.dropdown("create_function", "true", ["true", "false"])  # noqa: F821
dbutils.widgets.dropdown("create_procedure", "true", ["true", "false"])  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
gold_schema = dbutils.widgets.get("gold_schema")  # noqa: F821
start_date = dbutils.widgets.get("start_date").strip() or None  # noqa: F821
end_date = dbutils.widgets.get("end_date").strip() or None  # noqa: F821
deploy_function = dbutils.widgets.get("create_function") == "true"  # noqa: F821
deploy_procedure = dbutils.widgets.get("create_procedure") == "true"  # noqa: F821

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from banking_etl.sp_daily_transaction import (  # noqa: E402
    call_procedure,
    call_table_function,
    create_procedure,
    create_table_function,
    run_daily_transaction,
    to_date,
)

start, end = to_date(start_date), to_date(end_date)

# COMMAND ----------

if deploy_function:
    print(create_table_function(spark, catalog=catalog, schema=gold_schema))  # noqa: F821
if deploy_procedure:
    print(create_procedure(spark, catalog=catalog, schema=gold_schema))  # noqa: F821

pyspark_result = run_daily_transaction(spark, start, end, catalog=catalog, schema=gold_schema)  # noqa: F821
function_result = call_table_function(spark, start, end, catalog=catalog, schema=gold_schema)  # noqa: F821

procedure_result = call_procedure(spark, start, end, catalog=catalog, schema=gold_schema)  # noqa: F821

pyspark_rows = [r.asDict() for r in pyspark_result.collect()]
for name, df in (("UC table function", function_result), ("SQL procedure", procedure_result)):
    rows = [r.asDict() for r in df.collect()]
    if rows != pyspark_rows:
        raise AssertionError(f"PySpark and {name} results differ:\n{pyspark_rows}\n{rows}")

display(function_result)  # noqa: F821

# COMMAND ----------

dbutils.notebook.exit(  # noqa: F821
    json.dumps(
        {
            "start_date": str(start) if start else None,
            "end_date": str(end) if end else None,
            "rows": [
                {
                    "date": str(r["date"]),
                    "total_transactions": r["total_transactions"],
                    "total_amount": None if r["total_amount"] is None else str(r["total_amount"]),
                }
                for r in pyspark_rows
            ],
        }
    )
)
