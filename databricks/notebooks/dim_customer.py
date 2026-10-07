# Databricks notebook source
# MAGIC %md
# MAGIC # Load_DimCustomer -> silver.customer / gold.dim_customer
# MAGIC Port of the Talend job `Load_DimCustomer`: customer LEFT JOIN city LEFT JOIN state,
# MAGIC UPPER() on customer_name / address / gender, SCD-1 MERGE into `gold.dim_customer`.
# MAGIC
# MAGIC Widgets:
# MAGIC * `catalog`, `schema_prefix` - Unity Catalog location (`<catalog>.<prefix>{bronze,silver,gold}`).
# MAGIC * `schema_override` - put every layer in one schema (ticket-scoped validation runs).
# MAGIC * `seed_fixture_dir` - optional folder with `sqlserver_{customer,city,state}.csv`; seeds bronze
# MAGIC   (only allowed together with `schema_override`, so shared bronze is never overwritten).
# MAGIC * `parity_csv` - optional legacy DimCustomer baseline; the run fails unless gold matches it exactly.

# COMMAND ----------

import json
import os
import sys

dbutils.widgets.text("catalog", "migration_demo")
dbutils.widgets.text("schema_prefix", "banking_mig_")
dbutils.widgets.text("schema_override", "")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("seed_fixture_dir", "")
dbutils.widgets.text("parity_csv", "")

catalog = dbutils.widgets.get("catalog").strip() or None
schema_prefix = dbutils.widgets.get("schema_prefix").strip()
schema_override = dbutils.widgets.get("schema_override").strip() or None
seed_fixture_dir = dbutils.widgets.get("seed_fixture_dir").strip()
parity_csv = dbutils.widgets.get("parity_csv").strip()

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
src_path = dbutils.widgets.get("src_path").strip() or os.path.normpath(
    os.path.join("/Workspace" + os.path.dirname(notebook_path), "..", "src")
)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from banking_etl.dims import customer as dim_customer  # noqa: E402

tables = dim_customer.Tables.build(catalog=catalog, schema_prefix=schema_prefix, schema_override=schema_override)
print(tables)

# COMMAND ----------

if seed_fixture_dir:
    if not schema_override:
        raise ValueError("seed_fixture_dir requires schema_override (never seed the shared bronze schema)")
    import csv

    if catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_override}")
    for name, target in [
        ("sqlserver_customer", tables.bronze_customer),
        ("sqlserver_city", tables.bronze_city),
        ("sqlserver_state", tables.bronze_state),
    ]:
        with open(os.path.join(seed_fixture_dir, f"{name}.csv"), newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cols = reader.fieldnames
            rows = [tuple(None if r[c] == "" else r[c] for c in cols) for r in reader]
        df = spark.createDataFrame(rows, ", ".join(f"{c} STRING" for c in cols))
        for c in cols:
            if c.endswith("_id"):
                df = df.withColumn(c, df[c].cast("int"))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        print(f"seeded {target}: {len(rows)} rows")

# COMMAND ----------

counts = dim_customer.run(spark, tables)
print(counts)

# COMMAND ----------

result = {"tables": tables.__dict__, "counts": counts}
if parity_csv:
    expected = dim_customer.read_parity_csv(spark, parity_csv)
    diff = dim_customer.assert_parity(spark.table(tables.gold_dim_customer), expected)
    result["parity"] = {k: v for k, v in diff.items() if not k.endswith("_sample")}
    print("PARITY OK", result["parity"])

display(spark.table(tables.gold_dim_customer).orderBy("customer_id"))
dbutils.notebook.exit(json.dumps(result))
