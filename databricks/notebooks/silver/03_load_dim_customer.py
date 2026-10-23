# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 7: Silver `dim_customer` load (Talend `Load_DimCustomer`)
# MAGIC Reads Bronze `sqlserver_customer` (main) LEFT JOIN `sqlserver_city` LEFT JOIN `sqlserver_state`,
# MAGIC upper-cases `customer_name`, `address` and `gender` only, casts `age` to INT, and MERGEs into
# MAGIC `silver.dim_customer` on `customer_id`. Then runs the Ticket 2 PK checks for `dim_customer`
# MAGIC (`pk_not_null.dim_customer`, `pk_unique.dim_customer`) into `ops.dq_results`.
# MAGIC
# MAGIC `seed_bronze_fixture=true` first overwrites the Bronze customer/city/state tables with the
# MAGIC `sample.bak` extract in `fixtures/t7_sample_db/`. Only for a ticket scratch prefix (e.g.
# MAGIC `bdwh_t7_`): it refuses the shared dev prefix and the prod target. A seeded run also compares the
# MAGIC result with the legacy `DimCustomer` rows in `expected_dim_customer.csv`.

# COMMAND ----------

# MAGIC %run ../00_setup/00_config

# COMMAND ----------

import json
import os
import uuid

from banking_dwh.silver import dim_customer

dbutils.widgets.dropdown("seed_bronze_fixture", "false", ["true", "false"], "Seed Bronze from fixture")  # noqa: F821
dbutils.widgets.dropdown("run_dq", "true", ["true", "false"], "Run PK DQ checks")  # noqa: F821
dbutils.widgets.dropdown("fail_on_error", "true", ["true", "false"], "Fail on DQ error")  # noqa: F821
dbutils.widgets.text("run_id", "", "Run id (defaults to a uuid)")  # noqa: F821
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())  # noqa: F821

out = {"run_id": run_id, "target_table": cfg.table("silver", "dim_customer")}  # noqa: F821
if dbutils.widgets.get("seed_bronze_fixture") == "true":  # noqa: F821
    fixture_dir = os.path.join(BUNDLE_ROOT, "fixtures", "t7_sample_db")  # noqa: F821
    out["seeded_bronze"] = dim_customer.seed_bronze_from_fixture(spark, cfg, fixture_dir)  # noqa: F821

out["load"] = dim_customer.load(spark, cfg)  # noqa: F821
print(json.dumps(out["load"], indent=2))

if dbutils.widgets.get("run_dq") == "true":  # noqa: F821
    out["dq"] = dim_customer.run_dq(
        spark, cfg, run_id, fail_on_error=dbutils.widgets.get("fail_on_error") == "true"  # noqa: F821
    )
    for r in out["dq"]:
        print("PASS" if r["passed"] else "FAIL", r["check_name"], r["failed_rows"])

if "seeded_bronze" in out:
    out["parity_vs_legacy_dimcustomer"] = dim_customer.parity_with_fixture(spark, cfg, fixture_dir)  # noqa: F821
    print(out["parity_vs_legacy_dimcustomer"])

out["sample"] = dim_customer.sample_rows(spark, cfg, 5)  # noqa: F821
dbutils.notebook.exit(json.dumps(out, default=str))  # noqa: F821
