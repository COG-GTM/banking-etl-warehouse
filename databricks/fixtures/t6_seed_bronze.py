# Databricks notebook source
# MAGIC %md
# MAGIC # Ticket 6 live-validation helper: seed scratch Bronze branch/account from fixtures
# MAGIC Writes the real `sample.bak` rows (`t6_sqlserver_branch.csv`, `t6_sqlserver_account.csv`, exported from a
# MAGIC SQL Server 2022 restore) into `<schema_prefix>bronze.sqlserver_branch|account`, shaped like the T3 Bronze
# MAGIC contracts. Only for a ticket-6 scratch prefix on the dev target (refuses anything else, never touches
# MAGIC silver/gold).
# MAGIC * `mode=reset`: overwrite both tables with the fixture rows as one new batch.
# MAGIC * `mode=append_change`: append a new batch where branch 5 is renamed and account 4 changes
# MAGIC   balance/status (exercises the SCD-1 update path).

# COMMAND ----------

import csv
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
_nb = _nb if _nb.startswith("/Workspace") else "/Workspace" + _nb
FIXTURE_DIR = _nb.rsplit("/", 1)[0]
ROOT = FIXTURE_DIR.rsplit("/fixtures", 1)[0]
if os.path.join(ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "src"))

from banking_dwh.common.config import EnvConfig  # noqa: E402
from banking_dwh.contracts.ddl import create_table_sql  # noqa: E402
from banking_dwh.contracts.tables import BRONZE_SQLSERVER_ACCOUNT, BRONZE_SQLSERVER_BRANCH  # noqa: E402

dbutils.widgets.dropdown("target", "dev", ["dev"], "Target (dev only)")  # noqa: F821
dbutils.widgets.text("catalog", "", "Catalog override")  # noqa: F821
dbutils.widgets.text("schema_prefix", "bdwh_t6_", "Scratch schema prefix (must end in t6_)")  # noqa: F821
dbutils.widgets.dropdown("mode", "reset", ["reset", "append_change"], "Mode")  # noqa: F821

prefix = dbutils.widgets.get("schema_prefix")  # noqa: F821
if not re.fullmatch(r"[A-Za-z0-9_]*t6_", prefix or ""):
    raise ValueError(f"Refusing to seed: schema_prefix {prefix!r} is not a ticket-6 scratch prefix (…t6_)")
cfg = EnvConfig.from_target("dev", catalog=dbutils.widgets.get("catalog") or None, schema_prefix=prefix)  # noqa: F821
mode = dbutils.widgets.get("mode")  # noqa: F821


def _read(name):
    with open(os.path.join(FIXTURE_DIR, name), newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


branches = [(int(r["branch_id"]), r["branch_name"], r["branch_location"]) for r in _read("t6_sqlserver_branch.csv")]
accounts = [
    (int(r["account_id"]), int(r["customer_id"]), r["account_type"], int(r["balance"]),
     datetime.fromisoformat(r["date_opened"]), r["status"])
    for r in _read("t6_sqlserver_account.csv")
]
if mode == "append_change":
    branches = [(i, "KC Bekasi" if i == 5 else n, loc) for i, n, loc in branches]
    accounts = [(i, c, t, 9_999_000 if i == 4 else b, d, "active" if i == 4 else s) for i, c, t, b, d, s in accounts]

batch_id = f"t6-seed-{mode}-{uuid.uuid4().hex[:8]}"
ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
out = {"mode": mode, "batch_id": batch_id, "tables": {}}
for contract, rows in ((BRONZE_SQLSERVER_BRANCH, branches), (BRONZE_SQLSERVER_ACCOUNT, accounts)):
    assert contract.layer == "bronze"
    fq = cfg.table("bronze", contract.name)
    spark.sql(create_table_sql(cfg, contract))  # noqa: F821
    df = spark.createDataFrame([(*r, ingested_at, batch_id) for r in rows], contract.ddl_schema())  # noqa: F821
    df.write.format("delta").mode("overwrite" if mode == "reset" else "append").saveAsTable(fq)
    out["tables"][fq] = {"written": len(rows), "total": spark.table(fq).count()}  # noqa: F821
print(out)
dbutils.notebook.exit(json.dumps(out))  # noqa: F821
