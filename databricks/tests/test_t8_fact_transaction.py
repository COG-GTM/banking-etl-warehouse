"""Ticket 8: fact_transaction Silver load (port of Talend Load_FactTransaction)."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.ddl import bronze_statements, star_schema_statements
from banking_dwh.contracts.tables import (
    BRONZE_FILE_TRANSACTION_CSV,
    BRONZE_FILE_TRANSACTION_EXCEL,
    BRONZE_SQLSERVER_BRANCH,
    BRONZE_SQLSERVER_TRANSACTION,
    DIM_ACCOUNT,
    DIM_BRANCH,
    FACT_TRANSACTION,
)
from banking_dwh.dq.checks import DataQualityError
from banking_dwh.silver import fact_transaction as ft

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
sys.path.insert(0, str(FIXTURES))
import t8_seed  # noqa: E402

T0 = datetime(2026, 1, 1, 0, 0, 0)
FILE = "/Volumes/x/y/landing/f.csv"


def _cfg(spark, prefix: str) -> EnvConfig:
    cfg = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix=prefix)
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    for stmt in bronze_statements(cfg, unity_catalog=False) + star_schema_statements(cfg, unity_catalog=False):
        spark.sql(stmt)
    return cfg


def _txn(tid, account=1, amount=100, branch=1, when="2024-01-01 10:00:00", ttype="Deposit", **meta):
    return {"transaction_id": tid, "account_id": account, "transaction_date": datetime.fromisoformat(when),
            "amount": amount, "transaction_type": ttype, "branch_id": branch, **meta}


def _sql(*rows, batch="b1", at=T0):
    return [{**r, "_batch_id": batch, "_ingested_at": at} for r in rows]


def _file(*rows, at=T0, source_file=FILE, rescued=None):
    return [{**r, "amount": None if r["amount"] is None else Decimal(r["amount"]), "_rescued_data": rescued,
             "_source_file": source_file, "_ingested_at": at} for r in rows]


def _seed(spark, cfg, sql=(), excel=(), csv=(), accounts=(1, 2, 3), branches=(1, 2)):
    t8_seed.write_rows(spark, cfg, BRONZE_SQLSERVER_TRANSACTION, list(sql), unity_catalog=False)
    t8_seed.write_rows(spark, cfg, BRONZE_FILE_TRANSACTION_EXCEL, list(excel), unity_catalog=False)
    t8_seed.write_rows(spark, cfg, BRONZE_FILE_TRANSACTION_CSV, list(csv), unity_catalog=False)
    t8_seed.write_rows(spark, cfg, DIM_ACCOUNT, [{"account_id": a, "_loaded_at": T0} for a in accounts],
                       unity_catalog=False)
    t8_seed.write_rows(spark, cfg, DIM_BRANCH, [{"branch_id": b, "_loaded_at": T0} for b in branches],
                       unity_catalog=False)


def _fact(spark, cfg) -> dict[int, dict]:
    return {r["transaction_id"]: r.asDict() for r in spark.table(cfg.table("silver", "fact_transaction")).collect()}


def _dq(summary, name):
    return next(r for r in summary["dq"] if r["check_name"] == name)


# --------------------------------------------------------------------------------------
def test_talend_job_stream_order_matches_source_priority():
    """tUnite_1 inputs in the .item XML: row1 SQL, row2 Excel, row3 CSV."""
    import xml.etree.ElementTree as ET
    import zipfile

    repo = Path(__file__).resolve().parents[2]
    with zipfile.ZipFile(repo / "talend_jobs" / "Load_FactTransaction.zip") as z:
        item = next(n for n in z.namelist() if n.endswith("Load_FactTransaction_0.1.item"))
        root = ET.fromstring(z.read(item))
    names = {}
    for node in root.iter():
        if node.tag.endswith("node"):
            uid = next(p.get("value") for p in node if p.get("name") == "UNIQUE_NAME")
            names[uid] = node.get("componentName")
    into_unite = [c.get("source") for c in root.iter() if c.tag.endswith("connection") and c.get("target") == "tUnite_1"]
    assert [names[s] for s in into_unite] == ["tMSSqlInput", "tFileInputExcel", "tFileInputDelimited"]
    assert [s.source_system for s in sorted(ft.SOURCES, key=lambda s: s.priority)] == ["sqlserver", "excel", "csv"]


def test_real_sources_union_dedup_and_quarantine(spark):
    """Real rows from sample.bak (10), transaction_excel.xlsx (7), transaction_csv.csv (12)."""
    cfg = _cfg(spark, "t8_")
    counts = t8_seed.seed_scratch(spark, cfg, unity_catalog=False)
    assert counts == {"bronze.sqlserver_transaction_db": 10, "bronze.file_transaction_excel": 7,
                      "bronze.file_transaction_csv": 12, "silver.dim_branch": 5, "silver.dim_account": 21}

    s = ft.load_fact_transaction(spark, cfg, run_id="real-1", unity_catalog=False)
    assert s["source_counts"] == {"sqlserver": 10, "excel": 7, "csv": 12}
    assert s["union_count"] == 29
    assert s["dedup_count"] == 25 and s["duplicates_removed"] == 4  # ids 6, 7 (SQL vs Excel), 14, 15 (Excel vs CSV)
    assert s["orphan_rows"] == 3 and s["orphans_by_fk"] == {"FK_FactTransaction_DimAccount": 3}  # accounts 22, 23
    assert s["final_count"] == 22
    assert s["winners_by_source"] == {"sqlserver": 10, "excel": 5, "csv": 7}
    assert s["rejects_by_reason"] == {"duplicate_transaction_id": 4, "FK_FactTransaction_DimAccount": 3}

    fact = _fact(spark, cfg)
    assert sorted(fact) == list(range(1, 23))
    assert fact[6]["_source_system"] == "sqlserver" and fact[6]["transaction_date"] == datetime(2022, 2, 21, 13, 10)
    assert fact[14]["_source_system"] == "excel"
    assert fact[3]["amount"] == Decimal("10000000.0000")
    assert all(r["_loaded_at"] is not None for r in fact.values())
    types = dict(spark.table(cfg.table("silver", "fact_transaction")).dtypes)
    assert types["amount"] == "decimal(19,4)" and types["transaction_date"] == "timestamp"

    assert all(_dq(s, n)["passed"] for n in ("pk_not_null.fact_transaction", "pk_unique.fact_transaction",
                                              "FK_FactTransaction_DimAccount", "FK_FactTransaction_DimBranch"))
    q = _dq(s, "FK_FactTransaction_DimAccount.quarantined")
    assert not q["passed"] and q["failed_rows"] == 3
    assert ft.dq_failures(s) == [] and len(ft.dq_failures(s, include_quarantine=True)) == 1

    rejects = spark.table(cfg.table("ops", "fact_transaction_rejects"))
    orphan_ids = sorted(r[0] for r in rejects.filter("reject_reason = 'FK_FactTransaction_DimAccount'")
                        .select("transaction_id").collect())
    assert orphan_ids == [23, 24, 25]
    dup = rejects.filter("reject_reason = 'duplicate_transaction_id' AND transaction_id = 14").collect()[0]
    assert dup["_source_system"] == "csv" and json.loads(dup["details"]) == {"kept_source": "excel"}
    dq_rows = spark.table(cfg.table("ops", "dq_results")).filter("run_id = 'real-1'").count()
    assert dq_rows == len(s["dq"]) == 7


def test_union_of_three_streams_conformed(spark):
    cfg = _cfg(spark, "t8u_")
    _seed(spark, cfg, sql=_sql(_txn(1, amount=5)), excel=_file(_txn(2, amount="6.5")), csv=_file(_txn(3, amount="7.25")))
    streams = ft.read_streams(spark, cfg)
    u = ft.union_streams(streams[x.source_system] for x in ft.SOURCES)
    assert u.columns == FACT_TRANSACTION.business_columns + list(ft._STAGE_EXTRA)
    rows = {r["transaction_id"]: r for r in u.collect()}
    assert {k: (r["_source_system"], r["amount"]) for k, r in rows.items()} == {
        1: ("sqlserver", Decimal("5.0000")), 2: ("excel", Decimal("6.5000")), 3: ("csv", Decimal("7.2500"))}
    assert rows[1]["_source_file"] == "sqlserver:sqlserver_transaction_db#b1"
    assert dict(u.dtypes)["amount"] == "decimal(19,4)"


def test_cross_source_duplicates_resolved_by_priority(spark):
    cfg = _cfg(spark, "t8p_")
    _seed(spark, cfg,
          sql=_sql(_txn(100, amount=1)),
          excel=_file(_txn(100, amount="2"), _txn(101, amount="2")),
          csv=_file(_txn(100, amount="3"), _txn(101, amount="3"), _txn(102, amount="3")))
    streams = ft.read_streams(spark, cfg)
    for order in (ft.SOURCES, tuple(reversed(ft.SOURCES))):  # union input order must not matter
        winners, losers = ft.deduplicate(ft.union_streams(streams[x.source_system] for x in order))
        got = {r["transaction_id"]: (r["_source_system"], int(r["amount"])) for r in winners.collect()}
        assert got == {100: ("sqlserver", 1), 101: ("excel", 2), 102: ("csv", 3)}
        assert losers.count() == 3
    s = ft.load_fact_transaction(spark, cfg, run_id="p", unity_catalog=False)
    assert s["union_count"] == 6 and s["dedup_count"] == 3 and s["final_count"] == 3
    assert {k: v["_source_system"] for k, v in _fact(spark, cfg).items()} == {100: "sqlserver", 101: "excel", 102: "csv"}


def test_latest_sql_batch_and_latest_file_snapshot(spark):
    cfg = _cfg(spark, "t8b_")
    later = datetime(2026, 2, 1)
    _seed(spark, cfg,
          sql=_sql(_txn(1, amount=1), _txn(300, amount=1), batch="old", at=T0) + _sql(_txn(1, amount=9), batch="new", at=later),
          csv=_file(_txn(5, amount="1"), _txn(6, amount="1"), at=T0, source_file="/a.csv")
          + _file(_txn(5, amount="2"), at=later, source_file="/b.csv"))
    s = ft.load_fact_transaction(spark, cfg, run_id="b", unity_catalog=False)
    fact = _fact(spark, cfg)
    assert s["source_counts"] == {"sqlserver": 1, "excel": 0, "csv": 1}
    # 300 left the SQL source, 6 left the CSV (only in the superseded file /a.csv): both absent.
    assert sorted(fact) == [1, 5] and fact[1]["amount"] == Decimal(9) and fact[5]["amount"] == Decimal(2)

    # all_files: every ingested file is read, the newest ingestion of a key wins.
    s = ft.load_fact_transaction(spark, cfg, run_id="b2", file_snapshot="all_files", unity_catalog=False)
    fact = _fact(spark, cfg)
    assert s["merge"] == {"inserted": 1, "updated": 0, "deleted": 0}
    assert sorted(fact) == [1, 5, 6] and fact[5]["amount"] == Decimal(2)
    with pytest.raises(ValueError, match="file_snapshot"):
        ft.load_fact_transaction(spark, cfg, file_snapshot="newest", unity_catalog=False)


def test_empty_sql_extract_deletes_sql_rows(spark):
    cfg = _cfg(spark, "t8e_")
    _seed(spark, cfg, sql=_sql(_txn(1), _txn(2), batch="old", at=T0), csv=_file(_txn(3, amount="1")))
    assert ft.load_fact_transaction(spark, cfg, run_id="e1", unity_catalog=False)["final_count"] == 3

    # Newer extraction run: branch extracted, transaction_db returned zero rows.
    t8_seed.write_rows(spark, cfg, BRONZE_SQLSERVER_BRANCH,
                       [{"branch_id": 1, "_batch_id": "new", "_ingested_at": datetime(2026, 2, 1)}], unity_catalog=False)
    s = ft.load_fact_transaction(spark, cfg, run_id="e2", unity_catalog=False)
    assert s["source_counts"]["sqlserver"] == 0
    assert s["merge"] == {"inserted": 0, "updated": 0, "deleted": 2}
    assert sorted(_fact(spark, cfg)) == [3]

    # A table of the older run landing late must not make that run current again.
    t8_seed.write_rows(spark, cfg, BRONZE_SQLSERVER_BRANCH,
                       [{"branch_id": 1, "_batch_id": "new", "_ingested_at": datetime(2026, 2, 1)},
                        {"branch_id": 1, "_batch_id": "old", "_ingested_at": datetime(2026, 3, 1)}], unity_catalog=False)
    s = ft.load_fact_transaction(spark, cfg, run_id="e3", unity_catalog=False)
    assert s["source_counts"]["sqlserver"] == 0 and s["merge"] == {"inserted": 0, "updated": 0, "deleted": 0}


def test_input_rejects_quarantined_before_dedup(spark):
    cfg = _cfg(spark, "t8r_")
    _seed(spark, cfg,
          excel=_file(_txn(200, amount="1"), rescued='{"amount":"abc"}') + _file(_txn(None, amount="1")),
          csv=_file(_txn(200, amount="2")))
    s = ft.load_fact_transaction(spark, cfg, run_id="r", unity_catalog=False)
    assert s["input_rejects"] == 2 and s["final_count"] == 1
    assert _fact(spark, cfg)[200]["_source_system"] == "csv"
    assert s["rejects_by_reason"] == {"bronze_rescued_data": 1, "null_transaction_id": 1}
    assert ft.dq_failures(s) == []  # regular PK/FK checks pass on the loaded table
    quarantined = ft.dq_failures(s, include_regular=False, include_quarantine=True)
    assert [(r["check_name"], r["failed_rows"]) for r in quarantined] == [("fact_transaction.input_rejects.quarantined", 2)]


def test_idempotent_rerun_and_truncate_semantics(spark):
    cfg = _cfg(spark, "t8i_")
    _seed(spark, cfg, sql=_sql(_txn(1), _txn(2)), csv=_file(_txn(3, amount="10"), _txn(4, account=99, amount="1")))
    s1 = ft.load_fact_transaction(spark, cfg, run_id="i1", unity_catalog=False)
    assert s1["merge"] == {"inserted": 3, "updated": 0, "deleted": 0} and s1["final_count"] == 3
    before = _fact(spark, cfg)

    s2 = ft.load_fact_transaction(spark, cfg, run_id="i2", unity_catalog=False)
    assert s2["merge"] == {"inserted": 0, "updated": 0, "deleted": 0}
    assert _fact(spark, cfg) == before  # same rows, same _loaded_at
    assert s2["rejects_count"] == s1["rejects_count"] == 1
    rejects = spark.table(cfg.table("ops", "fact_transaction_rejects")).collect()
    assert {r["run_id"] for r in rejects} == {"i2"}

    # Source changes: id 3 amount corrected, id 2 gone from SQL -> update 1, delete 1 (TRUNCATE + INSERT result).
    _seed(spark, cfg, sql=_sql(_txn(1)), csv=_file(_txn(3, amount="11"), _txn(4, account=99, amount="1")))
    s3 = ft.load_fact_transaction(spark, cfg, run_id="i3", unity_catalog=False)
    assert s3["merge"] == {"inserted": 0, "updated": 1, "deleted": 1}
    after = _fact(spark, cfg)
    assert sorted(after) == [1, 3] and after[3]["amount"] == Decimal(11)
    assert after[1]["_loaded_at"] == before[1]["_loaded_at"]


def test_ri_failure_surfaced(spark):
    cfg = _cfg(spark, "t8f_")
    _seed(spark, cfg, sql=_sql(_txn(1), _txn(2, account=77), _txn(3, branch=88), _txn(4, account=None)))

    with pytest.raises(DataQualityError, match="violate foreign keys"):
        ft.load_fact_transaction(spark, cfg, run_id="f0", orphan_policy="fail", unity_catalog=False)
    assert spark.table(cfg.table("silver", "fact_transaction")).count() == 0

    q = ft.load_fact_transaction(spark, cfg, run_id="f1", unity_catalog=False)
    assert sorted(_fact(spark, cfg)) == [1, 4]  # NULL FK passes, like SQL Server
    assert q["orphans_by_fk"] == {"FK_FactTransaction_DimAccount": 1, "FK_FactTransaction_DimBranch": 1}
    assert {r["check_name"] for r in ft.dq_failures(q, include_quarantine=True)} == {
        "FK_FactTransaction_DimAccount.quarantined", "FK_FactTransaction_DimBranch.quarantined"}

    s = ft.load_fact_transaction(spark, cfg, run_id="f2", orphan_policy="load", unity_catalog=False)
    assert s["final_count"] == 4
    failed = {r["check_name"]: r["failed_rows"] for r in ft.dq_failures(s)}
    assert failed == {"FK_FactTransaction_DimAccount": 1, "FK_FactTransaction_DimBranch": 1}
    # notebook: fail_on_dq=false + fail_on_quarantine=true must not raise on regular FK failures
    assert ft.dq_failures(s, include_regular=False, include_quarantine=True) == []
    stored = spark.table(cfg.table("ops", "dq_results")).filter("run_id = 'f2' AND NOT passed").collect()
    assert {r["check_name"] for r in stored} == set(failed)
    sample = json.loads(next(r for r in stored if r["check_name"] == "FK_FactTransaction_DimAccount")["details"])
    assert sample["sample"] == [{"transaction_id": 2, "account_id": 77}]


def test_rejects_invalid_policy_and_seed_guard(spark):
    cfg = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix="t8z_")
    with pytest.raises(ValueError, match="orphan_policy"):
        ft.load_fact_transaction(spark, cfg, orphan_policy="drop")
    for prefix in ("bdwh_", "", "t9_"):
        with pytest.raises(ValueError, match="Refusing to seed"):
            t8_seed.seed_scratch(spark, EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix=prefix))
    gold_like = FACT_TRANSACTION.__class__("gold", "x", FACT_TRANSACTION.columns, "")
    with pytest.raises(ValueError, match="Refusing to seed layer"):
        t8_seed.write_rows(spark, cfg, gold_like, [])
