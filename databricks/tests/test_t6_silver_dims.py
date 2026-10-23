"""Ticket 6: Silver dim_branch / dim_account SCD-1 MERGE loads (Talend Load_DimBranch, Load_DimAccount).

Bronze inputs are built from ``fixtures/t6_sqlserver_{branch,account}.csv`` (the real rows of
``data_sources/sample.bak``) in the shape of the T3 Bronze contracts, so these tests don't depend on
T3 code. ``fixtures/t6_legacy_dim_*.csv`` are the legacy DWH tables produced by the Talend-equivalent
INSERT on SQL Server 2022 and are the parity oracle for the initial load.
"""

from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.ddl import create_table_sql
from banking_dwh.contracts.tables import (
    BRONZE_SQLSERVER_ACCOUNT, BRONZE_SQLSERVER_BRANCH, DIM_ACCOUNT, DIM_BRANCH, DQ_RESULTS,
)
from banking_dwh.dq.checks import DataQualityError
from banking_dwh.silver import dim_account, dim_branch
from banking_dwh.silver.merge import latest_per_key, merge_sql, run_pk_checks

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
T0 = datetime(2026, 10, 23, 1, 0, 0)
T1 = datetime(2026, 10, 23, 2, 0, 0)
T2 = datetime(2026, 10, 23, 3, 0, 0)
L0 = datetime(2026, 10, 23, 10, 0, 0)
L1 = datetime(2026, 10, 23, 11, 0, 0)
L2 = datetime(2026, 10, 23, 12, 0, 0)


def _read(name: str) -> list[dict]:
    with open(FIXTURES / name, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _branch_rows() -> list[tuple]:
    return [(int(r["branch_id"]), r["branch_name"], r["branch_location"]) for r in _read("t6_sqlserver_branch.csv")]


def _account_rows() -> list[tuple]:
    return [
        (int(r["account_id"]), int(r["customer_id"]), r["account_type"], int(r["balance"]),
         datetime.fromisoformat(r["date_opened"]), r["status"])
        for r in _read("t6_sqlserver_account.csv")
    ]


@pytest.fixture()
def cfg(spark) -> EnvConfig:
    c = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix="t6_")
    for layer in ("bronze", "silver", "ops"):
        spark.sql(f"DROP SCHEMA IF EXISTS {c.schema(layer)} CASCADE")
        spark.sql(f"CREATE SCHEMA {c.schema(layer)}")
    for contract in (BRONZE_SQLSERVER_BRANCH, BRONZE_SQLSERVER_ACCOUNT):
        spark.sql(create_table_sql(c, contract, unity_catalog=False))
    return c


def _append_bronze(spark, cfg, contract, rows, ingested_at: datetime, batch_id: str) -> None:
    data = [(*r, ingested_at, batch_id) for r in rows]
    df = spark.createDataFrame(data, contract.ddl_schema())
    df.write.format("delta").mode("append").saveAsTable(cfg.table(contract.layer, contract.name))


def _seed(spark, cfg, ingested_at=T0, batch="b0"):
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_BRANCH, _branch_rows(), ingested_at, batch)
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_ACCOUNT, _account_rows(), ingested_at, batch)


def _silver(spark, cfg, contract) -> dict:
    key = contract.primary_key[0]
    return {r[key]: r.asDict() for r in spark.table(cfg.table("silver", contract.name)).collect()}


def _load_both(spark, cfg, loaded_at, **kw):
    return (
        dim_branch.load(spark, cfg, loaded_at=loaded_at, unity_catalog=False, **kw),
        dim_account.load(spark, cfg, loaded_at=loaded_at, unity_catalog=False, **kw),
    )


# ---------------------------------------------------------------- initial load + parity
def test_initial_load_counts_and_metrics(spark, cfg):
    _seed(spark, cfg)
    b, a = _load_both(spark, cfg, L0)
    assert (b["source_rows"], b["inserted"], b["updated"], b["target_rows"]) == (5, 5, 0, 5)
    assert (a["source_rows"], a["inserted"], a["updated"], a["target_rows"]) == (21, 21, 0, 21)
    assert b["null_key_rows"] == a["null_key_rows"] == 0
    assert b["duplicate_key_rows"] == a["duplicate_key_rows"] == 0
    for out in (b, a):
        assert {d["check_name"].split(".")[0] for d in out["dq"]} == {"pk_not_null", "pk_unique"}
        assert all(d["passed"] for d in out["dq"])
    assert {r["_loaded_at"] for r in _silver(spark, cfg, DIM_ACCOUNT).values()} == {L0}


def test_initial_load_matches_legacy_dwh_exactly(spark, cfg):
    """Parity with DWH.dbo.DimBranch / DimAccount as loaded by the Talend-equivalent INSERT."""
    _seed(spark, cfg)
    _load_both(spark, cfg, L0)
    branches = _silver(spark, cfg, DIM_BRANCH)
    legacy_b = {int(r["BranchID"]): (r["BranchName"], r["BranchLocation"]) for r in _read("t6_legacy_dim_branch.csv")}
    assert {k: (v["branch_name"], v["branch_location"]) for k, v in branches.items()} == legacy_b

    accounts = _silver(spark, cfg, DIM_ACCOUNT)
    legacy_a = {
        int(r["AccountID"]): (int(r["CustomerID"]), r["AccountType"], Decimal(r["Balance"]),
                              date.fromisoformat(r["DateOpened"]), r["Status"])
        for r in _read("t6_legacy_dim_account.csv")
    }
    got = {k: (v["customer_id"], v["account_type"], v["balance"], v["date_opened"], v["status"])
           for k, v in accounts.items()}
    assert got == legacy_a


# ---------------------------------------------------------------- type mapping
def test_type_mapping_matches_silver_contract(spark, cfg):
    _seed(spark, cfg)
    _load_both(spark, cfg, L0)
    for contract in (DIM_BRANCH, DIM_ACCOUNT):
        df = spark.table(cfg.table("silver", contract.name))
        assert [f.name for f in df.schema.fields] == contract.column_names
        got = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        want = {c.name: c.type.lower() for c in contract.columns}
        assert got == want
    acct1 = _silver(spark, cfg, DIM_ACCOUNT)[1]
    assert acct1["balance"] == Decimal("1500000.0000")
    assert acct1["date_opened"] == date(2020, 5, 1)  # datetime2 2020-05-01 09:00:00 -> DATE


def test_transform_casts_and_truncates_time(spark):
    df = spark.createDataFrame(
        [(7, 3, "saving", 2147483647, datetime(2021, 12, 31, 23, 59, 59), "active", T0, "b")],
        BRONZE_SQLSERVER_ACCOUNT.ddl_schema(),
    )
    r = dim_account.transform(df).collect()[0]
    assert r["balance"] == Decimal("2147483647.0000")
    assert r["date_opened"] == date(2021, 12, 31)
    assert dim_account.transform(df).schema["balance"].dataType.simpleString() == "decimal(19,4)"


# ---------------------------------------------------------------- reruns, updates, inserts
def test_rerun_without_changes_is_idempotent(spark, cfg):
    _seed(spark, cfg)
    _load_both(spark, cfg, L0)
    before_b, before_a = _silver(spark, cfg, DIM_BRANCH), _silver(spark, cfg, DIM_ACCOUNT)
    b, a = _load_both(spark, cfg, L1)
    assert (b["inserted"], b["updated"], b["target_rows"]) == (0, 0, 5)
    assert (a["inserted"], a["updated"], a["target_rows"]) == (0, 0, 21)
    assert _silver(spark, cfg, DIM_BRANCH) == before_b
    assert _silver(spark, cfg, DIM_ACCOUNT) == before_a  # _loaded_at untouched as well


def test_rerun_with_same_rows_reextracted_is_idempotent(spark, cfg):
    """A second Bronze batch with identical values (daily re-extract) changes nothing in Silver."""
    _seed(spark, cfg, T0, "b0")
    _load_both(spark, cfg, L0)
    _seed(spark, cfg, T1, "b1")
    b, a = _load_both(spark, cfg, L1)
    assert (b["duplicate_key_rows"], b["inserted"], b["updated"]) == (5, 0, 0)
    assert (a["duplicate_key_rows"], a["inserted"], a["updated"]) == (21, 0, 0)
    assert {r["_loaded_at"] for r in _silver(spark, cfg, DIM_ACCOUNT).values()} == {L0}


def test_changed_row_is_updated_scd1(spark, cfg):
    _seed(spark, cfg)
    _load_both(spark, cfg, L0)
    branches = [(i, "KC Bekasi" if i == 5 else n, loc) for i, n, loc in _branch_rows()]
    accounts = [(i, c, t, 9_999_000 if i == 4 else bal, d, "active" if i == 4 else s)
                for i, c, t, bal, d, s in _account_rows()]
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_BRANCH, branches, T1, "b1")
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_ACCOUNT, accounts, T1, "b1")
    b, a = _load_both(spark, cfg, L1)
    assert (b["inserted"], b["updated"], b["target_rows"]) == (0, 1, 5)
    assert (a["inserted"], a["updated"], a["target_rows"]) == (0, 1, 21)
    sb, sa = _silver(spark, cfg, DIM_BRANCH), _silver(spark, cfg, DIM_ACCOUNT)
    assert sb[5]["branch_name"] == "KC Bekasi" and sb[5]["_loaded_at"] == L1
    assert {k for k, v in sb.items() if v["_loaded_at"] == L0} == {1, 2, 3, 4}
    assert (sa[4]["balance"], sa[4]["status"], sa[4]["_loaded_at"]) == (Decimal("9999000.0000"), "active", L1)
    assert sum(1 for v in sa.values() if v["_loaded_at"] == L1) == 1


def test_update_to_null_is_detected(spark, cfg):
    """Null-safe comparison: a value becoming NULL is a change (plain = would miss it)."""
    _seed(spark, cfg)
    _load_both(spark, cfg, L0)
    branches = [(i, n, None if i == 2 else loc) for i, n, loc in _branch_rows()]
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_BRANCH, branches, T1, "b1")
    out = dim_branch.load(spark, cfg, loaded_at=L1, unity_catalog=False)
    assert out["updated"] == 1
    assert _silver(spark, cfg, DIM_BRANCH)[2]["branch_location"] is None


def test_new_row_is_inserted(spark, cfg):
    _seed(spark, cfg)
    _load_both(spark, cfg, L0)
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_BRANCH, [(6, "KC Bandung", "Jl. Asia Afrika No 8")], T1, "b1")
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_ACCOUNT,
                   [(22, 16, "saving", 1_000, datetime(2024, 1, 2, 8, 30), "active")], T1, "b1")
    b, a = _load_both(spark, cfg, L1)
    assert (b["inserted"], b["updated"], b["target_rows"]) == (1, 0, 6)
    assert (a["inserted"], a["updated"], a["target_rows"]) == (1, 0, 22)
    assert _silver(spark, cfg, DIM_BRANCH)[6]["_loaded_at"] == L1
    assert _silver(spark, cfg, DIM_ACCOUNT)[22]["date_opened"] == date(2024, 1, 2)
    # rows absent from a later Bronze batch are kept (no deletes, same as Talend)
    assert _silver(spark, cfg, DIM_BRANCH)[1]["_loaded_at"] == L0


# ---------------------------------------------------------------- dedup / bad keys
def test_duplicate_bronze_keys_do_not_break_merge_latest_wins(spark, cfg):
    _seed(spark, cfg, T0, "b0")
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_BRANCH, [(1, "KC Jakarta Pusat", "Jl. Thamrin No 1")], T2, "b2")
    _append_bronze(spark, cfg, BRONZE_SQLSERVER_BRANCH, [(1, "KC Jakarta Lama", "Jl. Kota No 1")], T1, "b1")
    out = dim_branch.load(spark, cfg, loaded_at=L0, unity_catalog=False)
    assert (out["source_rows"], out["duplicate_key_rows"], out["inserted"]) == (7, 2, 5)
    assert _silver(spark, cfg, DIM_BRANCH)[1]["branch_name"] == "KC Jakarta Pusat"


def test_duplicate_key_in_same_batch_is_deterministic(spark):
    df = spark.createDataFrame(
        [(1, "B", "x", T0, "b0"), (1, "A", "x", T0, "b0"), (2, "C", "y", T0, "b0")],
        "branch_id INT, branch_name STRING, branch_location STRING, _ingested_at TIMESTAMP, _batch_id STRING",
    )
    rows = {r["branch_id"]: r["branch_name"] for r in latest_per_key(df, ["branch_id"]).collect()}
    assert rows == {1: "A", 2: "C"}


def test_null_keys_are_dropped_and_counted(spark, cfg):
    _seed(spark, cfg)
    df = spark.table(cfg.table("bronze", "sqlserver_branch"))
    nulls = df.limit(1).selectExpr("CAST(NULL AS INT) AS branch_id", "'ghost' AS branch_name",
                                   "branch_location", "_ingested_at", "_batch_id")
    src = dim_branch.transform(df.unionByName(nulls))
    from banking_dwh.silver.merge import scd1_merge
    out = scd1_merge(spark, cfg, DIM_BRANCH, src, loaded_at=L0, unity_catalog=False)
    assert (out.null_key_rows, out.inserted, out.target_rows) == (1, 5, 5)


# ---------------------------------------------------------------- DQ wiring
def test_dq_results_written_and_failure_raises(spark, cfg):
    _seed(spark, cfg)
    _load_both(spark, cfg, L0, dq_run_id="t6-run")
    rows = spark.table(cfg.table("ops", DQ_RESULTS.name)).filter("run_id = 't6-run'").collect()
    assert sorted(r["check_name"] for r in rows) == [
        "pk_not_null.dim_account", "pk_not_null.dim_branch", "pk_unique.dim_account", "pk_unique.dim_branch",
    ]
    assert all(r["passed"] for r in rows)
    spark.sql(f"INSERT INTO {cfg.table('silver', 'dim_branch')} VALUES (1, 'dup', 'dup', TIMESTAMP'2026-01-01')")
    with pytest.raises(DataQualityError):
        run_pk_checks(spark, cfg, DIM_BRANCH)
    assert not all(d["passed"] for d in run_pk_checks(spark, cfg, DIM_BRANCH, fail_on_error=False))


# ---------------------------------------------------------------- SQL safety + notebooks
def test_merge_sql_shape_and_identifier_validation():
    sql = merge_sql("cat.sch.dim_branch", "v", ["branch_id"], ["branch_id", "branch_name", "_loaded_at"])
    assert "ON t.branch_id = s.branch_id" in sql
    assert "WHEN MATCHED AND (NOT (t.branch_name <=> s.branch_name)) THEN UPDATE" in sql
    assert "_loaded_at <=>" not in sql  # _loaded_at alone never counts as a change
    with pytest.raises(ValueError):
        merge_sql("cat.sch.dim; DROP TABLE x", "v", ["branch_id"], ["branch_id"])
    with pytest.raises(ValueError):
        merge_sql("cat.sch.t", "v", ["branch_id"], ["branch_id", "x = 1 --"])


@pytest.mark.parametrize("nb,module", [("01_load_dim_branch.py", "dim_branch"), ("02_load_dim_account.py", "dim_account")])
def test_notebooks_follow_pattern(nb, module):
    text = (Path(__file__).resolve().parents[1] / "notebooks" / "silver" / nb).read_text()
    assert text.startswith("# Databricks notebook source")
    assert "# MAGIC %run ../00_setup/00_config" in text
    assert f"from banking_dwh.silver import {module}" in text
    assert "dbutils.notebook.exit(json.dumps(" in text
    assert "migration_demo" not in text  # names come from cfg, never hard-coded
