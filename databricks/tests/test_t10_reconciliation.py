"""Ticket 10: parity fixtures, reconciliation gate and scratch seeding (local Spark + Delta)."""

from __future__ import annotations

import csv
import dataclasses
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.tables import GOLD_OBJECTS, RECONCILIATION_RESULTS, SILVER_TABLES, get_contract
from banking_dwh.dq import reconciliation as rec
from banking_dwh.orchestration import parity_seed

FIX = rec.FIXTURES_DIR
DATA = Path(__file__).resolve().parents[2] / "data_sources"


def _rows(name):
    with open(FIX / name, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------- fixtures (no Spark)
def test_manifest_matches_fixture_files():
    manifest = json.loads((FIX / "manifest.json").read_text())
    for name, meta in manifest["files"].items():
        assert len(_rows(name)) == meta["rows"], name
    for spec in rec.default_specs():
        assert spec.fixture in manifest["files"], spec.fixture


def test_fixture_row_counts_match_restored_source():
    assert {n: len(_rows(f"{n}.csv")) for n in ("dim_branch", "dim_account", "dim_customer", "fact_transaction")} \
        == {"dim_branch": 5, "dim_account": 21, "dim_customer": 20, "fact_transaction": 22}


def test_fact_fixture_follows_talend_first_occurrence_dedup():
    """tUnite(SQL -> Excel -> CSV) + tUniqRow(transaction_id): first stream wins, FK rejects dropped."""
    fact = _rows("fact_transaction.csv")
    ids = [int(r["TransactionID"]) for r in fact]
    assert len(ids) == len(set(ids))
    load = json.loads((FIX / "manifest.json").read_text())["talend_fact_load"]
    assert load["stream_rows"] == {"sqlserver": 10, "excel": 7, "csv": 12}
    assert load["after_tUniqRow"] == len(fact) + len(load["insert_rejected_fk"])
    accounts = {r["AccountID"] for r in _rows("dim_account.csv")}
    assert all(r["AccountID"] in accounts for r in fact)
    with open(DATA / "transaction_csv.csv", encoding="iso-8859-1") as fh:
        csv_src = {int(r["transaction_id"]): r for r in csv.DictReader(fh)}
    by_id = {int(r["TransactionID"]): r for r in fact}
    for dropped in load["duplicates_dropped"]:
        tid = dropped["transaction_id"]
        if dropped["stream"] == "csv" and tid in by_id:  # the CSV duplicate must not be the stored row
            assert (Decimal(by_id[tid]["Amount"]) != Decimal(csv_src[tid]["amount"])
                    or by_id[tid]["TransactionDate"] != csv_src[tid]["transaction_date"]), tid


def test_dim_customer_fixture_is_upcased_left_join():
    cust = _rows("dim_customer.csv")
    for r in cust:
        for col in ("CustomerName", "Address", "Gender"):
            assert r[col] == r[col].upper()
    assert any(r["Email"] != r["Email"].upper() for r in cust)  # email untouched


def test_specs_cover_every_silver_table_and_gold_object():
    names = {s.name.split("(")[0] for s in rec.default_specs()}
    assert names == {f"silver.{t.name}" for t in SILVER_TABLES} | {f"gold.{g.name}" for g in GOLD_OBJECTS}
    fact = next(s for s in rec.default_specs() if s.name == "silver.fact_transaction")
    assert fact.sum_columns == ["amount"] and fact.key == ("transaction_id",)
    acct = next(s for s in rec.default_specs() if s.name == "silver.dim_account")
    assert acct.sum_columns == ["balance"]
    with pytest.raises(ValueError):
        rec.default_specs(["bronze"])


def test_reconciliation_contract():
    assert RECONCILIATION_RESULTS.layer == "ops" and RECONCILIATION_RESULTS.name == "reconciliation_results"
    assert RECONCILIATION_RESULTS.owner_ticket == 10
    assert get_contract("ops", "reconciliation_results") is RECONCILIATION_RESULTS


def test_empty_layer_selection_is_rejected():
    with pytest.raises(ValueError):
        rec.default_specs([])
    with pytest.raises(rec.ReconciliationError):
        rec.assert_reconciled([])


@pytest.mark.parametrize("catalog,prefix,target", [
    ("migration_demo", "bdwh_", "dev"), ("migration_demo", "", "dev"), ("migration_demo", "bdwh_t10_", "prod"),
    ("migration_demo", "t10x_", "dev"), ("banking_dwh", "t10_", "dev"), ("banking_dwh", "bdwh_t10_", "dev"),
])
def test_seeding_refuses_non_scratch_namespaces(catalog, prefix, target):
    cfg = EnvConfig.from_target(target, catalog=catalog, schema_prefix=prefix)
    with pytest.raises(parity_seed.NotScratchNamespaceError):
        parity_seed.assert_scratch(cfg)
    with pytest.raises(parity_seed.NotScratchNamespaceError):
        parity_seed.gold_standin_statements(cfg)


def test_gold_standins_are_built_from_silver_not_fixtures():
    cfg = EnvConfig.from_target("dev", catalog="migration_demo", schema_prefix="bdwh_t10_")
    sql = " ".join(parity_seed.gold_standin_statements(cfg))
    assert "migration_demo.bdwh_t10_gold.fn_balance_per_customer" in sql
    assert "migration_demo.bdwh_t10_silver.fact_transaction" in sql
    assert "bdwh_gold" not in sql and "bdwh_silver" not in sql


# ---------------------------------------------------------------- local Spark
@pytest.fixture(scope="module")
def t10_cfg(spark):
    cfg = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix="t10_")
    for layer in ("silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    return cfg


def _local_specs():
    """Gold specs pointed at the session-scoped stand-ins (OSS Spark can't call qualified SQL functions)."""
    out = []
    for s in rec.default_specs():
        if s.layer == "gold":
            call = s.name.split(".", 1)[1]
            s = dataclasses.replace(s, relation=lambda cfg, c=call: c)
        out.append(s)
    return out


@pytest.fixture()
def seeded(spark, t10_cfg):
    counts = parity_seed.seed_silver(spark, t10_cfg)
    parity_seed.create_gold_standins(spark, t10_cfg, temporary=True)
    return counts


def test_seed_loads_every_silver_table(seeded, spark, t10_cfg):
    assert seeded == {"dim_branch": 5, "dim_account": 21, "dim_customer": 20, "fact_transaction": 22}
    assert spark.table(t10_cfg.table("silver", "fact_transaction")).filter("_loaded_at IS NULL").count() == 0


def test_clean_seed_reconciles(seeded, spark, t10_cfg):
    results = rec.run_reconciliation(spark, t10_cfg, _local_specs())
    failed = [r for r in results if not r.passed]
    assert not failed, failed
    rec.assert_reconciled(results)
    summary = rec.summarize(results)
    fact = summary["objects"]["silver.fact_transaction"]["metrics"]
    assert fact["row_count"] == "22" and fact["distinct_keys"] == "22"
    assert summary["objects"]["gold.v_daily_transaction"]["metrics"]["row_count"] == "9"
    assert summary["objects"]["gold.fn_balance_per_customer('Shelly')"]["metrics"]["row_count"] == "2"
    assert {r.metric for r in results} >= {"row_count", "distinct_keys", "sum_amount", "sum_balance",
                                           "sum_total_amount", "sum_current_balance", "rows_missing",
                                           "rows_unexpected"}


def test_results_are_written(seeded, spark, t10_cfg):
    results = rec.run_reconciliation(spark, t10_cfg, _local_specs()[:1])
    n = rec.write_results(spark, t10_cfg, results, "run-local-1")
    df = spark.table(t10_cfg.table("ops", "reconciliation_results")).filter("run_id = 'run-local-1'")
    assert df.count() == n == len(results)
    assert df.columns == RECONCILIATION_RESULTS.column_names
    dq = rec.as_dq_results(results)
    assert len(dq) == 1 and dq[0].check_type == "reconciliation" and dq[0].passed


@pytest.mark.parametrize("scenario,expect_failed", [
    ("fact_row_missing", {"silver.fact_transaction.row_count", "silver.fact_transaction.sum_amount",
                          "silver.fact_transaction.rows_missing", "gold.v_daily_transaction.sum_total_amount"}),
    ("balance_drift", {"silver.dim_account.sum_balance", "silver.dim_account.rows_missing",
                       "silver.dim_account.rows_unexpected", "gold.fn_balance_per_customer('').sum_current_balance"}),
    ("customer_not_upcased", {"silver.dim_customer.rows_missing", "silver.dim_customer.rows_unexpected"}),
])
def test_broken_seed_fails(seeded, spark, t10_cfg, scenario, expect_failed):
    parity_seed.break_scenarios(spark, t10_cfg, [scenario])
    results = rec.run_reconciliation(spark, t10_cfg, _local_specs())
    failed = {r.check_name for r in results if not r.passed}
    assert expect_failed <= failed
    with pytest.raises(rec.ReconciliationError):
        rec.assert_reconciled(results)
    assert not rec.summarize(results)["passed"]


def test_missing_object_fails_exists_check(spark, t10_cfg):
    spec = dataclasses.replace(rec.default_specs(["silver"])[0], relation=lambda cfg: cfg.table("silver", "nope"))
    [r] = rec.reconcile_spec(spark, t10_cfg, spec)
    assert r.metric == "exists" and not r.passed and "nope" in r.object_name


def test_unknown_break_scenario_rejected(spark, t10_cfg):
    with pytest.raises(ValueError):
        parity_seed.break_scenarios(spark, t10_cfg, ["drop_everything"])


def test_count_metrics_detect_duplicate_keys(spark, t10_cfg):
    spec = rec.default_specs(["silver"])[0]
    exp = rec.expected_df(spark, spec)
    dup = exp.unionByName(exp.limit(1))
    prof = rec.profile(dup, spec)
    assert prof["row_count"] == "6" and prof["distinct_keys"] == "5"
    assert Counter(r.metric for r in rec.reconcile_spec(spark, t10_cfg, spec)) == Counter(
        ["exists", "row_count", "distinct_keys", "rows_missing", "rows_unexpected"])
