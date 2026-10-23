"""Ticket 9: gold analytics objects replacing sp_DailyTransaction / sp_BalancePerCustomer.

Expected results come from the ORIGINAL procedures run on SQL Server 2022
(fixtures/t9_legacy_parity/expected.json, rebuilt by build_expected.py). Local OSS Spark cannot call
a schema-qualified SQL table function, so the functions are created as TEMPORARY FUNCTIONs with the
same body (gold_object_statements(..., temporary_functions=True)); the view is created for real.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path

import pytest

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.tables import GOLD_OBJECTS
from banking_dwh.gold import analytics as ga
from banking_dwh.gold import parity

ROOT = Path(__file__).resolve().parents[1]
DATASET_PREFIX = {"legacy": "t9_", "edge": "t9_edge_"}


def _cfg(dataset: str) -> EnvConfig:
    return EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix=DATASET_PREFIX[dataset])


@pytest.fixture(scope="module")
def seeded(spark):
    """Both datasets seeded into their own local t9 scratch namespaces, gold view created."""
    out = {}
    for dataset in parity.DATASETS:
        cfg = _cfg(dataset)
        for layer in ("silver", "gold"):
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
        out[dataset] = (cfg, parity.seed_silver(spark, cfg, dataset))
        spark.sql(ga.create_view_sql(cfg))
    return out


def _use(spark, cfg: EnvConfig) -> None:
    for stmt in ga.gold_object_statements(cfg, temporary_functions=True)[1:]:
        spark.sql(stmt)


def _call(spark, case: dict):
    sql = (ga.call_daily_transaction_sql(None) if case["proc"] == "sp_DailyTransaction"
           else ga.call_balance_per_customer_sql(None))
    return spark.sql(sql, args=parity.case_args(case)).collect()


# ---------------------------------------------------------------- generated DDL
def test_committed_prod_ddl_matches_generator(capsys):
    ga.main(["--target", "prod"])
    assert (ROOT / "ddl" / "04_gold_objects.sql").read_text() == capsys.readouterr().out


def test_ddl_uses_cfg_names_only():
    cfg = EnvConfig.from_target("dev", schema_prefix="bdwh_t9_")
    stmts = ga.gold_object_statements(cfg)
    assert [s.split("(")[0].split()[-1] for s in stmts] == [
        f"migration_demo.bdwh_t9_gold.{o.name}" for o in GOLD_OBJECTS]
    joined = "\n".join(stmts)
    assert "banking_dwh.silver" not in joined and "banking_dwh.gold" not in joined and "bdwh_silver" not in joined
    assert joined.count("migration_demo.bdwh_t9_silver.fact_transaction") == 3


def test_seed_guard_refuses_non_t9_namespaces(spark):
    for cfg in (EnvConfig.from_target("prod"), EnvConfig.from_target("dev"),
                EnvConfig.from_target("dev", schema_prefix="bdwh_t8_"),
                EnvConfig.from_target("dev", catalog="banking_dwh", schema_prefix="t9_"),
                EnvConfig.from_target("dev", catalog="other_catalog", schema_prefix="bdwh_t9_")):
        with pytest.raises(PermissionError):
            parity.seed_silver(spark, cfg, "legacy")
    parity.assert_t9_scratch(EnvConfig.from_target("dev", schema_prefix="bdwh_t9_"))
    parity.assert_t9_scratch(EnvConfig.from_target("dev", schema_prefix="bdwh_t9_edge_"))


def test_seed_counts_match_legacy_dwh(seeded):
    assert seeded["legacy"][1] == {"dim_branch": 5, "dim_account": 21, "dim_customer": 20, "fact_transaction": 22}
    assert seeded["edge"][1] == {"dim_branch": 1, "dim_account": 17, "dim_customer": 12, "fact_transaction": 12}


# ---------------------------------------------------------------- contracts
@pytest.mark.parametrize("obj", GOLD_OBJECTS, ids=lambda o: o.name)
def test_output_schema_matches_gold_contract(spark, seeded, obj):
    cfg = seeded["legacy"][0]
    _use(spark, cfg)
    if obj.kind == "view":
        df = spark.table(cfg.table("gold", obj.name))
    elif obj.name == ga.FN_DAILY_TRANSACTION:
        df = spark.sql(ga.call_daily_transaction_sql(None), args={"start_date": date(2024, 1, 1), "end_date": date(2024, 1, 31)})
    else:
        df = spark.sql(ga.call_balance_per_customer_sql(None), args={"customer_name": "a"})
    got = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    assert got == [(c.name, c.type.lower()) for c in obj.output]


def test_persistent_functions_can_be_created(spark, seeded):
    cfg = seeded["legacy"][0]
    for stmt in ga.gold_object_statements(cfg)[1:]:
        spark.sql(stmt)
    names = {r[0].split(".")[-1] for r in spark.sql(f"SHOW USER FUNCTIONS IN {cfg.schema('gold')}").collect()}
    assert {ga.FN_DAILY_TRANSACTION, ga.FN_BALANCE_PER_CUSTOMER} <= names


# ---------------------------------------------------------------- parity with SQL Server
@pytest.mark.parametrize(
    "case", parity.expected_cases(),
    ids=lambda c: f"{c['dataset']}-{c['proc']}-{str(list(c['params'].values()))[:40]}")
def test_parity_with_original_procedure(spark, seeded, case):
    _use(spark, seeded[case["dataset"]][0])
    ok, msg = parity.compare(parity.actual_rows(_call(spark, case), case), case)
    assert ok, msg


def test_expected_fixture_covers_key_semantics():
    by = {(c["dataset"], str(c["params"].get("customer_name"))): len(c["rows"]) for c in parity.expected_cases()}
    assert by[("legacy", "shelly")] == by[("legacy", "SHELLY")] == 2  # CI collation
    assert by[("edge", "jose")] == 0 and by[("edge", "josé")] == 1    # accent-sensitive
    assert by[("edge", "ana_lia")] == 2                               # '_' wildcard
    assert by[("edge", "None")] == 0 and by[("edge", "")] == 12       # NULL vs empty


@pytest.mark.parametrize("dataset", parity.DATASETS)
def test_view_equals_unbounded_function(spark, seeded, dataset):
    cfg = seeded[dataset][0]
    _use(spark, cfg)
    view = spark.sql(f"SELECT * FROM {cfg.table('gold', ga.V_DAILY_TRANSACTION)} ORDER BY transaction_date").collect()
    fn = spark.sql(ga.call_daily_transaction_sql(None),
                   args={"start_date": date(1900, 1, 1), "end_date": date(9999, 12, 31)}).collect()
    assert view == fn and len(view) > 0


def test_function_body_returns_rows_ordered_by_date(spark, seeded):
    _use(spark, seeded["legacy"][0])
    rows = spark.sql(f"SELECT * FROM {ga.FN_DAILY_TRANSACTION}(DATE'1900-01-01', DATE'9999-12-31')").collect()
    dates = [r.transaction_date for r in rows]
    assert dates == sorted(dates) and len(dates) == 9


@pytest.mark.parametrize("tz", ["America/Los_Angeles", "Asia/Jakarta"])
def test_dates_do_not_depend_on_session_time_zone(spark, seeded, tz):
    case = next(c for c in parity.expected_cases("edge") if c["params"] == {"start_date": "2023-01-01", "end_date": "2024-12-31"})
    _use(spark, seeded["edge"][0])
    spark.conf.set("spark.sql.session.timeZone", tz)
    try:
        ok, msg = parity.compare(parity.actual_rows(_call(spark, case), case), case)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert ok, msg


# ---------------------------------------------------------------- name filter
def test_pathological_pattern_does_not_backtrack(spark, seeded):
    _use(spark, seeded["edge"][0])
    for arg in ("%" * 5000 + "ana", "%a" * 50 + "%q", "_%" * 49 + "q"):
        start = time.monotonic()
        spark.sql(ga.call_balance_per_customer_sql(None), args={"customer_name": arg}).collect()
        assert time.monotonic() - start < 20


def test_bracket_classes_are_literal_documented_divergence(spark, seeded):
    _use(spark, seeded["edge"][0])
    lit = spark.sql(ga.call_balance_per_customer_sql(None), args={"customer_name": "[VIP]"}).collect()
    assert [r.customer_name for r in lit] == ["[VIP] TONO"]  # T-SQL would treat [VIP] as one of V/I/P
