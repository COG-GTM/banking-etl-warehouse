import json
import shutil

import pytest
from pyspark.sql import functions as F

from banking_etl.validation import cutover, fixtures
from banking_etl.validation.tables import Schemas


@pytest.fixture()
def names(request):
    return Schemas(catalog=None, work_suffix=f"t10_{request.node.name[-12:].strip('_')}")


def test_dry_run_cutover_green(spark, names):
    rep = cutover.dry_run_cutover(spark, names, cutover_id="green")
    assert rep.succeeded, rep.summary()
    assert rep.consumers_on == "gold"
    steps = [(s["step"], s["status"]) for s in rep.steps]
    assert steps == [
        ("seed_legacy_and_sources", "OK"), ("consumers_on_legacy", "OK"), ("historical_load", "OK"),
        ("freeze_legacy", "OK"), ("final_incremental", "OK"), ("reconcile_gate", "GREEN"),
        ("switch_consumers", "OK"), ("rollback_rehearsal", "OK"), ("talend_decommission", "SKIPPED_DRY_RUN"),
    ]
    incr = json.loads(rep.steps[4]["details"])
    assert incr["rows_in_increment"] > 0 and incr["fact_rows_after"] == 22

    results = spark.table(names.ops_table(cutover.RESULTS_TABLE)).filter(F.col("run_id") == rep.reconciliation.run_id)
    assert results.filter("record_type = 'check'").count() == len(rep.reconciliation.checks)
    assert results.filter("status != 'PASS'").count() == 0
    log = spark.table(names.ops_table(cutover.CUTOVER_LOG_TABLE)).filter("cutover_id = 'green'")
    assert log.count() == 9
    assert spark.table(names.work_table("consumer_fact_transaction")).count() == 22


def test_red_gate_rolls_back_and_blocks_decommission(spark, names):
    def corrupt(spark, names):
        spark.sql(f"DELETE FROM {names.work_table('fact_transaction')} WHERE transaction_id = 1")

    rep = cutover.dry_run_cutover(spark, names, cutover_id="red", before_gate=corrupt)
    assert not rep.succeeded
    assert rep.consumers_on == "legacy"
    steps = dict((s["step"], s["status"]) for s in rep.steps)
    assert steps["reconcile_gate"] == "RED"
    assert steps["rollback"] == "ROLLED_BACK"
    assert steps["talend_decommission"] == "BLOCKED"
    assert "switch_consumers" not in steps
    failed = spark.table(names.ops_table(cutover.RESULTS_TABLE)).filter(
        (F.col("run_id") == rep.reconciliation.run_id) & (F.col("status") == "FAIL"))
    assert failed.filter("record_type = 'mismatch' AND pk_value = '1'").count() >= 1


def _steps(rep):
    return {s["step"]: s["status"] for s in rep.steps}


def test_late_arrival_is_merged_by_key(spark, names):
    cutover.ensure_schemas(spark, names.work, names.ops)
    cutover.seed(spark, names, fixtures.DEFAULT_FIXTURES)
    cutover.historical_load(spark, names, cutover.DEFAULT_HISTORICAL_CUTOFF)
    fact = names.work_table("fact_transaction")
    oldest = spark.table(fact).orderBy("transaction_date").first()["transaction_id"]
    spark.sql(f"DELETE FROM {fact} WHERE transaction_id = {oldest}")  # older business date, id not yet in Delta
    out = cutover.final_incremental(spark, names)
    assert out["late_arrivals"] == 1
    assert out["fact_rows_after"] == 22
    assert cutover.final_incremental(spark, names)["rows_in_increment"] == 0  # idempotent


def test_smoke_drift_after_switch_rolls_back(spark, names, monkeypatch):
    real = cutover._consumer_smoke

    def drifting(spark, names):
        out = real(spark, names)
        out["fact_transaction"]["consumer"] -= 1
        return out

    monkeypatch.setattr(cutover, "_consumer_smoke", drifting)
    rep = cutover.dry_run_cutover(spark, names, cutover_id="smoke")
    steps = _steps(rep)
    assert steps["reconcile_gate"] == "GREEN"
    assert steps["switch_consumers"] == "FAILED"
    assert steps["rollback"] == "ROLLED_BACK" and steps["talend_decommission"] == "BLOCKED"
    assert rep.consumers_on == "legacy" and not rep.succeeded
    assert cutover.consumers_target(spark, names) == "legacy"


def test_error_mid_switch_restores_all_views_and_writes_log(spark, names, monkeypatch):
    real = cutover.point_consumers
    calls = {"n": 0}

    def flaky(spark, names, at):
        if at == "gold":
            calls["n"] += 1
            spark.sql(f"CREATE OR REPLACE VIEW {names.work_table('consumer_dim_branch')} "
                      f"COMMENT 'cutover consumer view -> gold' AS SELECT * FROM {names.work_table('dim_branch')}")
            raise RuntimeError("view swap failed")
        real(spark, names, at)

    monkeypatch.setattr(cutover, "point_consumers", flaky)
    rep = cutover.dry_run_cutover(spark, names, cutover_id="boom")
    steps = _steps(rep)
    assert steps["switch_consumers"] == "ERROR"
    assert steps["rollback"] == "ROLLED_BACK" and steps["talend_decommission"] == "BLOCKED"
    for t in ("dim_branch", "fact_transaction"):
        comment = spark.sql(f"DESCRIBE TABLE EXTENDED {names.work_table('consumer_' + t)}") \
            .filter("col_name = 'Comment'").first()["data_type"]
        assert comment.endswith("-> legacy"), t
    log = spark.table(names.ops_table(cutover.CUTOVER_LOG_TABLE)).filter("cutover_id = 'boom'")
    assert {r.step: r.status for r in log.collect()}["switch_consumers"] == "ERROR"


def test_tampered_baseline_is_rejected(spark, names, tmp_path):
    base = tmp_path / "legacy_dwh"
    shutil.copytree(fixtures.DEFAULT_FIXTURES, base)
    with open(base / "dwh" / "FactTransaction.csv", "a") as f:
        f.write("999,1,2024-01-01 00:00:00.000,1.0000,Deposit,1\n")
    with pytest.raises(fixtures.FixtureIntegrityError, match="FactTransaction"):
        cutover.dry_run_cutover(spark, names, str(base), cutover_id="tamper")


def test_refuses_shared_or_unsafe_schema_names():
    with pytest.raises(ValueError):
        Schemas(catalog=None, work_suffix="t10; DROP SCHEMA x")
    with pytest.raises(ValueError, match="shared schema"):
        cutover.dry_run_cutover(None, Schemas(catalog=None, work_suffix="gold"))
