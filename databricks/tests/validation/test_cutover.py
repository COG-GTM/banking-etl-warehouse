import json

import pytest
from pyspark.sql import functions as F

from banking_etl.validation import cutover
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
