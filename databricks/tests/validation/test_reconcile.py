from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from banking_etl.validation import fixtures, procs
from banking_etl.validation.reconcile import RESULT_SCHEMA, reconcile


def _failed(rep, check_type, table=None):
    return [r for r in rep.failed if r["check_type"] == check_type and (table is None or r["table_name"] == table)]


@pytest.fixture(scope="module")
def green_report(spark, built, legacy):
    gold, rejects = built
    return reconcile(spark, legacy, gold, run_label="test-green",
                     legacy_procs=fixtures.load_legacy_procs(spark), target_procs=procs.run_all(gold),
                     legacy_rejects=fixtures.load_talend_rejects(spark), target_rejects=rejects)


def test_reference_gold_is_100_percent_match(green_report):
    assert green_report.green, green_report.failed
    assert green_report.match_pct == 100.0
    assert green_report.mismatches == []
    types = {r["check_type"] for r in green_report.checks}
    assert types == {"schema", "row_count", "pk_unique", "fk_orphans", "column_checksum", "row_checksum",
                     "aggregate", "row_diff", "proc_parity", "reject_parity"}


def test_required_aggregates_present(green_report):
    names = {(r["table_name"], r["check_name"]) for r in green_report.checks if r["check_type"] == "aggregate"}
    assert ("fact_transaction", "sum_amount_by_type") in names
    assert {("fact_transaction", "min_transaction_date"), ("fact_transaction", "max_transaction_date"),
            ("dim_account", "min_date_opened"), ("dim_account", "max_date_opened")} <= names


def test_report_rows_fit_result_schema(spark, green_report):
    df = green_report.to_df(spark)
    assert df.schema == RESULT_SCHEMA
    assert df.count() == len(green_report.rows)


def test_changed_amount_detected_with_row_level_mismatch(spark, built, legacy):
    gold = dict(built[0])
    gold["fact_transaction"] = gold["fact_transaction"].withColumn(
        "amount", F.when(F.col("transaction_id") == 1, F.col("amount") + F.lit(Decimal("0.0100"))).otherwise(F.col("amount"))
    )
    rep = reconcile(spark, legacy, gold, run_label="test-amount")
    assert not rep.green
    assert {r["check_name"] for r in _failed(rep, "column_checksum", "fact_transaction")} == {"amount"}
    assert {r["check_name"] for r in _failed(rep, "aggregate", "fact_transaction")} >= {"sum_amount_by_type"}
    diffs = [m for m in rep.mismatches if m["table_name"] == "fact_transaction" and m["check_type"] == "value_mismatch"]
    assert [(m["pk_value"], m["column_name"]) for m in diffs] == [("1", "amount")]
    assert Decimal(diffs[0]["target_value"]) - Decimal(diffs[0]["legacy_value"]) == Decimal("0.01")


def test_missing_and_extra_rows(spark, built, legacy):
    gold = dict(built[0])
    gold["dim_branch"] = gold["dim_branch"].filter("branch_id != 5")
    rep = reconcile(spark, legacy, gold, run_label="test-missing")
    assert _failed(rep, "row_count", "dim_branch")
    diffs = [(m["check_type"], m["pk_value"]) for m in rep.mismatches if m["table_name"] == "dim_branch"]
    assert diffs == [("missing_in_target", "5")]


def test_duplicate_pk_detected(spark, built, legacy):
    gold = dict(built[0])
    gold["dim_account"] = gold["dim_account"].unionByName(gold["dim_account"].filter("account_id = 1"))
    rep = reconcile(spark, legacy, gold, run_label="test-dupe")
    assert [r["check_name"] for r in _failed(rep, "pk_unique", "dim_account")] == ["pk_unique_target"]
    assert any(m["check_type"] == "pk_duplicate_target" and m["pk_value"] == "1" for m in rep.mismatches)


def test_fk_orphans_detected(spark, built, legacy):
    gold = dict(built[0])
    gold["fact_transaction"] = gold["fact_transaction"].withColumn(
        "branch_id", F.when(F.col("transaction_id") == 2, F.lit(999)).otherwise(F.col("branch_id")))
    rep = reconcile(spark, legacy, gold, run_label="test-fk")
    assert [r["check_name"] for r in _failed(rep, "fk_orphans")] == ["branch_id->dim_branch.branch_id"]
    assert any(m["check_type"] == "fk_orphan" and m["pk_value"] == "2" for m in rep.mismatches)


def test_schema_drift_and_missing_table(spark, built, legacy):
    gold = dict(built[0])
    gold["dim_branch"] = gold["dim_branch"].drop("branch_location")
    del gold["dim_customer"]
    rep = reconcile(spark, legacy, gold, run_label="test-schema")
    assert {r["table_name"] for r in _failed(rep, "schema")} == {"dim_branch", "dim_customer"}


def test_proc_and_reject_parity_failures(spark, built, legacy):
    gold, rejects = built
    gold = dict(gold)
    gold["fact_transaction"] = gold["fact_transaction"].filter("transaction_id != 3")
    rep = reconcile(spark, legacy, gold, run_label="test-proc",
                    legacy_procs=fixtures.load_legacy_procs(spark), target_procs=procs.run_all(gold),
                    legacy_rejects=fixtures.load_talend_rejects(spark), target_rejects=rejects.limit(2))
    assert _failed(rep, "proc_parity")
    assert _failed(rep, "reject_parity")
