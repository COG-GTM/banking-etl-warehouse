from banking_etl.validation import fixtures, procs
from banking_etl.validation.reconcile import reconcile


def test_like_pattern_collapses_wildcards():
    assert procs.like_pattern("%") == "%"
    assert procs.like_pattern("shelly juwita") == "%SHELLY JUWITA%"


def test_proc_outputs_match_sql_server(spark, built, legacy):
    gold, _ = built
    rep = reconcile(spark, legacy, gold, run_label="test-procs",
                    legacy_procs=fixtures.load_legacy_procs(spark), target_procs=procs.run_all(gold),
                    tables=())
    assert rep.checks, "no proc checks ran"
    assert rep.green, rep.failed


def test_balance_case_insensitive_like_sql_server(built):
    gold, _ = built
    lower = procs.balance_per_customer(gold["dim_customer"], gold["dim_account"], gold["fact_transaction"], "shelly")
    upper = procs.balance_per_customer(gold["dim_customer"], gold["dim_account"], gold["fact_transaction"], "SHELLY")
    assert lower.count() > 0
    assert sorted(lower.collect()) == sorted(upper.collect())
