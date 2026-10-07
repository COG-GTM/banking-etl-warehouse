import json
from pathlib import Path

from banking_etl.validation import fixtures, legacy_baseline

FIXTURES = Path(fixtures.DEFAULT_FIXTURES)


def test_split_batches_breaks_before_create_procedure():
    script = "USE DWH;\nGO\nPRINT 'x'\nCREATE PROCEDURE p AS SELECT 1;\nGO\n"
    assert legacy_baseline.split_batches(script) == ["USE DWH;", "PRINT 'x'", "CREATE PROCEDURE p AS SELECT 1;"]


def test_split_batches_repo_procedure_script_has_one_proc_per_batch():
    sql = (legacy_baseline.REPO_ROOT / "sql_scripts" / "02_create_procedures.sql").read_text()
    procs = [b for b in legacy_baseline.split_batches(sql) if "CREATE PROCEDURE" in b.upper()]
    assert len(procs) == 2
    assert all(b.upper().lstrip().startswith("CREATE PROCEDURE") for b in procs)


def test_talend_fact_stream_first_source_wins():
    sql = [(1, 10, "d", 5, "Deposit", 1)]
    excel = [(1, 99, "d", 7, "Withdrawal", 2), (2, 11, "d", 3, "Deposit", 1)]
    csv = [(2, 98, "d", 1, "Deposit", 1), (3, 12, "d", 4, "Transfer", 1)]
    out = legacy_baseline.talend_fact_stream(sql, excel, csv)
    assert [(s, r[0], r[1]) for s, r in out] == [("sqlserver", 1, 10), ("excel", 2, 11), ("csv", 3, 12)]


def test_manifest_matches_fixture_files():
    import hashlib

    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    assert manifest["row_counts"]["dwh/FactTransaction"] == 22
    assert manifest["row_counts"]["talend_rejects"] == 3
    for rel, digest in manifest["sha256"].items():
        assert hashlib.sha256((FIXTURES / rel).read_bytes()).hexdigest() == digest, rel
