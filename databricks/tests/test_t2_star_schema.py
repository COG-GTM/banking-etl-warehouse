import json
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts import ddl
from banking_dwh.contracts.tables import (
    ALL_TABLES, BRONZE_TABLES, DIM_ACCOUNT, DIM_BRANCH, DIM_CUSTOMER, FACT_TRANSACTION, SILVER_TABLES,
)
from banking_dwh.dq.checks import (
    DataQualityError, assert_all_passed, referential_integrity_checks, run_star_schema_checks, write_results,
)

ROOT = Path(__file__).resolve().parents[1]
LEGACY_DDL = ROOT.parent / "sql_scripts" / "01_create_tables.sql"
TYPE_MAP = {"INT": "INT", "MONEY": "DECIMAL(19,4)", "DATETIME": "TIMESTAMP", "DATE": "DATE"}


def _legacy_columns():
    """Parse sql_scripts/01_create_tables.sql -> {table: [(col, type)]}."""
    text = LEGACY_DDL.read_text()
    out = {}
    for name, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", text, re.S):
        cols = []
        for line in body.splitlines():
            m = re.match(r"\s+(\w+) (\w+)", line)
            if m and m.group(1) not in ("CONSTRAINT",):
                cols.append((m.group(1), m.group(2)))
        out[name] = cols
    return out


@pytest.mark.parametrize("contract", SILVER_TABLES, ids=lambda t: t.name)
def test_silver_contract_matches_legacy_ddl_with_type_mapping(contract):
    legacy = _legacy_columns()[contract.legacy_name]
    mapped = [(c.legacy_name, c.type) for c in contract.columns if c.legacy_name]
    expected = [(name, TYPE_MAP.get(t, "STRING" if t == "VARCHAR" else t)) for name, t in legacy]
    assert mapped == expected


def test_primary_and_foreign_keys_match_legacy():
    assert DIM_ACCOUNT.primary_key == ("account_id",)
    assert DIM_BRANCH.primary_key == ("branch_id",)
    assert DIM_CUSTOMER.primary_key == ("customer_id",)
    assert FACT_TRANSACTION.primary_key == ("transaction_id",)
    fks = {fk.name: (fk.columns, fk.ref_table) for fk in FACT_TRANSACTION.foreign_keys}
    assert fks == {
        "FK_FactTransaction_DimAccount": (("account_id",), "dim_account"),
        "FK_FactTransaction_DimBranch": (("branch_id",), "dim_branch"),
    }


def test_uc_ddl_has_informational_constraints():
    cfg = EnvConfig.from_target("prod")
    sql = ddl.create_table_sql(cfg, FACT_TRANSACTION)
    assert "CREATE TABLE IF NOT EXISTS banking_dwh.silver.fact_transaction" in sql
    assert "CONSTRAINT pk_fact_transaction PRIMARY KEY (transaction_id)" in sql
    assert ("CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (account_id) "
            "REFERENCES banking_dwh.silver.dim_account (account_id)") in sql
    assert ("CONSTRAINT FK_FactTransaction_DimBranch FOREIGN KEY (branch_id) "
            "REFERENCES banking_dwh.silver.dim_branch (branch_id)") in sql
    assert "amount DECIMAL(19,4)" in sql and "transaction_date TIMESTAMP" in sql
    assert "CLUSTER BY (transaction_date)" in sql
    local = ddl.create_table_sql(cfg, FACT_TRANSACTION, unity_catalog=False)
    assert "CONSTRAINT" not in local and "CLUSTER BY" not in local


def test_dims_created_before_fact():
    names = [t.name for t in SILVER_TABLES]
    assert names.index("fact_transaction") > max(names.index("dim_account"), names.index("dim_branch"))


@pytest.mark.parametrize("what", ["star_schema", "bronze"])
def test_committed_ddl_scripts_are_in_sync(what, capsys):
    ddl.main(["--target", "prod", "--what", what])
    rendered = capsys.readouterr().out
    path = ROOT / "ddl" / ("02_star_schema.sql" if what == "star_schema" else "03_bronze_tables.sql")
    assert path.read_text() == rendered, f"Regenerate {path.name} with python -m banking_dwh.contracts.ddl"


def test_contract_names_are_unique():
    keys = [(t.layer, t.name) for t in ALL_TABLES]
    assert len(keys) == len(set(keys))
    assert {t.name for t in BRONZE_TABLES} >= {"file_transaction_csv", "file_transaction_excel", "sqlserver_transaction_db"}


# ---------------------------------------------------------------- local Delta tests
@pytest.fixture()
def star(spark, local_schemas):
    cfg = local_schemas
    for t in reversed(SILVER_TABLES):
        spark.sql(f"DROP TABLE IF EXISTS {cfg.table(t.layer, t.name)}")
    spark.sql(f"DROP TABLE IF EXISTS {cfg.table('ops', 'dq_results')}")
    for s in ddl.star_schema_statements(cfg, unity_catalog=False):
        spark.sql(s)
    return cfg


def _seed(spark, cfg, fact_rows, dup_branch=False):
    now = datetime(2026, 1, 1)
    branches = [(1, "KCP A", "Jakarta", now), (2, "KCP B", "Bandung", now)]
    if dup_branch:
        branches.append((2, "KCP B dup", "Bandung", now))
    spark.createDataFrame(branches, DIM_BRANCH.ddl_schema()).write.format("delta").mode("append").saveAsTable(cfg.table("silver", "dim_branch"))
    spark.createDataFrame(
        [(10, 100, "Savings", Decimal("1000.0000"), date(2023, 1, 1), "active", now)], DIM_ACCOUNT.ddl_schema()
    ).write.format("delta").mode("append").saveAsTable(cfg.table("silver", "dim_account"))
    spark.createDataFrame(
        [(100, "BUDI", "JL. A", "JAKARTA", "DKI", 30, "MALE", "b@x.id", now)], DIM_CUSTOMER.ddl_schema()
    ).write.format("delta").mode("append").saveAsTable(cfg.table("silver", "dim_customer"))
    spark.createDataFrame(fact_rows, FACT_TRANSACTION.ddl_schema()).write.format("delta").mode("append").saveAsTable(
        cfg.table("silver", "fact_transaction"))


def _fact(tid, account, branch):
    return (tid, account, datetime(2024, 1, 18, 13, 10), Decimal("50000.0000"), "Deposit", branch, "csv", datetime(2026, 1, 1))


def test_local_tables_have_contract_schema(spark, star):
    for t in SILVER_TABLES:
        schema = spark.table(star.table(t.layer, t.name)).schema
        assert [f.name for f in schema] == t.column_names
        assert [f.dataType.simpleString() for f in schema] == [c.type.lower() for c in t.columns]


def test_clean_star_schema_passes_all_checks(spark, star):
    _seed(spark, star, [_fact(1, 10, 1), _fact(2, None, 2)])  # NULL FK passes (SQL Server semantics)
    results = run_star_schema_checks(spark, star)
    assert all(r.passed for r in results), [r for r in results if not r.passed]
    assert {r.check_name for r in results} >= {"FK_FactTransaction_DimAccount", "FK_FactTransaction_DimBranch",
                                               "pk_unique.fact_transaction", "pk_not_null.dim_customer"}
    assert_all_passed(results)


def test_orphan_fact_rows_fail_referential_integrity(spark, star):
    _seed(spark, star, [_fact(1, 10, 1), _fact(2, 99, 1), _fact(3, 10, 7), _fact(4, 98, 8)])
    by_name = {r.check_name: r for r in referential_integrity_checks(spark, star)}
    acc, br = by_name["FK_FactTransaction_DimAccount"], by_name["FK_FactTransaction_DimBranch"]
    assert (acc.passed, acc.failed_rows) == (False, 2)
    assert (br.passed, br.failed_rows) == (False, 2)
    assert sorted(s["transaction_id"] for s in json.loads(acc.details)["sample"]) == [2, 4]
    with pytest.raises(DataQualityError, match="FK_FactTransaction_DimAccount"):
        assert_all_passed(list(by_name.values()))


def test_delta_does_not_enforce_keys_so_dq_catches_duplicates(spark, star):
    _seed(spark, star, [_fact(1, 10, 1), _fact(1, 10, 1), _fact(1, 10, 1)], dup_branch=True)
    by_name = {r.check_name: r for r in run_star_schema_checks(spark, star)}
    # failed_rows counts every row that shares a duplicated key, not the number of keys.
    assert by_name["pk_unique.fact_transaction"].failed_rows == 3
    assert by_name["pk_unique.dim_branch"].failed_rows == 2
    assert not by_name["pk_unique.dim_account"].failed_rows


def test_results_are_written_to_ops_table(spark, star):
    _seed(spark, star, [_fact(1, 10, 1)])
    results = run_star_schema_checks(spark, star)
    assert write_results(spark, star, results, "run-1") == len(results)
    df = spark.table(star.table("ops", "dq_results"))
    assert df.filter("run_id = 'run-1'").count() == len(results)
