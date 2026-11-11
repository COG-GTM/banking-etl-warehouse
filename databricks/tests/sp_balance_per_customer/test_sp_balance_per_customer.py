"""``sp_BalancePerCustomer`` port vs. results captured from the real T-SQL procedure.

``fixtures/expected.json`` was produced by ``generate_expected.py``, which runs the repo's
unchanged ``sql_scripts/0{1,2}_*.sql`` on SQL Server 2022 (collation
SQL_Latin1_General_CP1_CI_AS) against ``fixtures/fixture.json``.
"""

from pathlib import Path

import pytest
import yaml
from pyspark.sql.types import DecimalType, StringType

from banking_etl import sp_balance_per_customer as bpc

from conftest import EXPECTED, FIXTURE, normalize

SEARCH_IDS = [s["id"] for s in FIXTURE["searches"]]
BUNDLE_ROOT = Path(__file__).resolve().parents[2]


def _search(search_id):
    return next(s["customer_name"] for s in FIXTURE["searches"] if s["id"] == search_id)


def _expected(search_id):
    return EXPECTED["results"][search_id]["rows"]


def test_oracle_was_generated_with_sql_server_default_collation():
    assert EXPECTED["database_collation"] == "SQL_Latin1_General_CP1_CI_AS"
    assert set(EXPECTED["results"]) == set(SEARCH_IDS)
    for search_id in SEARCH_IDS:
        assert EXPECTED["results"][search_id]["customer_name"] == _search(search_id)


@pytest.fixture(scope="module")
def table_function(spark, gold_schema):
    """Persistent function in the session catalog.

    Open-source Spark's parser rejects schema-qualified table-valued function calls
    (``INVALID_TABLE_VALUED_FUNC_NAME``), which Databricks accepts, so locally the
    function is created qualified but called unqualified with ``gold_schema`` as the
    current schema.
    """
    bpc.register_functions(spark, catalog=None, gold_schema=gold_schema)
    previous = spark.catalog.currentDatabase()
    spark.catalog.setCurrentDatabase(gold_schema)
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {bpc.FUNCTION_NAME}")
    yield lambda name: spark.sql(f"SELECT * FROM {bpc.FUNCTION_NAME}(:customer_name)",
                                 args={"customer_name": name})
    spark.catalog.setCurrentDatabase(previous)


@pytest.mark.parametrize("search_id", SEARCH_IDS)
def test_pyspark_function_matches_tsql(spark, gold_schema, search_id):
    df = bpc.balance_per_customer(spark, _search(search_id), catalog=None, gold_schema=gold_schema)
    assert normalize(df) == _expected(search_id)


@pytest.mark.parametrize("search_id", SEARCH_IDS)
def test_sql_table_function_matches_tsql(table_function, search_id):
    df = table_function(_search(search_id))
    assert normalize(df) == _expected(search_id)


@pytest.mark.parametrize("search_id", ["substring_lower", "unterminated_bracket_never_matches",
                                       "backslash_is_literal", "empty_string_matches_all_named"])
def test_temporary_sql_function_matches_tsql(spark, gold_schema, search_id):
    bpc.register_functions(spark, catalog=None, gold_schema=gold_schema, temporary=True)
    try:
        df = bpc.call_table_function(spark, _search(search_id), catalog=None,
                                     gold_schema=gold_schema, temporary=True)
        assert normalize(df) == _expected(search_id)
    finally:
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {bpc.FUNCTION_NAME}")
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {bpc.LIKE_HELPER_NAME}")


def test_scenarios_cover_the_required_semantics():
    rows = {r["account_type"] + "/" + r["customer_name"]: r
            for r in _expected("empty_string_matches_all_named")}
    # Account 102 ('Active', no transactions) keeps its balance via ISNULL(..., 0).
    assert rows["Checking/Shelly Juwita"]["current_balance"] == "500.0000"
    # Account 107 has only a NULL-amount transaction: SUM is NULL -> ISNULL -> 0.
    assert rows["Checking/budi rahardjo"]["current_balance"] == "0.0000"
    # 'ACTIVE ' qualifies; 'deposit' / 'DEPOSIT  ' add; ' Deposit' (leading space) subtracts.
    assert rows["Checking/SHELLY Anggraini"]["current_balance"] == "322.5000"
    # NULL balance stays NULL (Balance + x is NULL in T-SQL too).
    assert rows["Deposito/SHELLY Anggraini"]["current_balance"] is None
    # ' active', NULL, 'inactive', 'closed' are excluded; NULL names never match '%%'.
    assert {r["customer_name"] for r in _expected("empty_string_matches_all_named")} >= {"Siti Café"}
    assert len(_expected("empty_string_matches_all_named")) == 11
    assert _expected("mixed_case_two_customers") and _expected("full_name_upper")
    assert _expected("null_matches_nothing") == []


def test_output_schema(spark, gold_schema, table_function):
    for df in (bpc.balance_per_customer(spark, "x", catalog=None, gold_schema=gold_schema),
               table_function("x")):
        assert tuple(df.columns) == bpc.OUTPUT_COLUMNS
        types = [f.dataType for f in df.schema.fields]
        assert types == [StringType(), StringType(), DecimalType(19, 4), DecimalType(19, 4)]


LIKE_PATTERNS = [
    None, "", "%", "%%", "abc", "%a_c%", "[abc]", "[a-c]", "[A-C]", "[Z-a]", "[z-a]", "[az-a]",
    "[a-c-e]", "[--]", "[-a]", "[a-]", "[^]", "[^^]", "[a^]", "[^z-a]", "[]", "[]a]", "[",
    "a[b", "]", "a]b", "[[]", "[%]", "[_]", "a\\b", "x.y*+?(){}|$^", "a&b", "[&\\]", "café",
    "line\nbreak", "  spaced  ",
] + [bpc.customer_name_pattern(s["customer_name"]) for s in FIXTURE["searches"]]


def test_sql_like_helper_equals_python_translation(spark, gold_schema, table_function):
    helper = f"`{gold_schema}`.`{bpc.LIKE_HELPER_NAME}`"
    df = spark.createDataFrame([(p,) for p in LIKE_PATTERNS], "pattern STRING")
    df.createOrReplaceTempView("like_patterns")
    got = {r.pattern: r.regex for r in
           spark.sql(f"SELECT pattern, {helper}(pattern) AS regex FROM like_patterns").collect()}
    for pattern in LIKE_PATTERNS:
        assert got[pattern] == bpc.tsql_like_to_regex(pattern), pattern


@pytest.mark.parametrize("pattern,value,matches", [
    ("%[%", "abc[", False),          # unterminated bracket never matches
    ("%[]%", "abc", False),          # empty class never matches
    ("[^]", "x", True),              # negated empty class: any single character
    ("[z-a]", "m", False),           # reversed range is empty
    ("[az-a]", "A", True),
    ("[a-c-e]", "-", True),
    ("[a-c-e]", "d", False),
    ("[A-C]", "b", True),            # case-insensitive ranges
    ("%\\b%", "a\\b", True),         # no default escape character
    ("%b %", "ab ", True),
    ("%b %", "ab", False),           # trailing pattern space is significant for LIKE
    ("abc", "abc\n", False),         # anchored at the very end, not before a newline
    ("%", "multi\nline", True),
    ("CAFÉ", "café", True),
    ("cafe", "café", False),         # accent-sensitive
])
def test_like_semantics_match_sql_server(spark, pattern, value, matches):
    regex = bpc.tsql_like_to_regex(pattern)
    got = spark.sql("SELECT :value RLIKE :regex AS m", args={"value": value, "regex": regex}).first().m
    assert got is matches


def test_customer_name_is_truncated_like_varchar_100():
    long_name = "x" * 150
    assert bpc.customer_name_pattern(long_name) == "%" + "x" * 100 + "%"
    assert bpc.customer_name_pattern(None) is None


def test_ddl_uses_configured_catalog_and_schema():
    ddl = bpc.table_function_ddl("my_cat", "my_gold")
    assert "`my_cat`.`my_gold`.`sp_balance_per_customer`" in ddl
    for table in ("dim_customer", "dim_account", "fact_transaction"):
        assert f"`my_cat`.`my_gold`.`{table}`" in ddl
    assert "`my_cat`.`my_gold`.`tsql_like_to_regex`(" in ddl
    assert "banking_dwh" not in ddl and "`gold`" not in ddl
    assert "`my_cat`.`my_gold`.`tsql_like_to_regex`" in bpc.like_helper_ddl("my_cat", "my_gold")


@pytest.mark.parametrize("bad", ["", "a.b", "a`b", "x; DROP TABLE y", "a b"])
def test_rejects_unsafe_identifiers(bad):
    with pytest.raises(ValueError):
        bpc.table_function_ddl(bad, "gold")
    with pytest.raises(ValueError):
        bpc.table_function_ddl("cat", bad)


def test_job_resource_definition():
    resource = BUNDLE_ROOT / "resources" / "sp_balance_per_customer.yml"
    job = yaml.safe_load(resource.read_text())["resources"]["jobs"]["sp_balance_per_customer"]
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params == {"catalog": "${var.catalog}", "gold_schema": "gold", "customer_name": ""}
    (task,) = job["tasks"]
    notebook = (resource.parent / task["notebook_task"]["notebook_path"]).resolve()
    assert notebook.is_file()
    assert notebook.read_text().startswith("# Databricks notebook source")
    assert "job_clusters" not in job and "new_cluster" not in task  # serverless
