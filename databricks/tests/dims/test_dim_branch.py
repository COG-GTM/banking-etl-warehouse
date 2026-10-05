import csv
from dataclasses import replace

import pytest
from pyspark.sql.types import IntegerType, StringType, TimestampType

from banking_etl.bronze.fixtures import stage_fixtures
from banking_etl.bronze.sqlserver import ingest_table
from banking_etl.config import Settings
from banking_etl.dims.branch import (
    TMAP_TO_DIM_BRANCH,
    dim_branch_table,
    load_dim_branch,
    load_silver_branch,
    silver_branch_table,
    to_dim_branch,
    to_silver_branch,
)
from banking_etl.gold.merge import identity_columns

from conftest import FIXTURES

# Isolated schemas so these tests never share gold.dim_branch with tests/gold.
PREFIX = "t5_"
DIM_COLS = ["BranchID", "BranchName", "BranchLocation"]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return [(int(r["BranchID"]), r["BranchName"], r["BranchLocation"]) for r in csv.DictReader(fh)]


# DWH.dbo.DimBranch after the Talend-equivalent INSERT on SQL Server 2022 (sample.bak restored).
SQLSERVER_DIM_BRANCH = read_csv(FIXTURES / "parity" / "dim_branch.csv")


@pytest.fixture(scope="module")
def t5(spark, tmp_path_factory):
    settings = Settings(catalog=None, schema_prefix=PREFIX)
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.schema(layer)}")
    root = tmp_path_factory.mktemp("landing_t5") / "sample_db"
    stage_fixtures(root, ["branch"])
    ingest_table(spark, settings, "branch", fixture_root=str(root))
    yield settings
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"DROP DATABASE IF EXISTS {settings.schema(layer)} CASCADE")


@pytest.fixture
def fresh_dim(spark, t5):
    spark.sql(f"DROP TABLE IF EXISTS {dim_branch_table(t5)}")
    load_silver_branch(spark, t5)
    return t5


def dim_rows(spark, settings):
    return {r.BranchID: r.asDict() for r in spark.table(dim_branch_table(settings)).collect()}


def overwrite_silver(spark, settings, rows):
    df = spark.createDataFrame(rows, "branch_id INT, branch_name STRING, branch_location STRING")
    to_silver_branch(df).write.format("delta").mode("overwrite").saveAsTable(silver_branch_table(settings))


# --------------------------------------------------------------------------- silver


def test_to_silver_branch_types_and_passthrough(spark):
    bronze = spark.createDataFrame(
        [(1, "  KC Jakarta ", "jl. lower", "fixture:x"), (2, None, None, "fixture:x")],
        "branch_id INT, branch_name STRING, branch_location STRING, _source STRING",
    )
    out = to_silver_branch(bronze)
    assert [(f.name, f.dataType) for f in out.schema.fields] == [
        ("branch_id", IntegerType()),
        ("branch_name", StringType()),
        ("branch_location", StringType()),
        ("_ingested_at", TimestampType()),
        ("_source", StringType()),
    ]
    rows = {r.branch_id: r for r in out.collect()}
    # Talend tMap: no trim / case change (TRIM_ALL_COLUMN=false, expressions are row1.<col>).
    assert rows[1].branch_name == "  KC Jakarta " and rows[1].branch_location == "jl. lower"
    assert rows[2].branch_name is None and rows[2]._ingested_at is None


def test_load_silver_branch_from_bronze_snapshot(spark, t5):
    result = load_silver_branch(spark, t5)
    assert result == {"source": "t5_bronze.sqlserver_branch", "target": "t5_silver.branch", "rows": 5}
    silver = spark.table(silver_branch_table(t5))
    assert silver.columns == ["branch_id", "branch_name", "branch_location", "_ingested_at", "_source"]
    assert silver.filter("_ingested_at IS NULL OR _source NOT LIKE 'fixture:%'").count() == 0
    assert load_silver_branch(spark, t5)["rows"] == 5  # overwrite, not append


def test_tmap_mapping_matches_talend_item():
    assert TMAP_TO_DIM_BRANCH == {
        "BranchID": "branch_id", "BranchName": "branch_name", "BranchLocation": "branch_location",
    }


def test_to_dim_branch_renames_only(spark):
    silver = to_silver_branch(spark.createDataFrame(
        [(7, "KC X", "Jl. Y")], "branch_id INT, branch_name STRING, branch_location STRING"))
    out = to_dim_branch(silver)
    assert out.columns == DIM_COLS
    assert [tuple(r) for r in out.collect()] == [(7, "KC X", "Jl. Y")]


# --------------------------------------------------------------------------- gold


def test_initial_load_creates_table_and_inserts_5_rows(spark, fresh_dim):
    m = load_dim_branch(spark, fresh_dim)
    assert m["target"] == "t5_gold.dim_branch" and m["rows"] == 5
    assert (m["numSourceRows"], m["numTargetRowsInserted"], m["numTargetRowsUpdated"], m["numTargetRowsDeleted"]) == (5, 5, 0, 0)
    assert identity_columns(spark, dim_branch_table(fresh_dim)) == ["BranchKey"]
    keys = [r["BranchKey"] for r in dim_rows(spark, fresh_dim).values()]
    assert None not in keys and len(set(keys)) == 5


def test_parity_with_talend_dwh_dimbranch(spark, fresh_dim):
    load_dim_branch(spark, fresh_dim)
    got = sorted(tuple(r) for r in spark.table(dim_branch_table(fresh_dim)).select(*DIM_COLS).collect())
    assert got == SQLSERVER_DIM_BRANCH
    # Independently: Talend's pass-through tMap of the source export.
    with open(FIXTURES / "sample_db" / "branch.csv", newline="", encoding="utf-8") as fh:
        source = sorted((int(r["branch_id"]), r["branch_name"], r["branch_location"]) for r in csv.DictReader(fh))
    assert got == source


def test_rerun_is_idempotent(spark, fresh_dim):
    load_dim_branch(spark, fresh_dim)
    before = dim_rows(spark, fresh_dim)
    m = load_dim_branch(spark, fresh_dim)
    assert (m["numTargetRowsInserted"], m["numTargetRowsUpdated"], m["numTargetRowsDeleted"]) == (0, 0, 0)
    assert dim_rows(spark, fresh_dim) == before


def test_changed_attribute_updates_in_place_with_stable_key(spark, fresh_dim):
    load_dim_branch(spark, fresh_dim)
    before = dim_rows(spark, fresh_dim)
    rows = [(r["BranchID"], r["BranchName"], r["BranchLocation"]) for r in before.values()]
    rows = [(i, "KC Bekasi" if i == 5 else n, loc) for i, n, loc in rows if i != 4] + [(6, "KC Bandung", "Jl. Asia Afrika No 8")]
    overwrite_silver(spark, fresh_dim, rows)

    m = load_dim_branch(spark, fresh_dim)
    assert (m["numTargetRowsInserted"], m["numTargetRowsUpdated"], m["numTargetRowsDeleted"]) == (1, 1, 0)
    after = dim_rows(spark, fresh_dim)
    assert after[5]["BranchName"] == "KC Bekasi" and after[5]["BranchKey"] == before[5]["BranchKey"]
    assert after[4] == before[4]  # missing from source: kept (no hard deletes)
    assert {k: after[k] for k in (1, 2, 3)} == {k: before[k] for k in (1, 2, 3)}
    assert after[6]["BranchKey"] not in {r["BranchKey"] for r in before.values()}
    assert len(after) == 6


def test_null_attribute_change_is_detected(spark, fresh_dim):
    load_dim_branch(spark, fresh_dim)
    before = dim_rows(spark, fresh_dim)
    rows = [(r["BranchID"], r["BranchName"], None if r["BranchID"] == 1 else r["BranchLocation"]) for r in before.values()]
    overwrite_silver(spark, fresh_dim, rows)
    m = load_dim_branch(spark, fresh_dim)
    assert m["numTargetRowsUpdated"] == 1
    after = dim_rows(spark, fresh_dim)
    assert after[1]["BranchLocation"] is None and after[1]["BranchKey"] == before[1]["BranchKey"]


def test_duplicate_source_keys_fail_loudly(spark, fresh_dim):
    overwrite_silver(spark, fresh_dim, [(1, "A", "x"), (1, "B", "y")])
    with pytest.raises(ValueError, match="duplicate"):
        load_dim_branch(spark, fresh_dim)


def test_names_resolve_through_settings():
    s = Settings(catalog="migration_demo", schema_prefix="banking_etl_")
    assert silver_branch_table(s) == "migration_demo.banking_etl_silver.branch"
    assert dim_branch_table(s) == "migration_demo.banking_etl_gold.dim_branch"
    assert dim_branch_table(replace(s, schema_prefix="")) == "migration_demo.gold.dim_branch"
