import itertools
from datetime import date
from decimal import Decimal

import pytest
from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql import functions as F

from banking_etl.gold.ddl import apply_star_schema, ddl_parameters
from banking_etl.gold.merge import merge_scd1

_ids = itertools.count()


@pytest.fixture
def target(spark):
    name = f"ops.scd1_target_{next(_ids)}"
    # OSS Delta only supports identity columns through DeltaTableBuilder (not SQL DDL).
    (
        DeltaTable.create(spark).tableName(name)
        .addColumn("ItemKey", "BIGINT", generatedByDefaultAs=IdentityGenerator())
        .addColumn("ItemID", "INT", nullable=False)
        .addColumn("Region", "STRING", nullable=False)
        .addColumn("Name", "STRING")
        .addColumn("Amount", "DECIMAL(19,4)")
        .addColumn("Opened", "DATE")
        .execute()
    )
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def src(spark, rows):
    return spark.createDataFrame(rows, "ItemID INT, Region STRING, Name STRING, Amount DOUBLE, Opened DATE")


def snapshot(spark, table):
    return {(r.ItemID, r.Region): r.asDict() for r in spark.table(table).collect()}


KEYS = ["ItemID", "Region"]
D = date(2020, 1, 1)


def test_insert_into_empty_generates_identity(spark, target):
    m = merge_scd1(spark, src(spark, [(1, "A", "one", 1.5, D), (2, "A", "two", None, None)]), target, KEYS)
    assert (m["numTargetRowsInserted"], m["numTargetRowsUpdated"], m["numTargetRowsDeleted"]) == (2, 0, 0)
    snap = snapshot(spark, target)
    assert snap[(1, "A")]["Amount"] == Decimal("1.5000") and snap[(1, "A")]["Opened"] == D
    keys = [r["ItemKey"] for r in snap.values()]
    assert None not in keys and len(set(keys)) == 2


def test_update_changed_insert_new_never_delete_keys_stable(spark, target):
    merge_scd1(spark, src(spark, [(1, "A", "one", 1.0, D), (2, "A", "two", 2.0, D), (3, "B", "three", 3.0, D)]), target, KEYS)
    before = snapshot(spark, target)

    m = merge_scd1(spark, src(spark, [(1, "A", "ONE", 1.0, D), (2, "A", "two", 2.0, D), (4, "B", "four", 4.0, D)]), target, KEYS)
    assert (m["numTargetRowsInserted"], m["numTargetRowsUpdated"], m["numTargetRowsDeleted"]) == (1, 1, 0)

    after = snapshot(spark, target)
    assert set(after) == {(1, "A"), (2, "A"), (3, "B"), (4, "B")}  # (3, B) absent from source but kept
    assert after[(1, "A")]["Name"] == "ONE"
    for k in before:
        assert after[k]["ItemKey"] == before[k]["ItemKey"]
    assert after[(4, "B")]["ItemKey"] not in {r["ItemKey"] for r in before.values()}


def test_rerun_same_source_is_noop(spark, target):
    rows = [(1, "A", "one", 1.0, D), (2, "A", None, None, None)]
    merge_scd1(spark, src(spark, rows), target, KEYS)
    before = snapshot(spark, target)
    m = merge_scd1(spark, src(spark, rows), target, KEYS)
    assert (m["numTargetRowsInserted"], m["numTargetRowsUpdated"]) == (0, 0)
    assert snapshot(spark, target) == before


def test_null_safe_change_detection(spark, target):
    merge_scd1(spark, src(spark, [(1, "A", None, 1.0, D), (2, "A", "two", 2.0, D), (3, "A", None, None, None)]), target, KEYS)
    m = merge_scd1(spark, src(spark, [(1, "A", "one", 1.0, D), (2, "A", None, 2.0, D), (3, "A", None, None, None)]), target, KEYS)
    assert m["numTargetRowsUpdated"] == 2  # NULL->value and value->NULL; NULL==NULL untouched
    snap = snapshot(spark, target)
    assert snap[(1, "A")]["Name"] == "one" and snap[(2, "A")]["Name"] is None


def test_update_cols_subset_leaves_other_columns(spark, target):
    merge_scd1(spark, src(spark, [(1, "A", "one", 1.0, D)]), target, KEYS)
    m = merge_scd1(spark, src(spark, [(1, "A", "changed", 9.0, D)]), target, KEYS, update_cols=["Amount"])
    assert m["numTargetRowsUpdated"] == 1
    row = snapshot(spark, target)[(1, "A")]
    assert (row["Name"], row["Amount"]) == ("one", Decimal("9.0000"))


def test_source_extra_columns_ignored_with_explicit_update_cols(spark, target):
    df = src(spark, [(1, "A", "one", 1.0, D)]).withColumn("_ingested_at", F.current_timestamp())
    merge_scd1(spark, df, target, KEYS, update_cols=["Name", "Amount", "Opened"])
    assert snapshot(spark, target)[(1, "A")]["Name"] == "one"


@pytest.mark.parametrize(
    "rows, kwargs, message",
    [
        ([(1, "A", "x", 1.0, D), (1, "A", "y", 1.0, D)], {}, "duplicate"),
        ([(None, "A", "x", 1.0, D)], {}, "NULL"),
        ([(1, "A", "x", 1.0, D)], {"update_cols": ["ItemKey"]}, "missing from source_df"),
        ([(1, "A", "x", 1.0, D)], {"update_cols": ["ItemID"]}, "both key and update"),
        ([(1, "A", "x", 1.0, D)], {"key_cols": []}, "must not be empty"),
    ],
)
def test_invalid_inputs_raise(spark, target, rows, kwargs, message):
    kwargs = {"key_cols": KEYS, **kwargs}
    with pytest.raises(ValueError, match=message):
        merge_scd1(spark, src(spark, rows), target, **kwargs)
    assert spark.table(target).count() == 0


def test_identity_column_cannot_be_written(spark, target):
    df = src(spark, [(1, "A", "x", 1.0, D)]).selectExpr("*", "CAST(42 AS BIGINT) AS ItemKey")
    with pytest.raises(ValueError, match="identity"):
        merge_scd1(spark, df, target, KEYS)


def test_merge_into_gold_dim_branch(spark, settings):
    apply_star_schema(spark, settings)
    dim = ddl_parameters(settings)["dim_branch"]
    df = spark.createDataFrame([(1, "KC JAKARTA", "Jl. Gatot Subroto No 13"), (2, "KC BOGOR", "Jl. Padjajaran No 43")],
                               "BranchID INT, BranchName STRING, BranchLocation STRING")
    try:
        merge_scd1(spark, df, dim, ["BranchID"])
        keys = {r.BranchID: r.BranchKey for r in spark.table(dim).collect()}
        m = merge_scd1(spark, df.filter("BranchID = 2").selectExpr("BranchID", "'KC BOGOR 2' AS BranchName", "BranchLocation"), dim, ["BranchID"])
        assert m["numTargetRowsUpdated"] == 1
        rows = {r.BranchID: r for r in spark.table(dim).collect()}
        assert {k: r.BranchKey for k, r in rows.items()} == keys
        assert rows[2].BranchName == "KC BOGOR 2"
        changes = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).table(dim)
        assert changes.filter("_change_type = 'update_postimage'").count() == 1
    finally:
        spark.sql(f"DELETE FROM {dim}")


def test_identity_regex_matches_databricks_show_create_table():
    from banking_etl.gold.merge import _IDENTITY_DDL

    ddl = """CREATE TABLE c.s.dim_branch (
  BranchKey BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'x',
  `Odd Key` BIGINT GENERATED ALWAYS AS IDENTITY,
  BranchID INT NOT NULL COMMENT 'GENERATED BY DEFAULT AS IDENTITY in a comment',
  CONSTRAINT `pk_dim_branch` PRIMARY KEY (`BranchID`))"""
    assert [m.group(1) or m.group(2) for m in _IDENTITY_DDL.finditer(ddl)] == ["BranchKey", "Odd Key"]
