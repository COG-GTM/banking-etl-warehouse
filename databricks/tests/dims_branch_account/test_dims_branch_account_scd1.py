"""SCD-1 MERGE behaviour of gold.dim_branch / gold.dim_account across reruns."""

from datetime import date, datetime
from decimal import Decimal

from banking_etl.dims import account, branch

from t5_dims_helpers import (
    fresh_layers,
    parity_account_rows,
    parity_branch_rows,
    seed_bronze,
    source_account_rows,
    source_branch_rows,
    table_rows,
)


def test_rerun_is_idempotent(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    branch.run(spark, layers)
    account.run(spark, layers)
    b = branch.run(spark, layers)["gold_merge"]
    a = account.run(spark, layers)["gold_merge"]
    for metrics, n in ((b, 5), (a, 21)):
        assert metrics.get("numTargetRowsInserted", 0) == 0
        assert metrics.get("numTargetRowsUpdated", 0) == 0
        assert metrics["gold_rows"] == n
    assert table_rows(spark, layers.table("gold", "dim_branch"), "branch_id") == parity_branch_rows()
    assert table_rows(spark, layers.table("gold", "dim_account"), "account_id") == parity_account_rows()


def test_branch_upsert_updates_in_place_inserts_new_and_keeps_missing(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    branch.run(spark, layers)

    changed = [r for r in source_branch_rows() if r[0] != 5]
    changed[0] = (1, "KC Jakarta Pusat", changed[0][2])
    changed.append((6, "KC Bandung", "Jl. Asia Afrika No 8"))
    seed_bronze(spark, layers, branch_rows=changed)
    metrics = branch.run(spark, layers)["gold_merge"]

    assert metrics["numTargetRowsUpdated"] == 1
    assert metrics["numTargetRowsInserted"] == 1
    assert metrics.get("numTargetRowsDeleted", 0) == 0
    rows = dict((r[0], r) for r in table_rows(spark, layers.table("gold", "dim_branch"), "branch_id"))
    assert len(rows) == 6
    assert rows[1][1] == "KC Jakarta Pusat"
    assert rows[6] == (6, "KC Bandung", "Jl. Asia Afrika No 8")
    assert rows[5] == parity_branch_rows()[4]


def test_account_upsert_tracks_balance_status_and_nulls(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    account.run(spark, layers)

    src = source_account_rows()
    src[0] = src[0][:3] + (1750000,) + src[0][4:]
    src[1] = src[1][:5] + ("terminated",)
    src[2] = src[2][:5] + (None,)
    src.append((22, 21, "saving", 100, datetime(2021, 1, 2, 3, 4, 5), "active"))
    seed_bronze(spark, layers, account_rows=src)
    metrics = account.run(spark, layers)["gold_merge"]

    assert metrics["numTargetRowsUpdated"] == 3
    assert metrics["numTargetRowsInserted"] == 1
    rows = dict((r[0], r) for r in table_rows(spark, layers.table("gold", "dim_account"), "account_id"))
    assert rows[1][3] == Decimal("1750000.0000")
    assert rows[2][5] == "terminated"
    assert rows[3][5] is None
    assert rows[22] == (22, 21, "saving", Decimal("100.0000"), date(2021, 1, 2), "active")
    untouched = parity_account_rows()[3:]
    assert [rows[r[0]] for r in untouched] == untouched


def test_gold_only_step_uses_existing_silver(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    branch.run(spark, layers, steps=["silver"])
    assert not spark.catalog.tableExists(layers.table("gold", "dim_branch"))
    result = branch.run(spark, layers, steps=["gold"])
    assert "silver_rows" not in result
    assert result["gold_merge"]["gold_rows"] == 5
