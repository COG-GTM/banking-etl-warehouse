from __future__ import annotations

from decimal import Decimal

import pytest
from conftest import bronze_df

from databricks.silver.common import deduplicate_on_key
from databricks.silver.dim_branch import transform_dim_branch
from databricks.silver.publish import assert_unique_key, upsert_dataframes

BRANCH_COLUMNS = ["branch_id", "branch_name", "branch_location"]


def branch(spark, rows):
    return transform_dim_branch(bronze_df(spark, rows, BRANCH_COLUMNS))


def test_merge_is_idempotent(spark):
    source = branch(
        spark,
        [
            {"branch_id": "1", "branch_name": "Pusat", "branch_location": "Jakarta"},
            {"branch_id": "2", "branch_name": "Timur", "branch_location": "Surabaya"},
        ],
    )

    first = upsert_dataframes(source.limit(0), source, "branch_id")
    second = upsert_dataframes(first, source, "branch_id")

    assert first.count() == 2
    assert second.count() == 2
    assert sorted(r.branch_id for r in second.collect()) == [1, 2]


def test_merge_updates_changed_rows_and_inserts_new_ones(spark):
    target = branch(
        spark,
        [
            {"branch_id": "1", "branch_name": "Pusat", "branch_location": "Jakarta"},
            {"branch_id": "2", "branch_name": "Timur", "branch_location": "Surabaya"},
        ],
    )
    incoming = branch(
        spark,
        [
            {"branch_id": "2", "branch_name": "Timur Baru", "branch_location": "Sidoarjo"},
            {"branch_id": "3", "branch_name": "Barat", "branch_location": "Bandung"},
        ],
    )

    merged = upsert_dataframes(target, incoming, "branch_id")

    rows = {r.branch_id: r for r in merged.collect()}
    assert set(rows) == {1, 2, 3}
    assert rows[1].branch_name == "Pusat"
    assert rows[2].branch_name == "Timur Baru"
    assert rows[2].branch_location == "Sidoarjo"
    assert rows[3].branch_name == "Barat"


def test_duplicate_business_keys_are_rejected_before_merge(spark):
    duplicated = branch(
        spark,
        [
            {"branch_id": "1", "branch_name": "Pusat", "branch_location": "Jakarta"},
            {"branch_id": "1", "branch_name": "Pusat Lama", "branch_location": "Jakarta"},
        ],
    )

    with pytest.raises(ValueError, match="duplicate business key"):
        assert_unique_key(duplicated, "branch_id")

    deduplicated = deduplicate_on_key(duplicated, "branch_id")
    assert deduplicated.count() == 1
    assert assert_unique_key(deduplicated, "branch_id").count() == 1


def test_null_business_key_is_rejected(spark):
    with_null = branch(
        spark, [{"branch_id": None, "branch_name": "Pusat", "branch_location": "Jakarta"}]
    )

    with pytest.raises(ValueError, match="null business key"):
        assert_unique_key(with_null, "branch_id")


def test_merge_preserves_decimal_typing_across_reruns(spark):
    from databricks.silver.dim_account import transform_dim_account

    columns = ["account_id", "customer_id", "account_type", "balance", "date_opened", "status"]
    source = transform_dim_account(
        bronze_df(
            spark,
            [
                {
                    "account_id": "1",
                    "customer_id": "9",
                    "account_type": "Savings",
                    "balance": "10.5000",
                    "date_opened": "2021-03-02",
                    "status": "Active",
                }
            ],
            columns,
        )
    )

    merged = upsert_dataframes(upsert_dataframes(source.limit(0), source, "account_id"), source, "account_id")

    assert merged.count() == 1
    assert dict(merged.dtypes)["balance"] == "decimal(19,4)"
    assert merged.collect()[0].balance == Decimal("10.5000")
