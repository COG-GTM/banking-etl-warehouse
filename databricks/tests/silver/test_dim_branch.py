from __future__ import annotations

from conftest import bronze_df

from databricks.silver.dim_branch import transform_dim_branch

BRANCH_COLUMNS = ["branch_id", "branch_name", "branch_location"]


def test_branch_is_typed_and_metadata_is_dropped(spark):
    source = bronze_df(
        spark,
        [
            {"branch_id": "1", "branch_name": "Jakarta Pusat", "branch_location": " Jakarta "},
            {"branch_id": " 2 ", "branch_name": None, "branch_location": ""},
        ],
        BRANCH_COLUMNS,
    )

    result = transform_dim_branch(source)

    assert result.columns == BRANCH_COLUMNS
    assert dict(result.dtypes) == {
        "branch_id": "int",
        "branch_name": "string",
        "branch_location": "string",
    }
    rows = {r.branch_id: r for r in result.collect()}
    assert rows[1].branch_name == "Jakarta Pusat"
    assert rows[1].branch_location == "Jakarta"
    assert rows[2].branch_name is None
    assert rows[2].branch_location is None
