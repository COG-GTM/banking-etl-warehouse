from __future__ import annotations

import re
from datetime import datetime, timezone

from bronze.config import BATCH_ID_COLUMN, INGEST_TS_COLUMN, SOURCE_TABLE_COLUMN
from bronze.metadata import new_batch_id, with_table_metadata


def test_batch_id_is_sortable_and_unique():
    fixed = datetime(2024, 3, 1, 12, 30, 45, tzinfo=timezone.utc)

    first = new_batch_id(fixed)
    second = new_batch_id(fixed)

    assert re.fullmatch(r"20240301T123045Z-[0-9a-f]{8}", first)
    assert first != second


def test_with_table_metadata_appends_columns_once(spark):
    df = spark.createDataFrame([("1",)], "branch_id string")

    result = with_table_metadata(df, "batch-1", "dbo.branch")

    assert result.columns == [
        "branch_id",
        INGEST_TS_COLUMN,
        SOURCE_TABLE_COLUMN,
        BATCH_ID_COLUMN,
    ]
    row = result.collect()[0]
    assert row[SOURCE_TABLE_COLUMN] == "dbo.branch"
    assert row[BATCH_ID_COLUMN] == "batch-1"
