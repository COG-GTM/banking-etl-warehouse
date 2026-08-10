"""Unit tests for the gold fact build (`databricks/gold/build_fact_transaction.py`).

The notebook is a Databricks notebook-source `.py` file; it is loaded here by path so the tests do
not depend on the `databricks/` tree being an importable package.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

NOTEBOOK_PATH = Path(__file__).resolve().parents[2] / "databricks" / "gold" / "build_fact_transaction.py"


def _load_notebook():
    spec = importlib.util.spec_from_file_location("build_fact_transaction", NOTEBOOK_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gold = _load_notebook()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_fact_transaction")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# --- fixtures mirroring the shapes of the three bronze tables -------------------------------------

BRONZE_SQL_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType()),
        StructField("account_id", IntegerType()),
        StructField("transaction_date", TimestampType()),
        StructField("amount", IntegerType()),  # T-SQL source column is INT / MONEY
        StructField("transaction_type", StringType()),
        StructField("branch_id", IntegerType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

# tFileInputDelimited read the raw CSV as text and applied a date pattern; bronze keeps it as text.
BRONZE_CSV_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("account_id", StringType()),
        StructField("transaction_date", StringType()),
        StructField("amount", StringType()),
        StructField("transaction_type", StringType()),
        StructField("branch_id", StringType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

# The Excel reader yields POI-typed cells: numerics as double/long, dates as real timestamps.
BRONZE_EXCEL_SCHEMA = StructType(
    [
        StructField("transaction_id", LongType()),
        StructField("account_id", DoubleType()),
        StructField("transaction_date", TimestampType()),
        StructField("amount", DoubleType()),
        StructField("transaction_type", StringType()),
        StructField("branch_id", DoubleType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

INGESTED = dt.datetime(2024, 2, 1, 0, 0, 0)


def _sql_df(spark, rows=None):
    rows = rows if rows is not None else [
        (1, 1, dt.datetime(2024, 1, 18, 13, 10), 50000, "Withdrawal", 1, INGESTED),
        (2, 2, dt.datetime(2024, 1, 19, 14, 0), 100000, "Payment", 1, INGESTED),
    ]
    return spark.createDataFrame(rows, BRONZE_SQL_SCHEMA)


def _csv_df(spark, rows=None):
    rows = rows if rows is not None else [
        ("14", "13", "21-01-2024 14:00:00", "1500000", "Deposit", "4", INGESTED),
        ("15", "14", "21-01-2024 08:00:00", "500000", "Transfer", "3", INGESTED),
    ]
    return spark.createDataFrame(rows, BRONZE_CSV_SCHEMA)


def _excel_df(spark, rows=None):
    rows = rows if rows is not None else [
        (11, 10.0, dt.datetime(2024, 1, 20, 15, 0), 1000000.0, "Transfer", 1.0, INGESTED),
        (12, 11.0, dt.datetime(2024, 1, 20, 10, 0), 500000.0, "Deposit", 1.0, INGESTED),
    ]
    return spark.createDataFrame(rows, BRONZE_EXCEL_SCHEMA)


def _dim_account(spark, ids=(1, 2, 10, 11, 13, 14)):
    return spark.createDataFrame(
        [(i,) for i in ids], StructType([StructField("account_id", IntegerType())])
    )


def _dim_branch(spark, ids=(1, 3, 4, 5)):
    return spark.createDataFrame(
        [(i,) for i in ids], StructType([StructField("branch_id", IntegerType())])
    )


# --- per-source normalization ---------------------------------------------------------------------


def _assert_common_schema(df):
    fields = {f.name: f.dataType for f in df.schema.fields}
    assert fields["transaction_id"] == IntegerType()
    assert fields["account_id"] == IntegerType()
    assert fields["transaction_date"] == TimestampType()
    assert fields["amount"] == DecimalType(19, 4)
    assert fields["transaction_type"] == StringType()
    assert fields["branch_id"] == IntegerType()


def test_normalize_sql_source(spark):
    out = gold.normalize_sql_source(_sql_df(spark))
    _assert_common_schema(out)
    row = out.filter("transaction_id = 1").collect()[0]
    assert row["amount"] == Decimal("50000.0000")
    assert row["transaction_date"] == dt.datetime(2024, 1, 18, 13, 10)
    assert row["_source_system"] == "sql_server"
    assert row["_source_priority"] == 1


def test_normalize_csv_source_parses_legacy_date_pattern(spark):
    out = gold.normalize_csv_source(_csv_df(spark))
    _assert_common_schema(out)
    row = out.filter("transaction_id = 14").collect()[0]
    # 21-01-2024 is dd-MM-yyyy: January 21st, not "the 1st of month 21".
    assert row["transaction_date"] == dt.datetime(2024, 1, 21, 14, 0)
    assert row["amount"] == Decimal("1500000.0000")
    assert row["account_id"] == 13
    assert row["_source_system"] == "csv"
    assert row["_source_priority"] == 3


def test_normalize_excel_source_downcasts_poi_numerics(spark):
    out = gold.normalize_excel_source(_excel_df(spark))
    _assert_common_schema(out)
    row = out.filter("transaction_id = 11").collect()[0]
    assert row["account_id"] == 10
    assert row["branch_id"] == 1
    assert row["amount"] == Decimal("1000000.0000")
    assert row["transaction_date"] == dt.datetime(2024, 1, 20, 15, 0)
    assert row["_source_priority"] == 2


# --- union across heterogeneous schemas -------------------------------------------------------------


def test_union_sources_combines_three_heterogeneous_sources(spark):
    unioned = gold.union_sources(
        [
            gold.normalize_sql_source(_sql_df(spark)),
            gold.normalize_excel_source(_excel_df(spark)),
            gold.normalize_csv_source(_csv_df(spark)),
        ]
    )
    _assert_common_schema(unioned)
    assert unioned.count() == 6
    assert sorted(r["transaction_id"] for r in unioned.collect()) == [1, 2, 11, 12, 14, 15]
    assert {r["_source_system"] for r in unioned.collect()} == {"sql_server", "excel", "csv"}


def test_union_sources_rejects_empty_input():
    with pytest.raises(ValueError):
        gold.union_sources([])


# --- deterministic dedup ------------------------------------------------------------------------


def test_dedup_prefers_source_priority_on_conflicting_duplicate(spark):
    """Same transaction_id in all three sources with different amounts -> SQL Server wins."""
    sql = _sql_df(spark, [(99, 1, dt.datetime(2024, 1, 18, 9, 0), 111, "Deposit", 1, INGESTED)])
    excel = _excel_df(spark, [(99, 1.0, dt.datetime(2024, 1, 18, 9, 0), 222.0, "Deposit", 1.0, INGESTED)])
    csv = _csv_df(spark, [("99", "1", "18-01-2024 09:00:00", "333", "Deposit", "1", INGESTED)])

    unioned = gold.union_sources(
        [
            gold.normalize_sql_source(sql),
            gold.normalize_excel_source(excel),
            gold.normalize_csv_source(csv),
        ]
    )
    stats = gold.duplicate_stats(unioned)
    assert stats == {
        "input_rows": 3,
        "distinct_ids": 1,
        "duplicate_rows_dropped": 2,
        "duplicated_ids": 1,
        "conflicting_ids": 1,
    }

    deduped = gold.dedup_transactions(unioned)
    assert deduped.count() == 1
    winner = deduped.collect()[0]
    assert winner["_source_system"] == "sql_server"
    assert winner["amount"] == Decimal("111.0000")


def test_dedup_within_one_source_prefers_newest_ingested_at(spark):
    older = dt.datetime(2024, 2, 1, 0, 0, 0)
    newer = dt.datetime(2024, 2, 2, 0, 0, 0)
    csv = _csv_df(
        spark,
        [
            ("50", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", older),
            ("50", "1", "18-01-2024 09:00:00", "900", "Deposit", "1", newer),
        ],
    )
    deduped = gold.dedup_transactions(gold.normalize_csv_source(csv))
    assert deduped.count() == 1
    assert deduped.collect()[0]["amount"] == Decimal("900.0000")


def test_dedup_is_deterministic_across_input_orderings(spark):
    """Reordering / repartitioning the input must not change the surviving row."""
    rows = [
        ("60", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),
        ("60", "1", "19-01-2024 09:00:00", "200", "Deposit", "1", INGESTED),
        ("60", "1", "17-01-2024 09:00:00", "300", "Deposit", "1", INGESTED),
    ]
    first = gold.dedup_transactions(gold.normalize_csv_source(_csv_df(spark, rows))).collect()
    reversed_rows = list(reversed(rows))
    second = gold.dedup_transactions(
        gold.normalize_csv_source(_csv_df(spark, reversed_rows).repartition(3))
    ).collect()
    assert len(first) == len(second) == 1
    assert first[0]["amount"] == second[0]["amount"] == Decimal("200.0000")


def test_duplicate_stats_ignores_identical_duplicates(spark):
    csv = _csv_df(
        spark,
        [
            ("70", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),
            ("70", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),
        ],
    )
    stats = gold.duplicate_stats(gold.normalize_csv_source(csv))
    assert stats["duplicate_rows_dropped"] == 1
    assert stats["duplicated_ids"] == 1
    assert stats["conflicting_ids"] == 0


# --- referential integrity / quarantine -----------------------------------------------------------


def test_referential_integrity_quarantines_orphans(spark):
    csv = _csv_df(
        spark,
        [
            ("80", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),  # ok
            ("81", "999", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),  # bad account
            ("82", "1", "18-01-2024 09:00:00", "100", "Deposit", "888", INGESTED),  # bad branch
            ("83", "999", "18-01-2024 09:00:00", "100", "Deposit", "888", INGESTED),  # both bad
            ("84", None, "18-01-2024 09:00:00", "100", "Deposit", None, INGESTED),  # nulls are ok
        ],
    )
    split = gold.split_referential_integrity(
        gold.normalize_csv_source(csv), _dim_account(spark), _dim_branch(spark)
    )
    assert sorted(r["transaction_id"] for r in split["valid"].collect()) == [80, 84]

    rejects = {r["transaction_id"]: r["reject_reason"] for r in split["rejects"].collect()}
    assert set(rejects) == {81, 82, 83}
    assert rejects[81] == f"account_id not found in {gold.DIM_ACCOUNT_TABLE}"
    assert rejects[82] == f"branch_id not found in {gold.DIM_BRANCH_TABLE}"
    assert rejects[83] == (
        f"account_id not found in {gold.DIM_ACCOUNT_TABLE}; "
        f"branch_id not found in {gold.DIM_BRANCH_TABLE}"
    )
    assert "_source_system" in split["rejects"].columns


def test_referential_integrity_does_not_fan_out_on_duplicate_dim_rows(spark):
    csv = _csv_df(spark, [("90", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED)])
    dim_account = _dim_account(spark, ids=(1, 1, 1))
    split = gold.split_referential_integrity(
        gold.normalize_csv_source(csv), dim_account, _dim_branch(spark)
    )
    assert split["valid"].count() == 1
    assert split["rejects"].count() == 0


# --- end-to-end pipeline ---------------------------------------------------------------------------


def test_build_fact_transaction_end_to_end(spark):
    sql = _sql_df(spark)
    excel = _excel_df(
        spark,
        [
            (11, 10.0, dt.datetime(2024, 1, 20, 15, 0), 1000000.0, "Transfer", 1.0, INGESTED),
            # duplicate of the SQL row with a conflicting amount -> SQL must win
            (1, 1.0, dt.datetime(2024, 1, 18, 13, 10), 999.0, "Withdrawal", 1.0, INGESTED),
        ],
    )
    csv = _csv_df(
        spark,
        [
            ("14", "13", "21-01-2024 14:00:00", "1500000", "Deposit", "4", INGESTED),
            ("25", "23", "22-01-2024 14:30:00", "400000", "Deposit", "15", INGESTED),  # bad branch
        ],
    )

    result = gold.build_fact_transaction(sql, csv, excel, _dim_account(spark), _dim_branch(spark))
    stats = result["stats"]
    assert stats["input_rows"] == 6
    assert stats["distinct_ids"] == 5
    assert stats["duplicate_rows_dropped"] == 1
    assert stats["conflicting_ids"] == 1
    assert stats["rows_missing_transaction_id"] == 0
    assert stats["valid_rows"] == 4
    assert stats["reject_rows"] == 1

    valid = {r["transaction_id"]: r for r in result["valid"].collect()}
    assert sorted(valid) == [1, 2, 11, 14]
    assert valid[1]["amount"] == Decimal("50000.0000")  # SQL row beat the Excel duplicate
    assert result["rejects"].collect()[0]["transaction_id"] == 25

    final = gold.to_target_schema(result["valid"])
    assert final.columns == gold.FACT_COLUMNS
    assert [f.dataType for f in final.schema.fields] == [
        f.dataType for f in gold.FACT_SCHEMA.fields
    ]


def test_build_drops_rows_without_transaction_id(spark):
    csv = _csv_df(
        spark,
        [
            (None, "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),
            ("95", "1", "18-01-2024 09:00:00", "100", "Deposit", "1", INGESTED),
        ],
    )
    result = gold.build_fact_transaction(
        _sql_df(spark, []), csv, _excel_df(spark, []), _dim_account(spark), _dim_branch(spark)
    )
    assert result["stats"]["rows_missing_transaction_id"] == 1
    assert result["stats"]["valid_rows"] == 1
