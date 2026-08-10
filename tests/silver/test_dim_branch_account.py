"""Local unit tests for the ticket-6 silver dimension builds.

The notebooks are Databricks notebook source files (`.py` with `# COMMAND ----------`
separators), which is still valid Python, so they are loaded straight from disk.
The notebook bodies guard their entrypoint on `dbutils` being defined, so importing
them here only defines the pure functions.
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
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = REPO_ROOT / "databricks" / "silver"


def _load_notebook(name: str):
    spec = importlib.util.spec_from_file_location(name, SILVER_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


branch_nb = _load_notebook("build_dim_branch")
account_nb = _load_notebook("build_dim_account")

def _fields_of(schema):
    return [(f.name, f.dataType) for f in schema.fields]


def _fields(df):
    return _fields_of(df.schema)


TS = dt.datetime(2024, 1, 1, 12, 0, 0)
LATER_TS = dt.datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ticket6-silver-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# --------------------------------------------------------------------------- branch


BRONZE_BRANCH_SCHEMA = StructType(
    [
        StructField("branch_id", StringType(), True),
        StructField("branch_name", StringType(), True),
        StructField("branch_location", StringType(), True),
        StructField("_loaded_at", TimestampType(), True),
        StructField("_source_system", StringType(), True),
    ]
)


def bronze_branch(spark, rows):
    return spark.createDataFrame(rows, BRONZE_BRANCH_SCHEMA)


def test_branch_casts_to_target_schema(spark):
    df = branch_nb.transform_branch(
        bronze_branch(spark, [("1", "Central", "Jakarta", TS, "sample_db")])
    )
    assert _fields(df) == _fields_of(branch_nb.TARGET_SCHEMA)
    row = df.collect()[0]
    assert (row.branch_id, row.branch_name, row.branch_location) == (1, "Central", "Jakarta")
    assert row._loaded_at == TS
    assert row._source_system == "sample_db"


def test_branch_derives_audit_columns_when_bronze_lacks_them(spark):
    bronze = spark.createDataFrame(
        [(7, "West", "Bandung")],
        StructType(
            [
                StructField("branch_id", IntegerType(), True),
                StructField("branch_name", StringType(), True),
                StructField("branch_location", StringType(), True),
            ]
        ),
    )
    row = branch_nb.transform_branch(bronze, source_system="unit_test").collect()[0]
    assert row._source_system == "unit_test"
    assert row._loaded_at is not None


def test_branch_dedup_keeps_latest_loaded_row(spark):
    df = branch_nb.transform_branch(
        bronze_branch(
            spark,
            [
                ("1", "Central", "Jakarta", TS, "sample_db"),
                ("1", "Central Renamed", "Jakarta", LATER_TS, "sample_db"),
                ("2", "East", "Surabaya", TS, "sample_db"),
            ],
        )
    )
    result = {r.branch_id: r.branch_name for r in branch_nb.deduplicate(df).collect()}
    assert result == {1: "Central Renamed", 2: "East"}


def test_branch_dq_rejects_null_business_key(spark):
    df = branch_nb.transform_branch(
        bronze_branch(spark, [(None, "Ghost", "Nowhere", TS, "sample_db")])
    )
    with pytest.raises(branch_nb.DataQualityError, match="NULL business key"):
        branch_nb.run_data_quality_checks(df, bronze_count=1)


def test_branch_dq_rejects_duplicate_business_key(spark):
    df = branch_nb.transform_branch(
        bronze_branch(
            spark,
            [
                ("1", "Central", "Jakarta", TS, "sample_db"),
                ("1", "Central", "Bogor", TS, "sample_db"),
            ],
        )
    )
    with pytest.raises(branch_nb.DataQualityError, match="not unique"):
        branch_nb.run_data_quality_checks(df, bronze_count=2)


def test_branch_dq_rejects_row_count_drop(spark):
    df = branch_nb.deduplicate(
        branch_nb.transform_branch(bronze_branch(spark, [("1", "Central", "Jakarta", TS, "s")]))
    )
    with pytest.raises(branch_nb.DataQualityError, match="below 90% of the bronze count"):
        branch_nb.run_data_quality_checks(df, bronze_count=100)


def test_branch_dq_passes_on_clean_data(spark):
    df = branch_nb.deduplicate(
        branch_nb.transform_branch(
            bronze_branch(
                spark,
                [
                    ("1", "Central", "Jakarta", TS, "sample_db"),
                    ("2", "East", "Surabaya", TS, "sample_db"),
                ],
            )
        )
    )
    branch_nb.run_data_quality_checks(df, bronze_count=2)


# -------------------------------------------------------------------------- account


BRONZE_ACCOUNT_SCHEMA = StructType(
    [
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("balance", StringType(), True),
        StructField("date_opened", StringType(), True),
        StructField("status", StringType(), True),
        StructField("_loaded_at", TimestampType(), True),
        StructField("_source_system", StringType(), True),
    ]
)


def bronze_account(spark, rows):
    return spark.createDataFrame(rows, BRONZE_ACCOUNT_SCHEMA)


def test_account_casts_money_and_date(spark):
    df = account_nb.transform_account(
        bronze_account(
            spark,
            [("10", "5", "SAVINGS", "1234.5", "31-12-2021", "Active", TS, "sample_db")],
        )
    )
    assert _fields(df) == _fields_of(account_nb.TARGET_SCHEMA)
    row = df.collect()[0]
    assert row.account_id == 10
    assert row.customer_id == 5
    assert row.balance == Decimal("1234.5000")
    assert row.date_opened == dt.date(2021, 12, 31)
    assert row.status == "Active"


def test_account_casts_timestamp_date_opened_to_date(spark):
    bronze = spark.createDataFrame(
        [(10, 5, "CHECKING", Decimal("10.00"), dt.datetime(2020, 3, 4, 22, 30), "Active")],
        StructType(
            [
                StructField("account_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("account_type", StringType(), True),
                StructField("balance", DecimalType(18, 2), True),
                StructField("date_opened", TimestampType(), True),
                StructField("status", StringType(), True),
            ]
        ),
    )
    row = account_nb.transform_account(bronze).collect()[0]
    assert row.date_opened == dt.date(2020, 3, 4)
    assert row.balance == Decimal("10.0000")


def test_account_dedup_keeps_latest_loaded_row(spark):
    df = account_nb.transform_account(
        bronze_account(
            spark,
            [
                ("10", "5", "SAVINGS", "100", "01-01-2020", "Active", TS, "sample_db"),
                ("10", "5", "SAVINGS", "250", "01-01-2020", "Closed", LATER_TS, "sample_db"),
                ("11", "6", "CHECKING", "1", "02-01-2020", "Active", TS, "sample_db"),
            ],
        )
    )
    rows = {r.account_id: (r.balance, r.status) for r in account_nb.deduplicate(df).collect()}
    assert rows == {10: (Decimal("250.0000"), "Closed"), 11: (Decimal("1.0000"), "Active")}


def test_account_dq_rejects_null_business_key(spark):
    df = account_nb.transform_account(
        bronze_account(
            spark, [(None, "5", "SAVINGS", "100", "01-01-2020", "Active", TS, "sample_db")]
        )
    )
    with pytest.raises(account_nb.DataQualityError, match="NULL business key"):
        account_nb.run_data_quality_checks(df, bronze_count=1)


def test_account_dq_rejects_duplicate_business_key(spark):
    df = account_nb.transform_account(
        bronze_account(
            spark,
            [
                ("10", "5", "SAVINGS", "100", "01-01-2020", "Active", TS, "sample_db"),
                ("10", "5", "SAVINGS", "250", "01-01-2020", "Closed", TS, "sample_db"),
            ],
        )
    )
    with pytest.raises(account_nb.DataQualityError, match="not unique"):
        account_nb.run_data_quality_checks(df, bronze_count=2)


def test_account_dq_rejects_empty_projection_of_non_empty_bronze(spark):
    empty = account_nb.transform_account(bronze_account(spark, []))
    with pytest.raises(account_nb.DataQualityError, match="silver projection is empty"):
        account_nb.run_data_quality_checks(empty, bronze_count=5)


def test_account_dq_passes_after_dedup(spark):
    df = account_nb.deduplicate(
        account_nb.transform_account(
            bronze_account(
                spark,
                [
                    ("10", "5", "SAVINGS", "100", "01-01-2020", "Active", TS, "sample_db"),
                    ("10", "5", "SAVINGS", "250", "01-01-2020", "Closed", LATER_TS, "sample_db"),
                ],
            )
        )
    )
    # `bronze_count` is the distinct-key count in bronze, so the dedup from two rows
    # to one does not trip the row-count gate.
    account_nb.run_data_quality_checks(df, bronze_count=1)


def test_target_columns_are_snake_case_and_typed():
    assert account_nb.TARGET_COLUMNS == [
        "account_id",
        "customer_id",
        "account_type",
        "balance",
        "date_opened",
        "status",
        "_loaded_at",
        "_source_system",
    ]
    assert branch_nb.TARGET_COLUMNS == [
        "branch_id",
        "branch_name",
        "branch_location",
        "_loaded_at",
        "_source_system",
    ]
    assert account_nb.TARGET_SCHEMA["balance"].dataType == DecimalType(19, 4)
    assert account_nb.TARGET_SCHEMA["date_opened"].dataType == DateType()
