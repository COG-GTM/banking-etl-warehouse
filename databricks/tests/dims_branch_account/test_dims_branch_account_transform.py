from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import types as T

from banking_etl.dims.account import transform_silver_account
from banking_etl.dims.branch import resolve_layers, transform_silver_branch

STR = T.StringType()


def _df(spark, rows, cols, types=None):
    types = types or [STR] * len(cols)
    schema = T.StructType([T.StructField(c, t) for c, t in zip(cols, types)])
    return spark.createDataFrame(rows, schema)


def test_resolve_layers_defaults_and_overrides():
    layers = resolve_layers()
    assert layers.table("bronze", "sqlserver_branch") == "migration_demo.banking_mig_bronze.sqlserver_branch"
    assert layers.table("silver", "branch") == "migration_demo.banking_mig_silver.branch"
    assert layers.table("gold", "dim_account") == "migration_demo.banking_mig_gold.dim_account"
    scoped = resolve_layers(bronze_schema="banking_mig_t5", silver_schema="banking_mig_t5", gold_schema="banking_mig_t5")
    assert scoped.gold == "migration_demo.banking_mig_t5"
    assert resolve_layers(catalog=None, schema_prefix="x_").silver == "x_silver"


def test_branch_typed_trimmed_and_null_key_dropped(spark):
    bronze = _df(
        spark,
        [("1", "  KC Jakarta ", " Jl. A "), (None, "orphan", "x"), ("abc", "bad key", "y")],
        ["branch_id", "branch_name", "branch_location"],
    )
    out = transform_silver_branch(bronze)
    assert out.columns == ["branch_id", "branch_name", "branch_location"]
    assert dict((f.name, f.dataType) for f in out.schema)["branch_id"] == T.IntegerType()
    assert [tuple(r) for r in out.collect()] == [(1, "KC Jakarta", "Jl. A")]


def test_branch_dedupe_prefers_latest_ingestion(spark):
    bronze = _df(
        spark,
        [
            (1, "old", "loc", datetime(2024, 1, 1)),
            (1, "new", "loc", datetime(2024, 2, 1)),
            (2, "only", "loc", datetime(2024, 1, 1)),
        ],
        ["branch_id", "branch_name", "branch_location", "_ingested_at"],
        [T.IntegerType(), STR, STR, T.TimestampType()],
    )
    rows = sorted(tuple(r) for r in transform_silver_branch(bronze).collect())
    assert rows == [(1, "new", "loc"), (2, "only", "loc")]


def test_branch_dedupe_is_deterministic_without_metadata(spark):
    rows = [(7, "b", "x"), (7, "a", "x")]
    bronze = _df(spark, rows, ["branch_id", "branch_name", "branch_location"], [T.IntegerType(), STR, STR])
    first = [tuple(r) for r in transform_silver_branch(bronze).collect()]
    second = [tuple(r) for r in transform_silver_branch(_df(spark, rows[::-1], ["branch_id", "branch_name", "branch_location"], [T.IntegerType(), STR, STR])).collect()]
    assert first == second == [(7, "a", "x")]


def test_account_types_money_and_date(spark):
    bronze = _df(
        spark,
        [(3, 1, " checking ", 25000000, datetime(2020, 6, 21, 9, 0), "active ")],
        ["account_id", "customer_id", "account_type", "balance", "date_opened", "status"],
        [T.IntegerType(), T.IntegerType(), STR, T.IntegerType(), T.TimestampType(), STR],
    )
    out = transform_silver_account(bronze)
    types = {f.name: f.dataType for f in out.schema}
    assert types["balance"] == T.DecimalType(19, 4)
    assert types["date_opened"] == T.DateType()
    assert [tuple(r) for r in out.collect()] == [
        (3, 1, "checking", Decimal("25000000.0000"), date(2020, 6, 21), "active")
    ]


def test_account_accepts_string_sources_and_nulls_bad_values(spark):
    bronze = _df(
        spark,
        [
            ("5", "4", "saving", "75000000", "2020-06-29 13:00:00", "active"),
            ("6", "x", "saving", "n/a", "not a date", "active"),
            ("", "1", "saving", "1", "2020-01-01", "active"),
        ],
        ["account_id", "customer_id", "account_type", "balance", "date_opened", "status"],
    )
    rows = sorted(tuple(r) for r in transform_silver_account(bronze).collect())
    assert rows == [
        (5, 4, "saving", Decimal("75000000.0000"), date(2020, 6, 29), "active"),
        (6, None, "saving", None, None, "active"),
    ]
