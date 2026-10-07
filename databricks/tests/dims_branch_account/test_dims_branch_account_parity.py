"""Exact parity of gold.dim_branch / gold.dim_account vs the legacy SQL Server DWH.

Baselines in databricks/fixtures/parity/ were produced by restoring sample.bak in
SQL Server 2022, running sql_scripts/01_create_tables.sql and the Talend-equivalent
INSERT ... SELECT of Load_DimBranch / Load_DimAccount.
"""

from pyspark.sql import types as T

from banking_etl.dims import account, branch

from t5_dims_helpers import (
    fresh_layers,
    parity_account_rows,
    parity_branch_rows,
    seed_bronze,
    table_rows,
)


def test_parity_baselines_have_expected_shape():
    assert len(parity_branch_rows()) == 5
    assert len(parity_account_rows()) == 21


def test_dim_branch_exact_parity(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    result = branch.run(spark, layers)
    assert result["silver_rows"] == 5
    assert result["gold_merge"]["numTargetRowsInserted"] == 5
    assert result["gold_merge"]["gold_rows"] == 5
    assert table_rows(spark, layers.table("gold", "dim_branch"), "branch_id") == parity_branch_rows()


def test_dim_account_exact_parity(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    branch.run(spark, layers)
    result = account.run(spark, layers)
    assert result["silver_rows"] == 21
    assert result["gold_merge"]["numTargetRowsInserted"] == 21
    assert result["gold_merge"]["gold_rows"] == 21
    assert (
        table_rows(spark, layers.table("gold", "dim_account"), "account_id")
        == parity_account_rows()
    )


def test_dim_account_aggregate_parity(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    account.run(spark, layers)
    row = spark.sql(
        f"SELECT COUNT(*) n, SUM(balance) s, MIN(date_opened) lo, MAX(date_opened) hi "
        f"FROM {layers.table('gold', 'dim_account')}"
    ).collect()[0]
    expected = parity_account_rows()
    assert row["n"] == len(expected)
    assert row["s"] == sum(r[3] for r in expected)
    assert row["lo"] == min(r[4] for r in expected)
    assert row["hi"] == max(r[4] for r in expected)


def test_gold_schemas_match_legacy_types(spark):
    layers = fresh_layers(spark)
    seed_bronze(spark, layers)
    branch.run(spark, layers)
    account.run(spark, layers)
    b = spark.table(layers.table("gold", "dim_branch")).schema
    a = spark.table(layers.table("gold", "dim_account")).schema
    assert [(f.name, f.dataType) for f in b] == [
        ("branch_id", T.IntegerType()),
        ("branch_name", T.StringType()),
        ("branch_location", T.StringType()),
    ]
    assert [(f.name, f.dataType) for f in a] == [
        ("account_id", T.IntegerType()),
        ("customer_id", T.IntegerType()),
        ("account_type", T.StringType()),
        ("balance", T.DecimalType(19, 4)),
        ("date_opened", T.DateType()),
        ("status", T.StringType()),
    ]
    assert not b["branch_id"].nullable
    assert not a["account_id"].nullable
