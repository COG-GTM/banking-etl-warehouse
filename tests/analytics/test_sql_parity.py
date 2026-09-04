"""The Spark SQL files must return the same rows as the DataFrame functions."""

from __future__ import annotations

from pathlib import Path

import pytest

from databricks.analytics.procedures import balance_per_customer, daily_transaction

SQL_DIR = Path(__file__).resolve().parents[2] / "databricks" / "analytics"


def load_sql(name: str, catalog: str, schema: str) -> str:
    sql = (SQL_DIR / name).read_text()
    return (
        sql.replace("${catalog}", catalog)
        .replace("${schema}", schema)
        .rstrip()
        .rstrip(";")
    )


@pytest.fixture(scope="module")
def views(spark, customer_df, account_df, fact_df):
    # Temp views have no catalog/schema, so substitute a dotted prefix that is stripped.
    customer_df.createOrReplaceTempView("DimCustomer")
    account_df.createOrReplaceTempView("DimAccount")
    fact_df.createOrReplaceTempView("FactTransaction")


def run_sql(spark, name: str, args: dict) -> str:
    sql = load_sql(name, "c", "s").replace("c.s.", "")
    return spark.sql(sql, args=args)


def test_daily_transaction_sql_matches_dataframe(spark, fact_df, views):
    sql_rows = run_sql(
        spark,
        "daily_transaction.sql",
        {"start_date": "2024-01-01", "end_date": "2024-01-31"},
    ).collect()
    df_rows = daily_transaction(fact_df, "2024-01-01", "2024-01-31").collect()
    assert [tuple(r) for r in sql_rows] == [tuple(r) for r in df_rows]
    assert len(sql_rows) > 0


@pytest.mark.parametrize("name", ["smith", "JANE", ""])
def test_balance_sql_matches_dataframe(
    spark, customer_df, account_df, fact_df, views, name
):
    sql_rows = {
        tuple(r)
        for r in run_sql(
            spark, "balance_per_customer.sql", {"customer_name": name}
        ).collect()
    }
    df_rows = {
        tuple(r)
        for r in balance_per_customer(customer_df, account_df, fact_df, name).collect()
    }
    assert sql_rows == df_rows
