"""Databricks entry point for the three dimension loads.

Everything Databricks-specific (widgets, catalog/schema names, Delta writes)
lives here; the transforms themselves are plain DataFrame functions.

Run as a job or notebook:

    %run ./run_silver_dimensions        # widgets: catalog, bronze/silver/gold schema, write_mode
"""

from __future__ import annotations

import argparse
from dataclasses import replace

from pyspark.sql import SparkSession

from .common import DimensionConfig
from .dim_account import DIM_ACCOUNT_CONFIG, transform_dim_account
from .dim_branch import DIM_BRANCH_CONFIG, transform_dim_branch
from .dim_customer import DIM_CUSTOMER_CONFIG, transform_dim_customer
from .publish import full_refresh_dimension, merge_dimension, write_silver

WRITE_MODES = ("merge", "full_refresh")


def _publish(spark: SparkSession, df, config: DimensionConfig, write_mode: str) -> None:
    write_silver(df, config.silver_fqn)
    silver = spark.table(config.silver_fqn)
    if write_mode == "merge":
        merge_dimension(spark, silver, config.gold_fqn, config.business_key, order_by=None)
    elif write_mode == "full_refresh":
        full_refresh_dimension(spark, silver, config.gold_fqn)
    else:
        raise ValueError(f"write_mode must be one of {WRITE_MODES}, got {write_mode!r}")


def run_dim_branch(spark: SparkSession, config: DimensionConfig, write_mode: str = "merge") -> None:
    df = transform_dim_branch(spark.table(config.bronze("branch")))
    _publish(spark, df, config, write_mode)


def run_dim_account(spark: SparkSession, config: DimensionConfig, write_mode: str = "merge") -> None:
    df = transform_dim_account(spark.table(config.bronze("account")))
    _publish(spark, df, config, write_mode)


def run_dim_customer(spark: SparkSession, config: DimensionConfig, write_mode: str = "merge") -> None:
    df = transform_dim_customer(
        spark.table(config.bronze("customer")),
        spark.table(config.bronze("city")),
        spark.table(config.bronze("state")),
    )
    _publish(spark, df, config, write_mode)


def run_all(
    spark: SparkSession,
    catalog: str = "banking",
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    write_mode: str = "merge",
) -> None:
    def scoped(config: DimensionConfig) -> DimensionConfig:
        return replace(
            config,
            catalog=catalog,
            bronze_schema=bronze_schema,
            silver_schema=silver_schema,
            gold_schema=gold_schema,
        )

    run_dim_branch(spark, scoped(DIM_BRANCH_CONFIG), write_mode)
    run_dim_account(spark, scoped(DIM_ACCOUNT_CONFIG), write_mode)
    run_dim_customer(spark, scoped(DIM_CUSTOMER_CONFIG), write_mode)


def _args_from_widgets() -> dict[str, str] | None:
    try:
        dbutils  # type: ignore[name-defined]  # noqa: B018 - injected by Databricks
    except NameError:
        return None
    for name, default in (
        ("catalog", "banking"),
        ("bronze_schema", "bronze"),
        ("silver_schema", "silver"),
        ("gold_schema", "gold"),
        ("write_mode", "merge"),
    ):
        dbutils.widgets.text(name, default)  # type: ignore[name-defined]
    return {
        name: dbutils.widgets.get(name)  # type: ignore[name-defined]
        for name in ("catalog", "bronze_schema", "silver_schema", "gold_schema", "write_mode")
    }


def main() -> None:
    kwargs = _args_from_widgets()
    if kwargs is None:
        parser = argparse.ArgumentParser(description="Load the silver/gold banking dimensions")
        parser.add_argument("--catalog", default="banking")
        parser.add_argument("--bronze-schema", default="bronze")
        parser.add_argument("--silver-schema", default="silver")
        parser.add_argument("--gold-schema", default="gold")
        parser.add_argument("--write-mode", default="merge", choices=WRITE_MODES)
        parsed = parser.parse_args()
        kwargs = {
            "catalog": parsed.catalog,
            "bronze_schema": parsed.bronze_schema,
            "silver_schema": parsed.silver_schema,
            "gold_schema": parsed.gold_schema,
            "write_mode": parsed.write_mode,
        }
    run_all(SparkSession.builder.getOrCreate(), **kwargs)


if __name__ == "__main__":
    main()
