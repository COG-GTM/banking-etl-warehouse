"""Post-load validation task for the medallion workflow.

Runs the two migrated analytics functions and the referential-integrity checks
that the legacy T-SQL foreign keys used to enforce, and fails the task when a
hard check breaks (orphans are reported, never dropped).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))

from gold.analytics import GoldAnalytics  # noqa: E402
from gold.config import GoldConfig  # noqa: E402


def check_primary_key(spark: SparkSession, config: GoldConfig) -> int:
    fact = spark.table(config.fact_fqn)
    return fact.groupBy("transaction_id").count().filter(F.col("count") > 1).count()


def check_orphans(spark: SparkSession, config: GoldConfig) -> int:
    fact = spark.table(config.fact_fqn)
    accounts = spark.table(config.dim_account_fqn).select("account_id")
    branches = spark.table(config.dim_branch_fqn).select("branch_id")
    orphan_accounts = (
        fact.filter(F.col("account_id").isNotNull())
        .join(accounts, "account_id", "left_anti")
        .count()
    )
    orphan_branches = (
        fact.filter(F.col("branch_id").isNotNull())
        .join(branches, "branch_id", "left_anti")
        .count()
    )
    return orphan_accounts + orphan_branches


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate the gold layer")
    parser.add_argument("--catalog", default="banking")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--customer-name", default="")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    config = GoldConfig(catalog=args.catalog, gold_schema=args.gold_schema)

    duplicate_keys = check_primary_key(spark, config)
    orphan_rows = check_orphans(spark, config)

    analytics = GoldAnalytics(spark, config)
    analytics.daily_transaction(args.start_date, args.end_date).show(truncate=False)
    analytics.balance_per_customer(args.customer_name).show(truncate=False)

    print(f"duplicate transaction_id keys={duplicate_keys} orphan fact rows={orphan_rows}")
    if duplicate_keys or orphan_rows:
        raise SystemExit(
            f"gold validation failed: duplicate_keys={duplicate_keys}, orphan_rows={orphan_rows}"
        )


if __name__ == "__main__":
    main()
