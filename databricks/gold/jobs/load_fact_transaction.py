"""Databricks entrypoint for `gold.fact_transaction`.

Everything cluster-specific (widgets, table reads, Delta MERGE) is confined to
this module; the transforms it calls are pure functions in
`databricks.gold.fact_transaction`.

Run as a job task:
    python databricks/gold/jobs/load_fact_transaction.py \
        --catalog banking --bronze-schema bronze --gold-schema gold
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

sys.path.append(str(Path(__file__).resolve().parents[2]))

from gold.config import GoldConfig, TransactionSource  # noqa: E402
from gold.fact_transaction import (  # noqa: E402
    FACT_COLUMNS,
    MERGE_KEY,
    FactLoadResult,
    build_fact_transaction,
)


def load_sources(
    spark: SparkSession, config: GoldConfig
) -> list[tuple[TransactionSource, DataFrame]]:
    return [(source, spark.table(source.table)) for source in config.sources()]


def merge_fact(spark: SparkSession, result: FactLoadResult, config: GoldConfig) -> None:
    """Idempotent upsert of the fact rows keyed on `transaction_id`.

    The legacy tMSSqlOutput used TRUNCATE + INSERT (full reload). A MERGE keeps
    the same end state for a full run while allowing incremental runs.
    """
    from delta.tables import DeltaTable  # imported here: Databricks runtime only

    target = DeltaTable.forName(spark, config.fact_fqn)
    updates = {column: f"source.{column}" for column in FACT_COLUMNS}
    (
        target.alias("target")
        .merge(result.fact.alias("source"), f"target.{MERGE_KEY} = source.{MERGE_KEY}")
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsert(values=updates)
        .execute()
    )


def write_side_channel(df: DataFrame, table: str, run_id: str | None) -> None:
    from pyspark.sql import functions as F

    tagged = df.withColumn("_run_id", F.lit(run_id))
    tagged.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


def run(spark: SparkSession, config: GoldConfig, run_id: str | None = None) -> FactLoadResult:
    result = build_fact_transaction(
        load_sources(spark, config),
        spark.table(config.dim_account_fqn),
        spark.table(config.dim_branch_fqn),
        config,
    )
    result.fact.cache()
    merge_fact(spark, result, config)
    write_side_channel(result.orphans, config.orphan_fqn, run_id)
    write_side_channel(result.duplicates, config.duplicate_fqn, run_id)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load gold.fact_transaction")
    parser.add_argument("--catalog", default="banking")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--mssql-table", default="mssql_transaction")
    parser.add_argument("--excel-table", default="excel_transaction")
    parser.add_argument("--csv-table", default="csv_transaction")
    parser.add_argument("--run-id", default=None)
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> GoldConfig:
    return GoldConfig(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        gold_schema=args.gold_schema,
        source_tables={
            "mssql": args.mssql_table,
            "excel": args.excel_table,
            "csv": args.csv_table,
        },
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    result = run(spark, config_from_args(args), args.run_id)
    print(
        f"fact rows merged={result.fact.count()} "
        f"orphans={result.orphans.count()} duplicates={result.duplicates.count()}"
    )


if __name__ == "__main__":
    main()
