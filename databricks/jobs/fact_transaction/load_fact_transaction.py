"""Entry point for the gold ``FactTransaction`` load.

Run from the ``databricks/jobs`` directory (or with it on ``PYTHONPATH``).

Databricks job / notebook usage::

    python -m fact_transaction.load_fact_transaction \
        --catalog main --schema gold \
        --csv-path dbfs:/mnt/raw/transaction_csv.csv \
        --excel-path dbfs:/mnt/raw/transaction_excel.xlsx

Local (no Delta, no Unity Catalog) usage::

    python -m fact_transaction.load_fact_transaction \
        --bronze-table "" \
        --csv-path data_sources/transaction_csv.csv \
        --excel-path data_sources/transaction_excel.xlsx \
        --format parquet --path /tmp/fact_transaction
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .readers import (
    read_csv_transactions,
    read_excel_transactions,
    read_table_transactions,
)
from .transforms import build_fact_transaction, dedupe_transactions, unify_transactions


@dataclass(frozen=True)
class JobParams:
    catalog: str = "main"
    schema: str = "gold"
    bronze_schema: str = "bronze"
    bronze_table: str | None = "transaction_db"
    target_table: str = "FactTransaction"
    csv_path: str | None = None
    excel_path: str | None = None
    excel_sheet: str = "Sheet1"
    output_format: str = "delta"
    output_path: str | None = None
    use_spark_excel: bool = False


def parse_args(argv: Sequence[str] | None = None) -> JobParams:
    parser = argparse.ArgumentParser(description="Load the gold FactTransaction table")
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema", default="gold")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument(
        "--bronze-table",
        default="transaction_db",
        help="bronze transaction table; pass an empty value to skip the DB source",
    )
    parser.add_argument("--target-table", default="FactTransaction")
    parser.add_argument("--csv-path")
    parser.add_argument("--excel-path")
    parser.add_argument("--excel-sheet", default="Sheet1")
    parser.add_argument("--format", dest="output_format", default="delta")
    parser.add_argument("--path", dest="output_path")
    parser.add_argument("--use-spark-excel", action="store_true")
    args = parser.parse_args(argv)
    return JobParams(
        catalog=args.catalog,
        schema=args.schema,
        bronze_schema=args.bronze_schema,
        bronze_table=args.bronze_table or None,
        target_table=args.target_table,
        csv_path=args.csv_path,
        excel_path=args.excel_path,
        excel_sheet=args.excel_sheet,
        output_format=args.output_format,
        output_path=args.output_path,
        use_spark_excel=args.use_spark_excel,
    )


def params_from_widgets(dbutils) -> JobParams:
    """Build :class:`JobParams` from Databricks notebook widgets."""
    defaults = JobParams()
    names = {
        "catalog": defaults.catalog,
        "schema": defaults.schema,
        "bronze_schema": defaults.bronze_schema,
        "bronze_table": defaults.bronze_table or "",
        "target_table": defaults.target_table,
        "csv_path": "",
        "excel_path": "",
        "excel_sheet": defaults.excel_sheet,
    }
    for name, default in names.items():
        dbutils.widgets.text(name, default)
    values = {name: dbutils.widgets.get(name) for name in names}
    return JobParams(
        catalog=values["catalog"],
        schema=values["schema"],
        bronze_schema=values["bronze_schema"],
        bronze_table=values["bronze_table"] or None,
        target_table=values["target_table"],
        csv_path=values["csv_path"] or None,
        excel_path=values["excel_path"] or None,
        excel_sheet=values["excel_sheet"],
    )


def bronze_table_name(params: JobParams) -> str:
    """Fully qualify ``bronze_table`` unless it already carries a namespace."""
    if not params.bronze_table:
        raise ValueError("bronze_table is not configured")
    if "." in params.bronze_table:
        return params.bronze_table
    return f"{params.catalog}.{params.bronze_schema}.{params.bronze_table}"


def read_sources(spark: SparkSession, params: JobParams) -> list[DataFrame]:
    """Read every configured source, in Talend tUnite merge order."""
    sources: list[DataFrame] = []
    if params.bronze_table:
        sources.append(read_table_transactions(spark, bronze_table_name(params)))
    if params.excel_path:
        sources.append(
            read_excel_transactions(
                spark,
                params.excel_path,
                sheet=params.excel_sheet,
                use_spark_excel=params.use_spark_excel,
            )
        )
    if params.csv_path:
        sources.append(read_csv_transactions(spark, params.csv_path))
    if not sources:
        raise ValueError(
            "no sources configured: pass --csv-path, --excel-path or --bronze-table"
        )
    return sources


def build(spark: SparkSession, params: JobParams) -> DataFrame:
    unified = unify_transactions(*read_sources(spark, params))
    return build_fact_transaction(dedupe_transactions(unified))


def write(fact_df: DataFrame, params: JobParams) -> None:
    writer = fact_df.write.format(params.output_format).mode("overwrite")
    if params.output_path:
        writer.option("overwriteSchema", "true").save(params.output_path)
    else:
        writer.option("overwriteSchema", "true").saveAsTable(
            f"{params.catalog}.{params.schema}.{params.target_table}"
        )


def main(argv: Sequence[str] | None = None) -> None:
    params = parse_args(argv)
    spark = SparkSession.builder.appName("load_fact_transaction").getOrCreate()
    write(build(spark, params), params)


if __name__ == "__main__":
    main()
