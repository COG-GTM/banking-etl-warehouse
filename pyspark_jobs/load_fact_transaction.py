"""Load_FactTransaction: SQL Server + Excel + CSV transactions -> DWH.dbo.FactTransaction.

Mirrors the Talend job:
  * tMSSqlInput (sample.dbo.transaction_db), tFileInputExcel and
    tFileInputDelimited are normalised to one schema,
  * tUnite merges them in the order SQL -> Excel -> CSV (``unionByName``),
  * tUniqRow keeps the first row per ``transaction_id``: rows are first
    narrowed to the highest-priority source for each key, then
    ``dropDuplicates(["transaction_id"])`` removes any remaining duplicates,
  * tMSSqlOutput truncates FactTransaction and inserts. Talend ran with
    die-on-error off, so rows violating the FK to DimAccount / DimBranch were
    rejected by SQL Server and skipped; here they are filtered out (and
    logged) before the write so the batch insert does not fail.
"""

from __future__ import annotations

import logging
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pyspark_jobs.common.config import EtlConfig, load_config
from pyspark_jobs.common.io import read_csv, read_excel, read_jdbc_table, write_jdbc
from pyspark_jobs.common.spark_session import get_spark

log = logging.getLogger(__name__)

TARGET_TABLE = "FactTransaction"
SOURCE_PRIORITY_COL = "_source_priority"
SOURCE_PRIORITY = {"sqlserver": 1, "excel": 2, "csv": 3}

CSV_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), False),
        StructField("account_id", IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", IntegerType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("branch_id", IntegerType(), True),
    ]
)


def normalize(df: DataFrame, source: str) -> DataFrame:
    """Cast a raw transaction stream to the common snake_case schema."""
    return df.select(
        F.col("transaction_id").cast("int").alias("transaction_id"),
        F.col("account_id").cast("int").alias("account_id"),
        F.col("transaction_date").cast("timestamp").alias("transaction_date"),
        F.col("amount").cast("decimal(19,4)").alias("amount"),
        F.trim(F.col("transaction_type").cast("string")).alias("transaction_type"),
        F.col("branch_id").cast("int").alias("branch_id"),
        F.lit(SOURCE_PRIORITY[source]).alias(SOURCE_PRIORITY_COL),
    )


def unite_and_dedup(sql_df: DataFrame, excel_df: DataFrame, csv_df: DataFrame) -> DataFrame:
    united = (
        normalize(sql_df, "sqlserver")
        .unionByName(normalize(excel_df, "excel"))
        .unionByName(normalize(csv_df, "csv"))
        .filter(F.col("transaction_id").isNotNull())
    )
    best = F.min(SOURCE_PRIORITY_COL).over(Window.partitionBy("transaction_id"))
    return (
        united.withColumn("_best_priority", best)
        .filter(F.col(SOURCE_PRIORITY_COL) == F.col("_best_priority"))
        .dropDuplicates(["transaction_id"])
        .drop("_best_priority", SOURCE_PRIORITY_COL)
    )


def enforce_foreign_keys(df: DataFrame, dim_account: DataFrame, dim_branch: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, rejected) against DimAccount / DimBranch.

    NULL FK values are allowed, as they are by the SQL Server constraints.
    """
    accounts = dim_account.select(F.col("AccountID").alias("_fk_account")).distinct()
    branches = dim_branch.select(F.col("BranchID").alias("_fk_branch")).distinct()
    checked = (
        df.join(accounts, df.account_id == accounts._fk_account, "left")
        .join(branches, df.branch_id == branches._fk_branch, "left")
        .withColumn(
            "_fk_ok",
            (F.col("account_id").isNull() | F.col("_fk_account").isNotNull())
            & (F.col("branch_id").isNull() | F.col("_fk_branch").isNotNull()),
        )
        .drop("_fk_account", "_fk_branch")
    )
    valid = checked.filter("_fk_ok").drop("_fk_ok")
    rejected = checked.filter(~F.col("_fk_ok")).drop("_fk_ok")
    return valid, rejected


def to_fact(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("transaction_id").alias("TransactionID"),
        F.col("account_id").alias("AccountID"),
        F.col("transaction_date").alias("TransactionDate"),
        F.col("amount").alias("Amount"),
        F.col("transaction_type").alias("TransactionType"),
        F.col("branch_id").alias("BranchID"),
    )


def extract(spark: SparkSession, config: EtlConfig) -> tuple[DataFrame, DataFrame, DataFrame]:
    sql_df = read_jdbc_table(spark, config.source, config.source_table(config.transaction_table))
    excel_df = read_excel(spark, config.files)
    csv_df = read_csv(
        spark,
        config.files.transaction_csv,
        schema=CSV_SCHEMA,
        timestamp_format=config.files.csv_timestamp_format,
    )
    return sql_df, excel_df, csv_df


def run(spark: SparkSession, config: EtlConfig) -> int:
    sql_df, excel_df, csv_df = extract(spark, config)
    deduped = unite_and_dedup(sql_df, excel_df, csv_df).cache()

    if config.enforce_fact_foreign_keys:
        dim_account = read_jdbc_table(spark, config.target, config.target_table("DimAccount"))
        dim_branch = read_jdbc_table(spark, config.target, config.target_table("DimBranch"))
        valid, rejected = enforce_foreign_keys(deduped, dim_account, dim_branch)
        rejected_rows = rejected.collect()
        if rejected_rows:
            log.warning(
                "Load_FactTransaction: skipping %s rows with no matching DimAccount/DimBranch: %s",
                len(rejected_rows),
                sorted(r.transaction_id for r in rejected_rows),
            )
    else:
        valid = deduped

    out = to_fact(valid).cache()
    write_jdbc(out, spark, config.target, config.target_table(TARGET_TABLE), config.fact_write_mode)
    rows = out.count()
    log.info("Load_FactTransaction: wrote %s rows to %s", rows, TARGET_TABLE)
    deduped.unpersist()
    out.unpersist()
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(get_spark("Load_FactTransaction"), load_config())


if __name__ == "__main__":
    main()
