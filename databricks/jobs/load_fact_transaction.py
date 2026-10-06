"""Load_FactTransaction: SQL + Excel + CSV transactions -> dwh.fact_transaction.

Mirrors the Talend job:
  tUnite    sample.dbo.transaction_db (merge order 1), transaction_excel.xlsx (2),
            transaction_csv.csv (3)
  tUniqRow  unique on transaction_id, keeping the first row in merge order
  tMap      snake_case -> FactTransaction columns
  FKs       rows with no matching dim_account / dim_branch are rejected (SQL Server
            enforced these constraints; Delta does not), so the dims must load first.
"""

try:
    import _bootstrap  # noqa: F401  (spark_python_task: puts databricks/ on sys.path)
except ImportError:
    pass

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common.config import JobConfig, source_jdbc
from common.io import read_csv, read_excel, read_jdbc, write_table
from common.runner import main

log = logging.getLogger(__name__)

TARGET = "fact_transaction"
FILE_DATE_FORMAT = "dd-MM-yyyy HH:mm:ss"
SOURCE_ORDER = {"sql": 1, "excel": 2, "csv": 3}


def _parse_transaction_date(df: DataFrame) -> F.Column:
    if isinstance(df.schema["transaction_date"].dataType, StringType):
        return F.to_timestamp(F.trim("transaction_date"), FILE_DATE_FORMAT)
    return F.col("transaction_date").cast("timestamp")


def normalize_source(df: DataFrame, source: str) -> DataFrame:
    return df.select(
        F.col("transaction_id").cast("int").alias("transaction_id"),
        F.col("account_id").cast("int").alias("account_id"),
        _parse_transaction_date(df).alias("transaction_date"),
        F.col("amount").cast("decimal(19,4)").alias("amount"),
        F.col("transaction_type").cast("string").alias("transaction_type"),
        F.col("branch_id").cast("int").alias("branch_id"),
        F.lit(SOURCE_ORDER[source]).alias("_source_order"),
        F.monotonically_increasing_id().alias("_row_order"),
    )


def dedup_transactions(unioned: DataFrame) -> DataFrame:
    """tUniqRow on transaction_id, deterministic about which source wins (SQL first)."""
    w = Window.partitionBy("transaction_id").orderBy("_source_order", "_row_order")
    return (
        unioned.where(F.col("transaction_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn", "_source_order", "_row_order")
    )


def transform(sql_src: DataFrame, excel_src: DataFrame, csv_src: DataFrame) -> DataFrame:
    unioned = (
        normalize_source(sql_src, "sql")
        .unionByName(normalize_source(excel_src, "excel"))
        .unionByName(normalize_source(csv_src, "csv"))
    )
    return dedup_transactions(unioned).select(
        F.col("transaction_id").alias("TransactionID"),
        F.col("account_id").alias("AccountID"),
        F.col("transaction_date").alias("TransactionDate"),
        F.col("amount").alias("Amount"),
        F.col("transaction_type").alias("TransactionType"),
        F.col("branch_id").alias("BranchID"),
    )


def split_foreign_key_rejects(
    fact: DataFrame, dim_account: DataFrame, dim_branch: DataFrame
) -> tuple[DataFrame, DataFrame]:
    accounts = dim_account.select("AccountID", F.lit(True).alias("_account_ok"))
    branches = dim_branch.select("BranchID", F.lit(True).alias("_branch_ok"))
    checked = fact.join(accounts, "AccountID", "left").join(branches, "BranchID", "left")
    ok = (F.col("AccountID").isNull() | F.col("_account_ok").isNotNull()) & (
        F.col("BranchID").isNull() | F.col("_branch_ok").isNotNull()
    )
    return checked.where(ok).select(*fact.columns), checked.where(~ok).select(*fact.columns)


def run(spark: SparkSession, cfg: JobConfig) -> int:
    fact = transform(
        read_jdbc(spark, source_jdbc(spark, cfg), "dbo.transaction_db"),
        read_excel(spark, cfg.landing_file("transaction_excel.xlsx")),
        read_csv(spark, cfg.landing_file("transaction_csv.csv")),
    ).cache()
    valid, rejects = split_foreign_key_rejects(
        fact, spark.table(cfg.table("dim_account")), spark.table(cfg.table("dim_branch"))
    )
    rejected = [r.TransactionID for r in rejects.orderBy("TransactionID").collect()]
    if rejected:
        log.warning("Rejected %d rows with no matching dim_account/dim_branch: %s", len(rejected), rejected)
    return write_table(valid, cfg, TARGET)


if __name__ == "__main__":
    main(run)
