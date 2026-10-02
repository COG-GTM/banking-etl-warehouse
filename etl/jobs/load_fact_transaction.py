"""Load_FactTransaction: SQL + Excel + CSV transactions -> DWH.dbo.FactTransaction.

Mirrors the Talend job:
  tUnite    sample.dbo.transaction_db (merge order 1), transaction_excel.xlsx (2),
            transaction_csv.csv (3)
  tUniqRow  unique on transaction_id, keeping the first row in merge order
  tMap      snake_case -> FactTransaction columns
  tMSSqlOutput (TRUNCATE + INSERT, die-on-error off): rows violating the
            FactTransaction -> DimAccount / DimBranch foreign keys are rejected, not loaded.
"""

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from etl.config import EtlConfig, load_config
from etl.jdbc import read_table, write_table

log = logging.getLogger(__name__)

FILE_DATE_FORMAT = "dd-MM-yyyy HH:mm:ss"
SOURCE_COLUMNS = (
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
)
SOURCE_ORDER = {"sql": 1, "excel": 2, "csv": 3}


def _parse_transaction_date(df: DataFrame) -> F.Column:
    if isinstance(df.schema["transaction_date"].dataType, StringType):
        return F.to_timestamp(F.trim("transaction_date"), FILE_DATE_FORMAT)
    return F.col("transaction_date").cast("timestamp")


def normalize_source(df: DataFrame, source: str) -> DataFrame:
    """Cast a transaction source to a common snake_case schema tagged with its merge order."""
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
    """tUniqRow on transaction_id: keep the first occurrence in tUnite merge order.

    Same result as dropDuplicates("transaction_id"), but deterministic about which row
    survives when sources disagree (e.g. transaction_id 6 and 7 exist in both the SQL
    table and the Excel file with different dates; Talend keeps the SQL rows).
    """
    w = Window.partitionBy("transaction_id").orderBy("_source_order", "_row_order")
    return (
        unioned.where(F.col("transaction_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def to_fact_schema(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("transaction_id").alias("TransactionID"),
        F.col("account_id").alias("AccountID"),
        F.col("transaction_date").alias("TransactionDate"),
        F.col("amount").cast("decimal(19,4)").alias("Amount"),
        F.col("transaction_type").alias("TransactionType"),
        F.col("branch_id").alias("BranchID"),
    )


def transform(sql_src: DataFrame, excel_src: DataFrame, csv_src: DataFrame) -> DataFrame:
    unioned = (
        normalize_source(sql_src, "sql")
        .unionByName(normalize_source(excel_src, "excel"))
        .unionByName(normalize_source(csv_src, "csv"))
    )
    return to_fact_schema(dedup_transactions(unioned))


def split_foreign_key_rejects(
    fact: DataFrame, account_ids: DataFrame, branch_ids: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Split fact rows into (valid, rejected) against the DimAccount / DimBranch keys."""
    accounts = account_ids.select(F.col("AccountID"), F.lit(True).alias("_account_ok"))
    branches = branch_ids.select(F.col("BranchID"), F.lit(True).alias("_branch_ok"))
    checked = fact.join(accounts, "AccountID", "left").join(branches, "BranchID", "left")
    ok = (F.col("AccountID").isNull() | F.col("_account_ok").isNotNull()) & (
        F.col("BranchID").isNull() | F.col("_branch_ok").isNotNull()
    )
    columns = fact.columns
    return checked.where(ok).select(*columns), checked.where(~ok).select(*columns)


def read_sources(spark: SparkSession, cfg: EtlConfig) -> tuple[DataFrame, DataFrame, DataFrame]:
    sql_src = read_table(spark, cfg.source, cfg.source_tables.transaction)
    excel_src = (
        spark.read.format("com.crealytics.spark.excel")
        .option("dataAddress", f"'{cfg.excel_sheet}'!A1")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cfg.transaction_excel_path)
    )
    csv_src = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(cfg.transaction_csv_path)
    )
    return sql_src, excel_src, csv_src


def run(spark: SparkSession, cfg: EtlConfig, mode: str | None = None) -> int:
    fact = transform(*read_sources(spark, cfg)).cache()
    if cfg.enforce_foreign_keys:
        fact, rejects = split_foreign_key_rejects(
            fact,
            read_table(spark, cfg.target, cfg.target_tables.dim_account),
            read_table(spark, cfg.target, cfg.target_tables.dim_branch),
        )
        rejected = rejects.orderBy("TransactionID").collect()
        if rejected:
            log.warning(
                "Rejected %d FactTransaction rows with no matching DimAccount/DimBranch: %s",
                len(rejected),
                [r.TransactionID for r in rejected],
            )
    return write_table(
        spark, fact, cfg.target, cfg.target_tables.fact_transaction, mode or cfg.write_mode
    )


if __name__ == "__main__":
    from etl.main import run_single

    run_single(run, load_config())
