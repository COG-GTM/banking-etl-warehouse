"""Load_FactTransaction: SQL Server + Excel + CSV transactions -> DWH.dbo.FactTransaction.

Mirrors the Talend job: the three inputs are merged with tUnite (merge order SQL Server,
Excel, CSV), tUniqRow keeps the first row per transaction_id, and tMSSqlOutput truncates
FactTransaction and inserts with die-on-error off. Because die-on-error is off, Talend
silently drops rows that fail to parse in the file inputs and rows that violate the
FactTransaction foreign keys; this job drops the same rows explicitly and logs them.
"""

from __future__ import annotations

import argparse
import logging

if __package__ in (None, ""):  # allow `spark-submit pyspark_jobs/<job>.py` without --py-files
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pyspark_jobs.common.config import EtlConfig
from pyspark_jobs.common.io import read_csv_as_strings, read_excel_as_strings, read_jdbc
from pyspark_jobs.common.jobs import configure_logging, load_table
from pyspark_jobs.common.spark_session import get_spark

log = logging.getLogger(__name__)

TARGET_TABLE = "FactTransaction"
MONEY = "decimal(19,4)"
SOURCE_COLUMNS = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id"]
# tUnite merge order: lower number wins in tUniqRow.
SOURCE_PRIORITY = {"sqlserver": 1, "excel": 2, "csv": 3}
EXCEL_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def extract(spark: SparkSession, cfg: EtlConfig) -> dict[str, DataFrame]:
    files = cfg.files
    return {
        "sqlserver": read_jdbc(spark, cfg.source_db, table="transaction_db"),
        "excel": read_excel_as_strings(
            spark,
            files.transaction_excel,
            SOURCE_COLUMNS,
            files.excel_sheet,
            files.excel_reader,
            EXCEL_TIMESTAMP_FORMAT,
        ),
        "csv": read_csv_as_strings(spark, files.transaction_csv, SOURCE_COLUMNS),
    }


def _typed_exprs(timestamp_format: str | None, lenient_timestamps: bool = False) -> list:
    """Cast expressions to the canonical source schema, in SOURCE_COLUMNS order.

    The CSV is parsed with its configured pattern only (Talend rejects other formats); Excel cells
    may come back as real datetimes or text, so ``lenient_timestamps`` adds a generic fallback.
    """

    def as_int(c: str):
        return F.trim(F.col(c).cast("string")).cast("int")

    if timestamp_format:
        ts = F.to_timestamp(F.trim("transaction_date"), timestamp_format)
        if lenient_timestamps:
            ts = F.coalesce(ts, F.to_timestamp(F.trim("transaction_date")))
    else:
        ts = F.col("transaction_date").cast("timestamp")
    return [
        as_int("transaction_id"),
        as_int("account_id"),
        ts,
        F.trim(F.col("amount").cast("string")).cast(MONEY),
        F.col("transaction_type").cast("string"),
        as_int("branch_id"),
    ]


def normalize(
    df: DataFrame,
    source: str,
    timestamp_format: str | None = None,
    cached: list[DataFrame] | None = None,
) -> DataFrame:
    """Normalizes one source to the canonical schema, dropping rows that fail to parse
    (Talend file inputs run with die-on-error off, which rejects such rows)."""
    flagged = df.select(
        *[F.col(c).cast("string").alias(f"_raw_{c}") for c in SOURCE_COLUMNS],
        *[expr.alias(c) for expr, c in zip(_typed_exprs(timestamp_format, source == "excel"), SOURCE_COLUMNS)],
    )
    rejected = F.col("transaction_id").isNull()
    for c in SOURCE_COLUMNS:
        raw = F.col(f"_raw_{c}")
        rejected = rejected | (raw.isNotNull() & (F.trim(raw) != "") & F.col(c).isNull())
    flagged = flagged.withColumn("_rejected", rejected).cache()
    if cached is not None:
        cached.append(flagged)
    rejected_count = flagged.filter("_rejected").count()
    if rejected_count:
        log.warning("%s: dropping %d unparseable rows", source, rejected_count)
    return (
        flagged.filter(~F.col("_rejected"))
        .select(*SOURCE_COLUMNS)
        .withColumn("_source", F.lit(source))
        .withColumn("_source_priority", F.lit(SOURCE_PRIORITY[source]))
        .withColumn("_source_row", F.monotonically_increasing_id())
    )


def unite_and_dedupe(sources: list[DataFrame]) -> DataFrame:
    """tUnite + tUniqRow: union all sources and keep the first row per transaction_id.

    A bare ``dropDuplicates(["transaction_id"])`` keeps an arbitrary row, so the survivor is
    picked explicitly by tUnite merge order (SQL Server, Excel, CSV), then input order.
    """
    united = sources[0]
    for df in sources[1:]:
        united = united.unionByName(df)
    first = Window.partitionBy("transaction_id").orderBy("_source_priority", "_source_row")
    return (
        united.withColumn("_rank", F.row_number().over(first))
        .filter(F.col("_rank") == 1)
        .dropDuplicates(["transaction_id"])
        .drop("_rank")
    )


def reject_orphans(facts: DataFrame, dim_account: DataFrame, dim_branch: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Splits rows into (valid, orphans) against FK_FactTransaction_DimAccount/DimBranch.

    NULL foreign keys are allowed by the DDL, so only non-null unknown keys are orphans.
    """
    accounts = dim_account.select(F.col("AccountID").alias("_fk_account")).distinct()
    branches = dim_branch.select(F.col("BranchID").alias("_fk_branch")).distinct()
    checked = (
        facts.join(accounts, facts.account_id == accounts._fk_account, "left")
        .join(branches, facts.branch_id == branches._fk_branch, "left")
        .withColumn(
            "_orphan",
            (F.col("account_id").isNotNull() & F.col("_fk_account").isNull())
            | (F.col("branch_id").isNotNull() & F.col("_fk_branch").isNull()),
        )
        .drop("_fk_account", "_fk_branch")
    )
    return checked.filter(~F.col("_orphan")).drop("_orphan"), checked.filter("_orphan").drop("_orphan")


def to_fact_schema(df: DataFrame) -> DataFrame:
    """tMap: rename to the DWH FactTransaction columns."""
    return df.select(
        F.col("transaction_id").alias("TransactionID"),
        F.col("account_id").alias("AccountID"),
        F.col("transaction_date").alias("TransactionDate"),
        F.col("amount").alias("Amount"),
        F.col("transaction_type").alias("TransactionType"),
        F.col("branch_id").alias("BranchID"),
    )


def transform(
    raw: dict[str, DataFrame],
    dim_account: DataFrame,
    dim_branch: DataFrame,
    csv_timestamp_format: str = "dd-MM-yyyy HH:mm:ss",
    cached: list[DataFrame] | None = None,
) -> DataFrame:
    """Builds the FactTransaction frame. Frames it caches are appended to ``cached`` so the
    caller can unpersist them once the result has been written."""
    sources = [
        normalize(raw["sqlserver"], "sqlserver", cached=cached),
        normalize(raw["excel"], "excel", EXCEL_TIMESTAMP_FORMAT, cached=cached),
        normalize(raw["csv"], "csv", csv_timestamp_format, cached=cached),
    ]
    unique = unite_and_dedupe(sources).cache()
    if cached is not None:
        cached.append(unique)
    valid, orphans = reject_orphans(unique, dim_account, dim_branch)
    orphan_rows = orphans.select("transaction_id", "account_id", "branch_id", "_source").collect()
    if orphan_rows:
        log.warning(
            "Dropping %d rows that violate FactTransaction foreign keys: %s",
            len(orphan_rows),
            [row.asDict() for row in orphan_rows],
        )
    return to_fact_schema(valid)


def run(spark: SparkSession, cfg: EtlConfig, mode: str = "overwrite") -> int:
    cached: list[DataFrame] = []
    try:
        facts = transform(
            extract(spark, cfg),
            read_jdbc(spark, cfg.dwh_db, table="DimAccount"),
            read_jdbc(spark, cfg.dwh_db, table="DimBranch"),
            cfg.files.csv_timestamp_format,
            cached,
        )
        return load_table(spark, cfg, facts, TARGET_TABLE, "TransactionID", mode, referenced_by_fk=False)
    finally:
        for df in cached:
            df.unpersist()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="overwrite = TRUNCATE + INSERT (Talend default); append = insert new TransactionIDs only",
    )
    args = parser.parse_args(argv)
    configure_logging()
    cfg = EtlConfig.from_env()
    run(get_spark("load_fact_transaction", cfg), cfg, args.mode)


if __name__ == "__main__":
    main()
