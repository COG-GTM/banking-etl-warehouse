"""Load_FactTransaction: SQL Server + Excel + CSV -> dwh.fact_transaction.

Talend flow: tMSSqlInput / tFileInputExcel / tFileInputDelimited -> tUnite -> tUniqRow
(key transaction_id) -> tMap -> tMSSqlOutput (INSERT, die-on-error off).

* tUnite  -> unionByName, in Talend's input order SQL Server, Excel, CSV.
* tUniqRow keeps the first row per transaction_id. Plain dropDuplicates keeps an arbitrary
  row, so rows whose TransactionID already came from a higher-priority source are removed
  first; dropDuplicates then handles duplicates within a single source.
* The SQL Server FKs made Talend skip rows whose AccountID/BranchID is not in the dims.
  Delta does not enforce FKs, so those orphan rows are filtered here
  (``reject_orphans=true``). The dimension loads must run first.
"""

import os
import sys
from functools import reduce

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType  # noqa: E402

from common import get_param, get_spark, qualified, read_jdbc_table, write_delta  # noqa: E402

SOURCE_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
]
TIMESTAMP_FORMATS = ["dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"]


def _to_timestamp(df: DataFrame, column: str):
    if not isinstance(df.schema[column].dataType, StringType):
        return F.col(column).cast("timestamp")
    trimmed = F.trim(F.col(column))
    return F.coalesce(
        *[F.expr(f"try_to_timestamp(trim(`{column}`), '{fmt}')") for fmt in TIMESTAMP_FORMATS],
        trimmed.cast("timestamp"),
    )


def normalize(df: DataFrame, source: str) -> DataFrame:
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    return df.select(
        F.col("transaction_id").cast("int").alias("transaction_id"),
        F.col("account_id").cast("int").alias("account_id"),
        _to_timestamp(df, "transaction_date").alias("transaction_date"),
        F.col("amount").cast("decimal(19,4)").alias("amount"),
        F.trim(F.col("transaction_type")).alias("transaction_type"),
        F.col("branch_id").cast("int").alias("branch_id"),
    ).withColumn("_source", F.lit(source))


def read_sources(spark) -> list:
    sources = [
        normalize(read_jdbc_table(get_param("source_transaction_table", "transaction_db", spark), spark), "sqlserver")
    ]

    excel_path = get_param("excel_path", "", spark)
    if excel_path:
        excel = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dataAddress", get_param("excel_data_address", "'Sheet1'!A1", spark))
            .load(excel_path)
        )
        sources.append(normalize(excel, "excel"))
    else:
        print("excel_path not set; skipping the Excel transaction source")

    csv_path = get_param("csv_path", "", spark)
    if csv_path:
        csv = spark.read.option("header", "true").option("encoding", "ISO-8859-15").csv(csv_path)
        sources.append(normalize(csv, "csv"))
    else:
        print("csv_path not set; skipping the CSV transaction source")
    return sources


def dedupe_first_wins(sources: list) -> DataFrame:
    """tUniqRow: keep the first row per transaction_id across sources in priority order."""
    kept = []
    seen = None
    for df in sources:
        if seen is not None:
            df = df.join(seen, "transaction_id", "left_anti")
        kept.append(df)
        keys = df.select("transaction_id")
        seen = keys if seen is None else seen.unionByName(keys)
    united = reduce(lambda a, b: a.unionByName(b), kept)
    return united.dropDuplicates(["transaction_id"])


def main() -> None:
    spark = get_spark()
    dim_account = spark.table(qualified("dim_account", spark))
    dim_branch = spark.table(qualified("dim_branch", spark))
    if dim_account.isEmpty() or dim_branch.isEmpty():
        raise RuntimeError("dim_account / dim_branch are empty: run the dimension loads first")

    sources = read_sources(spark)
    union_count = reduce(lambda a, b: a.unionByName(b), sources).count()
    deduped = dedupe_first_wins(sources).filter(F.col("transaction_id").isNotNull())
    deduped_count = deduped.count()

    if get_param("reject_orphans", "true", spark).lower() == "true":
        valid = deduped.join(
            dim_account.select(F.col("AccountID").alias("account_id")), "account_id", "left_semi"
        ).join(dim_branch.select(F.col("BranchID").alias("branch_id")), "branch_id", "left_semi")
        orphans = deduped.join(valid.select("transaction_id"), "transaction_id", "left_anti")
        orphan_ids = sorted(r.transaction_id for r in orphans.select("transaction_id").collect())
        if orphan_ids:
            print(f"Rejected {len(orphan_ids)} rows with unknown AccountID/BranchID: {orphan_ids}")
        deduped = valid

    print(f"Transactions: {union_count} unioned -> {deduped_count} after dedupe -> {deduped.count()} to load")

    df = deduped.select(
        F.col("transaction_id").alias("TransactionID"),
        F.col("account_id").alias("AccountID"),
        F.col("transaction_date").alias("TransactionDate"),
        F.col("amount").alias("Amount"),
        F.col("transaction_type").alias("TransactionType"),
        F.col("branch_id").alias("BranchID"),
    )
    write_delta(df, qualified("fact_transaction", spark), key="TransactionID", spark=spark)


if __name__ == "__main__":
    main()
