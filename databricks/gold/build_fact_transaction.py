# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: `dwh.gold.fact_transaction`
# MAGIC
# MAGIC Databricks replacement for the Talend job `Load_FactTransaction`
# MAGIC (`talend_jobs/Load_FactTransaction.zip` ->
# MAGIC `IDX_INTERNSHIP/process/Load_FactTransaction_0.1.item`).
# MAGIC
# MAGIC ## Legacy component graph (authoritative, from the `.item` XML)
# MAGIC
# MAGIC ```
# MAGIC tMSSqlInput (transaction_db)   --row1, mergeOrder=1--\
# MAGIC tFileInputExcel (Sheet1)       --row2, mergeOrder=2---> tUnite_1 --> tUniqRow_1 --> tMap_1 --> tMSSqlOutput (DWH.FactTransaction)
# MAGIC tFileInputDelimited (csv)      --row3, mergeOrder=3--/
# MAGIC ```
# MAGIC
# MAGIC * `tMSSqlInput` query: `SELECT transaction_id, account_id, transaction_date, amount,
# MAGIC   transaction_type, branch_id FROM dbo.transaction_db` (no WHERE clause -> no source filtering).
# MAGIC * `tFileInputExcel`: `transaction_excel.xlsx`, sheet `Sheet1`, 1 header row, date pattern
# MAGIC   `dd-MM-yyyy HH:mm:ss`.
# MAGIC * `tFileInputDelimited`: `transaction_csv.csv`, field separator `,`, 1 header row, date pattern
# MAGIC   `dd-MM-yyyy HH:mm:ss`, `REMOVE_EMPTY_ROW=true`.
# MAGIC * `tUniqRow_1`: unique key = `transaction_id` only (all other columns `KEY_ATTRIBUTE=false`),
# MAGIC   `ONLY_ONCE_EACH_DUPLICATED_KEY=false`. Talend emits the **first** row seen for a key on the
# MAGIC   `UNIQUE` flow and discards the rest, so the winner is decided by the `tUnite` merge order.
# MAGIC * `tMap_1`: straight passthrough with a rename to the DWH column names
# MAGIC   (`transaction_id -> TransactionID`, ...). No filter, no lookup, no derived expression.
# MAGIC * `tMSSqlOutput`: `DWH.FactTransaction`, `TABLE_ACTION=TRUNCATE`, `DATA_ACTION=INSERT`
# MAGIC   (i.e. the legacy job is a full reload every run).
# MAGIC
# MAGIC ## What changes on Databricks
# MAGIC
# MAGIC | Talend | Databricks |
# MAGIC | --- | --- |
# MAGIC | `tUnite` (positional merge, identical schemas required) | explicit per-source `select` + cast, then `unionByName` |
# MAGIC | `tUniqRow` "keep first row seen" (order = merge order, otherwise arbitrary) | `row_number()` over a **documented** deterministic ordering |
# MAGIC | `TRUNCATE` + `INSERT` | idempotent `MERGE INTO` on `transaction_id` (full overwrite still available via widget) |
# MAGIC | FK constraints enforced by SQL Server | Delta FKs are informational only -> explicit RI check + `dwh.gold.fact_transaction_rejects` quarantine |
# MAGIC
# MAGIC Output schema (ticket 3): `transaction_id INT`, `account_id INT`, `transaction_date TIMESTAMP`,
# MAGIC `amount DECIMAL(19,4)`, `transaction_type STRING`, `branch_id INT`.

# COMMAND ----------

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

CATALOG = "dwh"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Bronze source tables (ticket 4/5 ingestion). `transaction_db` is the SQL Server source table
# named in the tMSSqlInput query (`FROM dbo.transaction_db`), snake_cased per the shared convention.
SQL_SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.transaction_db"
CSV_SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.transaction_csv"
EXCEL_SOURCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.transaction_excel"

DIM_ACCOUNT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_account"
DIM_BRANCH_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.dim_branch"

TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fact_transaction"
REJECTS_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.fact_transaction_rejects"

AMOUNT_TYPE = DecimalType(19, 4)

# Talend read both the CSV and the Excel sheet with this java date pattern; the Spark equivalent
# uses the same field letters (`dd-MM-yyyy HH:mm:ss`), so the literal is reused as-is.
LEGACY_DATE_PATTERN = "dd-MM-yyyy HH:mm:ss"

FACT_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
]

FACT_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), False),
        StructField("account_id", IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", AMOUNT_TYPE, True),
        StructField("transaction_type", StringType(), True),
        StructField("branch_id", IntegerType(), True),
    ]
)

# Source priority reproduces the tUnite merge order recorded in the .item XML
# (row1 = SQL mergeOrder 1, row2 = Excel mergeOrder 2, row3 = CSV mergeOrder 3), which is what
# decided which row tUniqRow kept for a duplicated transaction_id.
SOURCE_PRIORITY: Dict[str, int] = {
    "sql_server": 1,
    "excel": 2,
    "csv": 3,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-source normalization
# MAGIC
# MAGIC `tUnite` requires the three inputs to already share a schema; in the legacy job that was
# MAGIC achieved implicitly because each input component carried its own hand-maintained metadata
# MAGIC (SQL: `INT`/`DATETIME2`/`VARCHAR`; CSV: everything read as text and parsed with a date
# MAGIC pattern; Excel: POI cell types). Bronze preserves those raw source types, so each source is
# MAGIC normalized **explicitly before** the union rather than relying on column position.
# MAGIC
# MAGIC Every normalized frame carries three lineage columns used downstream by the dedup:
# MAGIC `_source_system`, `_source_priority` and `_ingested_at`.

# COMMAND ----------


def _to_timestamp(col: Column, date_pattern: str = LEGACY_DATE_PATTERN) -> Column:
    """Parse a transaction date that may already be a timestamp or a `dd-MM-yyyy HH:mm:ss` string.

    Bronze keeps the source representation: the JDBC source lands a real `DATETIME2`, the Excel
    source lands either a timestamp (POI date cell) or text, and the CSV source always lands text.
    A value that is already a timestamp fails the legacy pattern and falls through to the cast.
    """
    return F.coalesce(
        F.try_to_timestamp(col.cast(StringType()), F.lit(date_pattern)),
        col.cast(TimestampType()),
    )


def _with_lineage(df: DataFrame, source_system: str, ingested_at_col: Optional[str]) -> DataFrame:
    """Attach `_source_system`, `_source_priority` and `_ingested_at` to a normalized frame."""
    if source_system not in SOURCE_PRIORITY:
        raise ValueError(f"unknown source system: {source_system}")
    if ingested_at_col is not None and ingested_at_col in df.columns:
        ingested_at = F.col(ingested_at_col).cast(TimestampType())
    else:
        # Bronze tables built by tickets 4/5 carry `_ingested_at`; when a source is replayed from a
        # frame that lacks it, fall back to a null so the dedup ordering degrades to source priority
        # plus the transaction_id tie-break rather than failing.
        ingested_at = F.lit(None).cast(TimestampType())
    return df.withColumn("_source_system", F.lit(source_system)).withColumn(
        "_source_priority", F.lit(SOURCE_PRIORITY[source_system]).cast(IntegerType())
    ).withColumn("_ingested_at", ingested_at)


def normalize_sql_source(df: DataFrame, ingested_at_col: str = "_ingested_at") -> DataFrame:
    """Normalize `dwh.bronze.transaction_db` (tMSSqlInput).

    Column names already match the fact schema (the tMSSqlInput query selects them verbatim);
    only the types need aligning: `INT amount` -> `DECIMAL(19,4)` and `DATETIME2` -> `TIMESTAMP`.
    """
    normalized = df.select(
        F.col("transaction_id").cast(IntegerType()).alias("transaction_id"),
        F.col("account_id").cast(IntegerType()).alias("account_id"),
        _to_timestamp(F.col("transaction_date")).alias("transaction_date"),
        F.col("amount").cast(AMOUNT_TYPE).alias("amount"),
        F.col("transaction_type").cast(StringType()).alias("transaction_type"),
        F.col("branch_id").cast(IntegerType()).alias("branch_id"),
        *( [F.col(ingested_at_col)] if ingested_at_col in df.columns else [] ),
    )
    return _with_lineage(normalized, "sql_server", ingested_at_col).select(
        *FACT_COLUMNS, "_source_system", "_source_priority", "_ingested_at"
    )


def normalize_csv_source(df: DataFrame, ingested_at_col: str = "_ingested_at") -> DataFrame:
    """Normalize `dwh.bronze.transaction_csv` (tFileInputDelimited).

    The CSV header is `transaction_id,account_id,transaction_date,amount,transaction_type,branch_id`
    and bronze lands every column as a string (schema-on-read of a raw file), so all six columns are
    cast here. `transaction_date` uses the legacy `dd-MM-yyyy HH:mm:ss` pattern
    (e.g. `21-01-2024 14:00:00`), which is *not* the Spark default and would silently parse to null
    without the explicit pattern.
    """
    normalized = df.select(
        F.col("transaction_id").cast(IntegerType()).alias("transaction_id"),
        F.col("account_id").cast(IntegerType()).alias("account_id"),
        _to_timestamp(F.col("transaction_date")).alias("transaction_date"),
        F.col("amount").cast(AMOUNT_TYPE).alias("amount"),
        F.trim(F.col("transaction_type").cast(StringType())).alias("transaction_type"),
        F.col("branch_id").cast(IntegerType()).alias("branch_id"),
        *( [F.col(ingested_at_col)] if ingested_at_col in df.columns else [] ),
    )
    return _with_lineage(normalized, "csv", ingested_at_col).select(
        *FACT_COLUMNS, "_source_system", "_source_priority", "_ingested_at"
    )


def normalize_excel_source(df: DataFrame, ingested_at_col: str = "_ingested_at") -> DataFrame:
    """Normalize `dwh.bronze.transaction_excel` (tFileInputExcel, `Sheet1`, 1 header row).

    The workbook header row matches the CSV header, but the Excel reader used in bronze (ticket 5)
    yields POI-typed cells: numeric columns arrive as `double`/`long` and `transaction_date` as a
    real timestamp. Numerics are cast down to `INT`/`DECIMAL(19,4)` and the date is passed through
    `_to_timestamp`, which tolerates both a timestamp and the legacy text pattern.
    """
    normalized = df.select(
        F.col("transaction_id").cast(IntegerType()).alias("transaction_id"),
        F.col("account_id").cast(IntegerType()).alias("account_id"),
        _to_timestamp(F.col("transaction_date")).alias("transaction_date"),
        F.col("amount").cast(AMOUNT_TYPE).alias("amount"),
        F.trim(F.col("transaction_type").cast(StringType())).alias("transaction_type"),
        F.col("branch_id").cast(IntegerType()).alias("branch_id"),
        *( [F.col(ingested_at_col)] if ingested_at_col in df.columns else [] ),
    )
    return _with_lineage(normalized, "excel", ingested_at_col).select(
        *FACT_COLUMNS, "_source_system", "_source_priority", "_ingested_at"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Union (`tUnite`)

# COMMAND ----------


def union_sources(frames: Iterable[DataFrame]) -> DataFrame:
    """Replacement for `tUnite_1`.

    Talend merged by column position; `unionByName` merges by name, which is safe because every
    input has already been projected onto the common fact schema above. Rows with a null
    `transaction_id` cannot participate in the dedup key or the MERGE, so they are dropped here and
    counted (Talend's schema marked `transaction_id` as non-nullable and would have failed the row).
    """
    frames = list(frames)
    if not frames:
        raise ValueError("union_sources requires at least one DataFrame")
    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame)
    return unioned


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deterministic dedup (`tUniqRow`)
# MAGIC
# MAGIC `tUniqRow_1` keys on `transaction_id` alone and keeps the **first** row it sees, so in the
# MAGIC legacy job the winner was whichever source the merge order put first (SQL, then Excel, then
# MAGIC CSV) — deterministic only as a side effect of the flow layout, and undefined for duplicates
# MAGIC *within* one source.
# MAGIC
# MAGIC `DataFrame.dropDuplicates("transaction_id")` would keep an **arbitrary** row (it depends on
# MAGIC partitioning and task completion order), so this build instead uses an explicit
# MAGIC `row_number()` over a documented total ordering:
# MAGIC
# MAGIC 1. `_source_priority` ascending — reproduces the tUnite merge order (SQL 1, Excel 2, CSV 3);
# MAGIC 2. `_ingested_at` descending, nulls last — within one source the freshest ingestion wins;
# MAGIC 3. `transaction_date` descending, nulls last, then `amount` descending, nulls last — final
# MAGIC    tie-break so two rows loaded in the same bronze batch still resolve identically on a rerun.
# MAGIC
# MAGIC Conflicting duplicates (same `transaction_id`, differing `amount`) are counted and logged
# MAGIC rather than hidden, because they indicate a genuine source disagreement a human must arbitrate.

# COMMAND ----------

def dedup_ordering() -> List[Column]:
    """The documented total ordering that decides which duplicate row survives."""
    return [
        F.col("_source_priority").asc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("transaction_date").desc_nulls_last(),
        F.col("amount").desc_nulls_last(),
    ]


def dedup_transactions(df: DataFrame) -> DataFrame:
    """Keep exactly one row per `transaction_id` using `dedup_ordering()`."""
    window = Window.partitionBy("transaction_id").orderBy(*dedup_ordering())
    return (
        df.withColumn("_dedup_rank", F.row_number().over(window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def duplicate_stats(df: DataFrame) -> Dict[str, int]:
    """Counts for the dedup log: rows in, distinct ids, rows dropped, conflicting ids.

    A *conflicting* duplicate is a `transaction_id` whose rows do not all agree on the business
    payload (`amount`, `account_id`, `transaction_date`, `transaction_type`, `branch_id`); those are
    the ones where picking a winner actually changes the fact table.
    """
    agg = df.groupBy("transaction_id").agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(
            F.concat_ws(
                "|",
                F.col("amount").cast(StringType()),
                F.col("account_id").cast(StringType()),
                F.col("transaction_date").cast(StringType()),
                F.col("transaction_type"),
                F.col("branch_id").cast(StringType()),
            )
        ).alias("distinct_payloads"),
    )
    summary = agg.select(
        F.sum("row_count").alias("input_rows"),
        F.count(F.lit(1)).alias("distinct_ids"),
        F.sum(F.when(F.col("row_count") > 1, 1).otherwise(0)).alias("duplicated_ids"),
        F.sum(F.when(F.col("distinct_payloads") > 1, 1).otherwise(0)).alias("conflicting_ids"),
    ).collect()[0]
    input_rows = int(summary["input_rows"] or 0)
    distinct_ids = int(summary["distinct_ids"] or 0)
    return {
        "input_rows": input_rows,
        "distinct_ids": distinct_ids,
        "duplicate_rows_dropped": input_rows - distinct_ids,
        "duplicated_ids": int(summary["duplicated_ids"] or 0),
        "conflicting_ids": int(summary["conflicting_ids"] or 0),
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential integrity + quarantine
# MAGIC
# MAGIC The Delta foreign keys declared in ticket 3 are **informational only** — Databricks does not
# MAGIC enforce them, unlike the SQL Server `FK_FactTransaction_DimAccount` /
# MAGIC `FK_FactTransaction_DimBranch` constraints that would have aborted the Talend load. The check
# MAGIC is therefore done explicitly here, and violating rows are routed to
# MAGIC `dwh.gold.fact_transaction_rejects` with a `reject_reason` instead of being dropped silently
# MAGIC (data loss) or failing the whole job (an unusable pipeline for one bad row).
# MAGIC
# MAGIC A null `account_id`/`branch_id` is *not* a violation: those columns are nullable in the DDL
# MAGIC and SQL Server does not enforce a FK on a null value.

# COMMAND ----------

REJECT_COLUMNS = FACT_COLUMNS + ["_source_system", "reject_reason"]


def split_referential_integrity(
    df: DataFrame, dim_account: DataFrame, dim_branch: DataFrame
) -> Dict[str, DataFrame]:
    """Split a deduplicated fact frame into `valid` rows and RI `rejects`."""
    accounts = dim_account.select(F.col("account_id").cast(IntegerType()).alias("_dim_account_id")).distinct()
    branches = dim_branch.select(F.col("branch_id").cast(IntegerType()).alias("_dim_branch_id")).distinct()

    checked = (
        df.join(accounts, df["account_id"] == F.col("_dim_account_id"), "left")
        .join(branches, df["branch_id"] == F.col("_dim_branch_id"), "left")
        .withColumn(
            "_account_missing",
            F.col("account_id").isNotNull() & F.col("_dim_account_id").isNull(),
        )
        .withColumn(
            "_branch_missing",
            F.col("branch_id").isNotNull() & F.col("_dim_branch_id").isNull(),
        )
    )

    reject_reason = F.concat_ws(
        "; ",
        F.when(F.col("_account_missing"), F.concat(F.lit("account_id not found in "), F.lit(DIM_ACCOUNT_TABLE))),
        F.when(F.col("_branch_missing"), F.concat(F.lit("branch_id not found in "), F.lit(DIM_BRANCH_TABLE))),
    )

    is_reject = F.col("_account_missing") | F.col("_branch_missing")
    valid = checked.filter(~is_reject).select(*FACT_COLUMNS, "_source_system", "_source_priority", "_ingested_at")
    rejects = (
        checked.filter(is_reject)
        .withColumn("reject_reason", reject_reason)
        .withColumn("rejected_at", F.current_timestamp())
        .select(*REJECT_COLUMNS, "rejected_at")
    )
    return {"valid": valid, "rejects": rejects}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build pipeline (pure, testable)

# COMMAND ----------


def build_fact_transaction(
    sql_df: DataFrame,
    csv_df: DataFrame,
    excel_df: DataFrame,
    dim_account: DataFrame,
    dim_branch: DataFrame,
) -> Dict[str, object]:
    """Full transformation: normalize -> union -> dedup -> RI split.

    Returns `{"valid": DataFrame, "rejects": DataFrame, "stats": dict}`. No I/O happens here so the
    whole pipeline is unit-testable against a local SparkSession.
    """
    unioned = union_sources(
        [
            normalize_sql_source(sql_df),
            normalize_excel_source(excel_df),
            normalize_csv_source(csv_df),
        ]
    )
    keyed = unioned.filter(F.col("transaction_id").isNotNull())
    stats = duplicate_stats(keyed)
    stats["rows_missing_transaction_id"] = unioned.count() - keyed.count()

    split = split_referential_integrity(dedup_transactions(keyed), dim_account, dim_branch)
    stats["reject_rows"] = split["rejects"].count()
    stats["valid_rows"] = split["valid"].count()
    return {"valid": split["valid"], "rejects": split["rejects"], "stats": stats}


def to_target_schema(df: DataFrame) -> DataFrame:
    """Project onto the exact ticket-3 gold schema (drops the lineage helper columns)."""
    return df.select(*FACT_COLUMNS)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write
# MAGIC
# MAGIC **Partitioning / ZORDER.** The fact table is small (thousands of rows for this dataset, and a
# MAGIC low-millions ceiling for the modelled bank), which is well under the point where Hive-style
# MAGIC partitioning helps — partitioning by e.g. `transaction_date` month would produce many tiny
# MAGIC files and slow the MERGE down. The table is therefore left **unpartitioned** and instead
# MAGIC `ZORDER BY (transaction_id, transaction_date)`: `transaction_id` is the MERGE key, so
# MAGIC clustering on it maximises file skipping during the merge, and `transaction_date` is the
# MAGIC dominant analytical predicate (`sp_DailyTransaction` filters a date range). Revisit with
# MAGIC `PARTITIONED BY (DATE_TRUNC('MONTH', transaction_date))` only once the table exceeds ~1 TB.

# COMMAND ----------


def merge_fact_transaction(spark: SparkSession, df: DataFrame, target_table: str = TARGET_TABLE) -> None:
    """Idempotent upsert of the gold fact table on `transaction_id`."""
    source_view = "_fact_transaction_source"
    to_target_schema(df).createOrReplaceTempView(source_view)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS tgt
        USING {source_view} AS src
          ON tgt.transaction_id = src.transaction_id
        WHEN MATCHED THEN UPDATE SET
          tgt.account_id       = src.account_id,
          tgt.transaction_date = src.transaction_date,
          tgt.amount           = src.amount,
          tgt.transaction_type = src.transaction_type,
          tgt.branch_id        = src.branch_id
        WHEN NOT MATCHED THEN INSERT (
          transaction_id, account_id, transaction_date, amount, transaction_type, branch_id
        ) VALUES (
          src.transaction_id, src.account_id, src.transaction_date, src.amount,
          src.transaction_type, src.branch_id
        )
        """
    )


def overwrite_fact_transaction(df: DataFrame, target_table: str = TARGET_TABLE) -> None:
    """Full reload, matching the legacy `TABLE_ACTION=TRUNCATE` + `DATA_ACTION=INSERT` behaviour."""
    to_target_schema(df).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target_table)


def write_rejects(df: DataFrame, rejects_table: str = REJECTS_TABLE) -> None:
    """Replace the quarantine table with this run's referential-integrity violations."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(rejects_table)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook entrypoint

# COMMAND ----------


def main() -> None:  # pragma: no cover - requires a Databricks runtime
    spark = SparkSession.builder.getOrCreate()

    dbutils.widgets.dropdown("write_mode", "merge", ["merge", "overwrite"], "Write mode")
    dbutils.widgets.text("source_catalog", CATALOG, "Source catalog")
    write_mode = dbutils.widgets.get("write_mode")
    catalog = dbutils.widgets.get("source_catalog")

    sql_df = spark.table(f"{catalog}.{BRONZE_SCHEMA}.transaction_db")
    csv_df = spark.table(f"{catalog}.{BRONZE_SCHEMA}.transaction_csv")
    excel_df = spark.table(f"{catalog}.{BRONZE_SCHEMA}.transaction_excel")
    dim_account = spark.table(f"{catalog}.{SILVER_SCHEMA}.dim_account")
    dim_branch = spark.table(f"{catalog}.{SILVER_SCHEMA}.dim_branch")

    result = build_fact_transaction(sql_df, csv_df, excel_df, dim_account, dim_branch)
    valid, rejects, stats = result["valid"], result["rejects"], result["stats"]
    valid.cache()

    print(
        "fact_transaction dedup: "
        f"{stats['input_rows']} rows in, {stats['distinct_ids']} distinct transaction_id, "
        f"{stats['duplicate_rows_dropped']} duplicate rows dropped across "
        f"{stats['duplicated_ids']} duplicated ids, "
        f"{stats['conflicting_ids']} ids with conflicting payloads (same id, different amount/"
        "account/date/type/branch), "
        f"{stats['rows_missing_transaction_id']} rows dropped for a null transaction_id"
    )
    if stats["conflicting_ids"]:
        print(
            "WARNING: conflicting duplicates present - the winner was chosen by source priority "
            "(sql_server > excel > csv), then newest _ingested_at. Winning rows:"
        )
        valid.select("transaction_id", "_source_system", "amount", "transaction_date").orderBy(
            "transaction_id"
        ).limit(20).show(truncate=False)

    print(f"referential integrity: {stats['valid_rows']} valid rows, {stats['reject_rows']} quarantined")
    write_rejects(rejects)

    if write_mode == "overwrite":
        overwrite_fact_transaction(valid)
    else:
        merge_fact_transaction(spark, valid)

    spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (transaction_id, transaction_date)")
    valid.unpersist()


# COMMAND ----------

if __name__ == "__main__" and "dbutils" in dir():  # pragma: no cover
    main()
