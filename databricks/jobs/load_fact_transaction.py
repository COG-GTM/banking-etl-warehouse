# Databricks notebook source
# MAGIC %md
# MAGIC # Load_FactTransaction (Talend -> PySpark)
# MAGIC
# MAGIC Migration of the Talend `Load_FactTransaction` job:
# MAGIC
# MAGIC | Talend component      | PySpark equivalent                                         |
# MAGIC |-----------------------|------------------------------------------------------------|
# MAGIC | `tMSSqlInput`         | `spark.read.format("jdbc")` on `sample.dbo.transaction_db` |
# MAGIC | `tFileInputExcel`     | `com.crealytics.spark.excel` (pandas fallback)             |
# MAGIC | `tFileInputDelimited` | `spark.read.csv` + `to_timestamp(..., "dd-MM-yyyy HH:mm:ss")` |
# MAGIC | `tUnite`              | `unionByName` after schema standardisation                 |
# MAGIC | `tUniqRow`            | `dropDuplicates(["TransactionID"])`                        |
# MAGIC | `tMap` + `tMSSqlOutput` | rename to DWH columns + Delta `MERGE` into `dwh.fact_transaction` |
# MAGIC
# MAGIC `FactTransaction` has FKs to `DimAccount`/`DimBranch`, and the Talend output runs with
# MAGIC "die on error" off, so SQL Server silently drops orphan rows. Delta does not enforce FKs,
# MAGIC so `reject_orphans` does the same filter explicitly against `dwh.dim_account`/`dwh.dim_branch`.
# MAGIC
# MAGIC Target schema mirrors `FactTransaction` in `sql_scripts/01_create_tables.sql`.

# COMMAND ----------

import logging
import os
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("load_fact_transaction")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

KEY_COLUMN = "TransactionID"
SOURCE_PRIORITY_COL = "_source_priority"
CSV_DATE_FORMAT = "dd-MM-yyyy HH:mm:ss"

# FactTransaction DDL: INT, INT, DATETIME, MONEY (= DECIMAL(19,4)), VARCHAR(50), INT
FACT_TRANSACTION_SCHEMA = StructType(
    [
        StructField("TransactionID", IntegerType(), nullable=False),
        StructField("AccountID", IntegerType(), nullable=True),
        StructField("TransactionDate", TimestampType(), nullable=True),
        StructField("Amount", DecimalType(19, 4), nullable=True),
        StructField("TransactionType", StringType(), nullable=True),
        StructField("BranchID", IntegerType(), nullable=True),
    ]
)
FACT_COLUMNS = [f.name for f in FACT_TRANSACTION_SCHEMA.fields]

# Source (snake_case) -> DWH (PascalCase), as done by the Talend tMap.
SOURCE_TO_FACT_COLUMNS = {
    "transaction_id": "TransactionID",
    "account_id": "AccountID",
    "transaction_date": "TransactionDate",
    "amount": "Amount",
    "transaction_type": "TransactionType",
    "branch_id": "BranchID",
}

# tUnite feeds tUniqRow in this order and tUniqRow keeps the first row it sees,
# so on conflicting duplicates the SQL Server row wins, then Excel, then CSV.
SOURCE_PRIORITY = {"sqlserver": 1, "excel": 2, "csv": 3}


@dataclass
class JobConfig:
    jdbc_url: str
    jdbc_table: str
    secret_scope: str
    jdbc_user_key: str
    jdbc_password_key: str
    excel_path: str
    csv_path: str
    target_table: str
    dim_account_table: str = "dwh.dim_account"
    dim_branch_table: str = "dwh.dim_branch"


def _get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        return DBUtils(spark)
    except ImportError:
        return None


def _get_param(spark: SparkSession, name: str, default: str) -> str:
    """Databricks widget value if set, else env var (upper-cased), else default."""
    dbutils = _get_dbutils(spark)
    if dbutils is not None:
        try:
            dbutils.widgets.text(name, default)
            value = dbutils.widgets.get(name)
            if value:
                return value
        except Exception:  # noqa: BLE001 - widgets unavailable in jobs without params
            pass
    return os.environ.get(name.upper(), default)


def load_config(spark: SparkSession) -> JobConfig:
    return JobConfig(
        jdbc_url=_get_param(
            spark,
            "jdbc_url",
            "jdbc:sqlserver://localhost:1433;databaseName=sample;encrypt=true;trustServerCertificate=true",
        ),
        jdbc_table=_get_param(spark, "jdbc_table", "dbo.transaction_db"),
        secret_scope=_get_param(spark, "secret_scope", "banking-etl"),
        jdbc_user_key=_get_param(spark, "jdbc_user_key", "sqlserver-user"),
        jdbc_password_key=_get_param(spark, "jdbc_password_key", "sqlserver-password"),
        excel_path=_get_param(spark, "excel_path", "data_sources/transaction_excel.xlsx"),
        csv_path=_get_param(spark, "csv_path", "data_sources/transaction_csv.csv"),
        target_table=_get_param(spark, "target_table", "dwh.fact_transaction"),
        dim_account_table=_get_param(spark, "dim_account_table", "dwh.dim_account"),
        dim_branch_table=_get_param(spark, "dim_branch_table", "dwh.dim_branch"),
    )


def get_jdbc_credentials(spark: SparkSession, config: JobConfig) -> tuple[str, str]:
    """Credentials from the Databricks secret scope; env vars only outside Databricks."""
    dbutils = _get_dbutils(spark)
    if dbutils is not None:
        return (
            dbutils.secrets.get(config.secret_scope, config.jdbc_user_key),
            dbutils.secrets.get(config.secret_scope, config.jdbc_password_key),
        )
    user = os.environ.get("SQLSERVER_USER")
    password = os.environ.get("SQLSERVER_PASSWORD")
    if not user or not password:
        raise RuntimeError(
            "SQL Server credentials not found: configure the Databricks secret scope "
            f"'{config.secret_scope}' or set SQLSERVER_USER / SQLSERVER_PASSWORD locally."
        )
    return user, password


# COMMAND ----------

# MAGIC %md ## Extract + standardise

# COMMAND ----------


def standardize(df: DataFrame, source: str) -> DataFrame:
    """Rename to DWH column names, cast to the FactTransaction types and tag the source."""
    lowered = {c: c.strip().lower() for c in df.columns}
    df = df.select([F.col(f"`{orig}`").alias(new) for orig, new in lowered.items()])
    missing = set(SOURCE_TO_FACT_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{source}: missing expected columns {sorted(missing)}")

    types = {f.name: f.dataType for f in FACT_TRANSACTION_SCHEMA.fields}
    projected = []
    for src_col, fact_col in SOURCE_TO_FACT_COLUMNS.items():
        col = F.col(src_col)
        if fact_col == "TransactionType":
            col = F.trim(col.cast(StringType()))
        projected.append(col.cast(types[fact_col]).alias(fact_col))
    return df.select(*projected, F.lit(SOURCE_PRIORITY[source]).alias(SOURCE_PRIORITY_COL))


def read_sqlserver_transactions(spark: SparkSession, config: JobConfig) -> DataFrame:
    user, password = get_jdbc_credentials(spark, config)
    df = (
        spark.read.format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", config.jdbc_table)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )
    return standardize(df, "sqlserver")


def read_excel_transactions(spark: SparkSession, path: str) -> DataFrame:
    """spark-excel when the crealytics library is attached to the cluster, else pandas."""
    try:
        df = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dataAddress", "'Sheet1'!A1")
            .load(path)
        )
    except Exception as exc:  # noqa: BLE001 - library missing -> ClassNotFound / DATA_SOURCE_NOT_FOUND
        logger.info("spark-excel unavailable (%s); falling back to pandas", type(exc).__name__)
        import pandas as pd

        local_path = path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path
        pdf = pd.read_excel(local_path, sheet_name="Sheet1", dtype={"transaction_type": str})
        pdf["transaction_date"] = pd.to_datetime(pdf["transaction_date"])
        df = spark.createDataFrame(pdf)
    return standardize(df, "excel")


def read_csv_transactions(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(path)
    df = df.withColumn("transaction_date", F.to_timestamp("transaction_date", CSV_DATE_FORMAT))
    return standardize(df, "csv")


# COMMAND ----------

# MAGIC %md ## tUnite + tUniqRow

# COMMAND ----------


def union_sources(*frames: DataFrame) -> DataFrame:
    """Talend tUnite: frames are already standardised, so union strictly by column name."""
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def deduplicate(unioned: DataFrame) -> DataFrame:
    """Talend tUniqRow on TransactionID.

    dropDuplicates alone keeps an arbitrary row per key, while tUniqRow keeps the first
    row in tUnite order. Restrict each key to its highest-priority source first so the
    surviving row is deterministic, then dropDuplicates collapses the remaining copies.
    """
    winners = unioned.groupBy(KEY_COLUMN).agg(F.min(SOURCE_PRIORITY_COL).alias(SOURCE_PRIORITY_COL))
    return (
        unioned.join(winners, on=[KEY_COLUMN, SOURCE_PRIORITY_COL], how="inner")
        .dropDuplicates([KEY_COLUMN])
        .select(*FACT_COLUMNS)
    )


@dataclass
class DedupStats:
    rows_before: int
    rows_after: int
    distinct_keys: int
    fk_rejected: int = 0

    @property
    def duplicates_removed(self) -> int:
        return self.rows_before - self.rows_after


def validate_dedup(unioned: DataFrame, deduped: DataFrame) -> DedupStats:
    rows_before = unioned.count()
    rows_after = deduped.count()
    distinct_keys = unioned.select(KEY_COLUMN).distinct().count()
    stats = DedupStats(rows_before, rows_after, distinct_keys)

    per_source = {
        name: unioned.filter(F.col(SOURCE_PRIORITY_COL) == prio).count()
        for name, prio in SOURCE_PRIORITY.items()
    }
    logger.info("Rows per source before union: %s", per_source)
    logger.info(
        "Dedup on %s: %d rows before, %d rows after, %d duplicates removed",
        KEY_COLUMN,
        stats.rows_before,
        stats.rows_after,
        stats.duplicates_removed,
    )

    assert deduped.filter(F.col(KEY_COLUMN).isNull()).count() == 0, "Null TransactionID after dedup"
    assert rows_after == distinct_keys, f"Expected {distinct_keys} unique keys, got {rows_after} rows"
    assert (
        deduped.groupBy(KEY_COLUMN).count().filter("count > 1").count() == 0
    ), "Duplicate TransactionID survived dedup"
    assert rows_after <= rows_before
    return stats


def reject_orphans(spark: SparkSession, deduped: DataFrame, config: JobConfig) -> tuple[DataFrame, int]:
    """Emulate the FactTransaction FKs: drop rows whose AccountID/BranchID is not in the dimension.

    NULL foreign keys are kept, as SQL Server allows them. A dimension table that does not
    exist yet is skipped with a warning so the job can still run standalone.
    """
    valid = deduped
    for fk_col, dim_table in (("AccountID", config.dim_account_table), ("BranchID", config.dim_branch_table)):
        if not spark.catalog.tableExists(dim_table):
            logger.warning("%s not found; skipping %s FK check", dim_table, fk_col)
            continue
        dim_keys = spark.table(dim_table).select(F.col(fk_col).alias("_fk")).distinct()
        valid = (
            valid.join(dim_keys, valid[fk_col] == dim_keys["_fk"], "left")
            .filter(F.col(fk_col).isNull() | F.col("_fk").isNotNull())
            .drop("_fk")
        )

    valid = valid.cache()
    rejected = deduped.join(valid.select(KEY_COLUMN), KEY_COLUMN, "left_anti")
    rejected_count = rejected.count()
    if rejected_count:
        ids = sorted(r[KEY_COLUMN] for r in rejected.select(KEY_COLUMN).collect())
        logger.warning("Rejected %d rows with unknown AccountID/BranchID (FK): TransactionID %s", rejected_count, ids)
    return valid, rejected_count


# COMMAND ----------

# MAGIC %md ## Load into Delta `dwh.fact_transaction`

# COMMAND ----------


def ensure_target_table(spark: SparkSession, target_table: str) -> None:
    if "." in target_table:
        schema = target_table.rsplit(".", 1)[0]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            TransactionID   INT NOT NULL,
            AccountID       INT,
            TransactionDate TIMESTAMP,
            Amount          DECIMAL(19, 4),
            TransactionType STRING,
            BranchID        INT
        ) USING DELTA
        COMMENT 'Migrated from Talend Load_FactTransaction (DWH.dbo.FactTransaction)'
        """
    )


def merge_into_fact(spark: SparkSession, deduped: DataFrame, target_table: str) -> None:
    """Idempotent upsert on TransactionID: inserts new keys, updates only rows whose values changed."""
    from delta.tables import DeltaTable

    ensure_target_table(spark, target_table)
    changed_condition = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in FACT_COLUMNS if c != KEY_COLUMN)
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(deduped.alias("s"), f"t.{KEY_COLUMN} = s.{KEY_COLUMN}")
        .whenMatchedUpdateAll(condition=changed_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------


def run(spark: SparkSession, config: Optional[JobConfig] = None, sql_df: Optional[DataFrame] = None) -> DedupStats:
    config = config or load_config(spark)
    sql_df = sql_df if sql_df is not None else read_sqlserver_transactions(spark, config)
    unioned = union_sources(
        sql_df,
        read_excel_transactions(spark, config.excel_path),
        read_csv_transactions(spark, config.csv_path),
    ).cache()
    deduped = deduplicate(unioned).cache()

    stats = validate_dedup(unioned, deduped)
    loadable, stats.fk_rejected = reject_orphans(spark, deduped, config)
    merge_into_fact(spark, loadable, config.target_table)

    target_count = spark.table(config.target_table).count()
    logger.info("%s now holds %d rows", config.target_table, target_count)
    assert target_count >= stats.rows_after - stats.fk_rejected, "Target has fewer rows than the loaded batch"

    for df in (unioned, deduped, loadable):
        df.unpersist()
    return stats


if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())
