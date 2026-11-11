"""Databricks port of the Talend job ``Load_DimAccount`` (IDX_INTERNSHIP, v0.1).

Talend flow: ``tMSSqlInput`` (sample.dbo.account) -> ``tMap_1`` -> ``tMSSqlOutput`` (DWH.DimAccount).

Rules carried over from the job definition (``Load_DimAccount_0.1.item``):

* R1 source: ``SELECT account_id, customer_id, account_type, balance, date_opened, status
  FROM dbo.account`` with no WHERE clause -> ``<catalog>.<bronze>.account``, all rows.
* R2 tMap: every output column is a straight ``row1.<col>`` copy (no expressions, no
  filter, empty Var table); column names follow the gold snake_case convention.
* R3 types (tMap / target schema): account_id, customer_id are ``id_Integer``;
  balance is ``id_Integer`` in the flow and lands in the ``MONEY`` column of DimAccount
  (stored here as DECIMAL(19,4)); date_opened is ``id_Date`` (source DATETIME2) landing
  in a ``DATE`` column, so the time part is dropped.
* R4 no trimming: ``TRIM_ALL_COLUMN=false`` and every ``TRIM_COLUMN`` entry is false.
* R5 key: AccountID is ``key=true nullable=false`` and the DimAccount PRIMARY KEY; with
  ``DIE_ON_ERROR=false`` rows violating it (NULL or duplicate key) are rejected and the
  job keeps going, so they are dropped here (one row kept per account_id).
* R6 load: ``TABLE_ACTION=CREATE_IF_NOT_EXISTS`` -> CREATE TABLE IF NOT EXISTS;
  ``DATA_ACTION=INSERT`` -> Delta MERGE on account_id. ``upsert`` (default) also updates
  changed rows; ``insert_only`` mirrors Talend exactly (existing keys are left untouched).
  Nothing is ever deleted from the target.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE_TABLE = "account"
TARGET_TABLE = "dim_account"
KEY_COLUMN = "account_id"
LOAD_MODES = ("upsert", "insert_only")

# Optional bronze ingestion-time columns used to keep the latest copy of a key.
INGESTION_ORDER_COLUMNS = ("_ingested_at", "_ingestion_timestamp", "_load_ts")

TARGET_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.IntegerType(), nullable=False),
        T.StructField("customer_id", T.IntegerType(), nullable=True),
        T.StructField("account_type", T.StringType(), nullable=True),
        T.StructField("balance", T.DecimalType(19, 4), nullable=True),
        T.StructField("date_opened", T.DateType(), nullable=True),
        T.StructField("status", T.StringType(), nullable=True),
    ]
)
TARGET_COLUMNS = [f.name for f in TARGET_SCHEMA.fields]
NON_KEY_COLUMNS = [c for c in TARGET_COLUMNS if c != KEY_COLUMN]

DATE_STRING_FORMATS = ("yyyy-MM-dd HH:mm:ss.SSSSSSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd-MM-yyyy")


@dataclass(frozen=True)
class DimAccountConfig:
    catalog: str = "banking_dwh"
    bronze_schema: str = "bronze"
    gold_schema: str = "gold"
    source_table: str = SOURCE_TABLE
    target_table: str = TARGET_TABLE
    load_mode: str = "upsert"

    def __post_init__(self) -> None:
        if self.load_mode not in LOAD_MODES:
            raise ValueError(f"load_mode must be one of {LOAD_MODES}, got {self.load_mode!r}")

    @property
    def source_fqn(self) -> str:
        return _fqn(self.catalog, self.bronze_schema, self.source_table)

    @property
    def target_fqn(self) -> str:
        return _fqn(self.catalog, self.gold_schema, self.target_table)


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fqn(*parts: str) -> str:
    return ".".join(_quote(p) for p in parts if p)


def _try_cast(column: str, sql_type: str) -> Column:
    # F.expr keeps this Spark Connect (serverless) compatible.
    return F.expr(f"try_cast({_quote(column)} AS {sql_type})")


def _to_date(df: DataFrame, column: str) -> Column:
    dtype = df.schema[column].dataType
    if isinstance(dtype, (T.DateType, T.TimestampType, T.TimestampNTZType)):
        return F.to_date(F.col(column))
    quoted = _quote(column)
    parsed = [F.expr(f"try_to_timestamp(trim(CAST({quoted} AS STRING)), '{fmt}')") for fmt in DATE_STRING_FORMATS]
    return F.to_date(F.coalesce(*parsed))


def read_source(spark: SparkSession, config: DimAccountConfig) -> DataFrame:
    """R1: the Talend query reads the whole account table without a filter."""
    return spark.table(config.source_fqn)


def transform(source: DataFrame) -> DataFrame:
    """R2-R5: the tMap pass-through plus the target's type and key constraints."""
    ordering_cols = [c for c in INGESTION_ORDER_COLUMNS if c in source.columns]

    mapped = source.select(
        _try_cast("account_id", "INT").alias("account_id"),
        _try_cast("customer_id", "INT").alias("customer_id"),
        F.col("account_type").cast("string").alias("account_type"),
        _try_cast("balance", "INT").cast(T.DecimalType(19, 4)).alias("balance"),
        _to_date(source, "date_opened").alias("date_opened"),
        F.col("status").cast("string").alias("status"),
        *[F.col(c) for c in ordering_cols],
    ).where(F.col(KEY_COLUMN).isNotNull())

    order_by = [F.col(c).desc_nulls_last() for c in ordering_cols] + [
        F.col(c).asc_nulls_last() for c in NON_KEY_COLUMNS
    ]
    ranked = mapped.withColumn("_rn", F.row_number().over(Window.partitionBy(KEY_COLUMN).orderBy(*order_by)))
    return ranked.where(F.col("_rn") == 1).select(*TARGET_COLUMNS)


def ensure_target(spark: SparkSession, config: DimAccountConfig) -> None:
    """R6: TABLE_ACTION=CREATE_IF_NOT_EXISTS."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_fqn(config.catalog, config.gold_schema)}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.target_fqn} (
            account_id INT NOT NULL,
            customer_id INT,
            account_type STRING,
            balance DECIMAL(19,4),
            date_opened DATE,
            status STRING
        ) USING DELTA
        COMMENT 'Account dimension, migrated from Talend job Load_DimAccount'
        """
    )


def merge_into_target(spark: SparkSession, rows: DataFrame, config: DimAccountConfig) -> None:
    """R6: DATA_ACTION=INSERT, made re-runnable as a MERGE on account_id."""
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, config.target_fqn)
    merge = target.alias("t").merge(rows.alias("s"), f"t.{KEY_COLUMN} = s.{KEY_COLUMN}")
    if config.load_mode == "upsert":
        changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in NON_KEY_COLUMNS)
        merge = merge.whenMatchedUpdate(condition=changed, set={c: f"s.{c}" for c in NON_KEY_COLUMNS})
    merge.whenNotMatchedInsert(values={c: f"s.{c}" for c in TARGET_COLUMNS}).execute()


def run(spark: SparkSession, config: DimAccountConfig) -> DataFrame:
    rows = transform(read_source(spark, config))
    ensure_target(spark, config)
    merge_into_target(spark, rows, config)
    return spark.table(config.target_fqn)


def parse_args(argv: list[str] | None = None) -> DimAccountConfig:
    parser = argparse.ArgumentParser(description="Load gold.dim_account from bronze.account")
    defaults = DimAccountConfig()
    parser.add_argument("--catalog", default=defaults.catalog)
    parser.add_argument("--bronze-schema", default=defaults.bronze_schema)
    parser.add_argument("--gold-schema", default=defaults.gold_schema)
    parser.add_argument("--source-table", default=defaults.source_table)
    parser.add_argument("--target-table", default=defaults.target_table)
    parser.add_argument("--load-mode", default=defaults.load_mode, choices=LOAD_MODES)
    args = parser.parse_args(argv)
    return DimAccountConfig(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        gold_schema=args.gold_schema,
        source_table=args.source_table,
        target_table=args.target_table,
        load_mode=args.load_mode,
    )


def main(argv: list[str] | None = None) -> None:
    config = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    result = run(spark, config)
    print(f"{config.target_fqn}: {result.count()} rows after {config.load_mode} merge")


if __name__ == "__main__":
    main()
