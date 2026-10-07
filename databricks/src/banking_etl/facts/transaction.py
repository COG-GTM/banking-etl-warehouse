"""Port of Talend ``Load_FactTransaction`` to Delta Lake.

Legacy job (talend_jobs/Load_FactTransaction.zip)::

    tDBInput(sample.dbo.transaction_db) --row1 (mergeOrder 1)--\
    tFileInputExcel(transaction_excel.xlsx) --row2 (mergeOrder 2)--> tUnite -> tUniqRow(transaction_id)
    tFileInputDelimited(transaction_csv.csv) --row3 (mergeOrder 3)--/          -> tMap -> tDBOutput
                                                                    (FactTransaction, TRUNCATE + INSERT)

Databricks mapping:

* :func:`build_silver` normalizes the three bronze streams to one typed schema, quarantines rows that
  fail type conversion or that Auto Loader flagged in ``_rescued_data`` (the file inputs ran with
  DIE_ON_ERROR=false, so Talend dropped them silently),
  ``unionByName`` (tUnite) and dedupes on ``transaction_id`` keeping the first row (tUniqRow).
* :func:`merge_gold` checks the FKs against ``gold.dim_account`` / ``gold.dim_branch`` (tDBOutput ran with
  DIE_ON_ERROR=false, so SQL Server FK violations were skipped row by row) and MERGEs into
  ``gold.fact_transaction``. ``delete_missing=True`` (default) also deletes target rows absent from silver,
  which reproduces TRUNCATE + INSERT as an idempotent upsert.

tUniqRow tie-break: tUniqRow keeps the first row it sees, and tUnite emits its inputs in mergeOrder.
Here "first" is ``source_rank`` (sqlserver=1, excel=2, csv=3), then the earliest bronze ingestion
timestamp when the bronze table has one, then the business columns ascending, so the result is deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCES: dict[str, int] = {"sqlserver": 1, "excel": 2, "csv": 3}

DATE_FORMATS: dict[str, tuple[str, ...]] = {
    "sqlserver": ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss"),
    "excel": ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "M/d/yy H:mm"),
    "csv": ("dd-MM-yyyy HH:mm:ss",),
}

BUSINESS_COLUMNS = ("transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id")

INGEST_ORDER_CANDIDATES = (
    "_source_row_number",
    "_row_number",
    "_ingested_at",
    "_ingest_ts",
    "_ingestion_ts",
    "_load_ts",
    "_loaded_at",
    "ingested_at",
    "_commit_timestamp",
)

AMOUNT_TYPE = T.DecimalType(19, 4)

FACT_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.IntegerType(), False),
        T.StructField("account_id", T.IntegerType(), True),
        T.StructField("transaction_date", T.TimestampType(), True),
        T.StructField("amount", AMOUNT_TYPE, True),
        T.StructField("transaction_type", T.StringType(), True),
        T.StructField("branch_id", T.IntegerType(), True),
    ]
)

REJECTS_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), True),
        T.StructField("stage", T.StringType(), False),
        T.StructField("reject_reason", T.StringType(), False),
        T.StructField("source_system", T.StringType(), True),
        T.StructField("transaction_id", T.IntegerType(), True),
        T.StructField("raw_record", T.StringType(), True),
        T.StructField("rejected_at", T.TimestampType(), True),
    ]
)


@dataclass(frozen=True)
class FactTransactionTables:
    bronze_sqlserver: str
    bronze_excel: str
    bronze_csv: str
    silver: str
    gold: str
    rejects: str
    dim_account: str
    dim_branch: str


def resolve_tables(
    catalog: str | None = "migration_demo",
    schema_prefix: str = "banking_mig_",
    *,
    bronze_schema: str | None = None,
    silver_schema: str | None = None,
    gold_schema: str | None = None,
    ops_schema: str | None = None,
    dim_schema: str | None = None,
) -> FactTransactionTables:
    """Fully qualified names; any ``*_schema`` overrides ``{schema_prefix}{layer}``."""

    def fq(schema: str | None, layer: str, table: str) -> str:
        name = f"{schema or schema_prefix + layer}.{table}"
        return f"{catalog}.{name}" if catalog else name

    gold = gold_schema
    return FactTransactionTables(
        bronze_sqlserver=fq(bronze_schema, "bronze", "sqlserver_transaction_db"),
        bronze_excel=fq(bronze_schema, "bronze", "file_transaction_excel"),
        bronze_csv=fq(bronze_schema, "bronze", "file_transaction_csv"),
        silver=fq(silver_schema, "silver", "transaction"),
        gold=fq(gold, "gold", "fact_transaction"),
        rejects=fq(ops_schema, "ops", "fact_transaction_rejects"),
        dim_account=fq(dim_schema or gold, "gold", "dim_account"),
        dim_branch=fq(dim_schema or gold, "gold", "dim_branch"),
    )


def _blank_to_null(c: Column) -> Column:
    return F.when(F.trim(c) == "", F.lit(None)).otherwise(c)


def _to_decimal(df: DataFrame, name: str) -> Column:
    dtype = df.schema[name].dataType
    src = f"trim(`{name}`)" if isinstance(dtype, T.StringType) else f"`{name}`"
    return F.expr(f"try_cast({src} AS DECIMAL(38,6))")


def _to_int(df: DataFrame, name: str) -> Column:
    """Whole numbers only (``6``, ``6.0``); ``6.5`` / ``abc`` / out of range -> NULL."""
    d = _to_decimal(df, name)
    ok = (d == F.floor(d)) & d.between(-2147483648, 2147483647)
    return F.when(ok, d.cast("int"))


def _to_amount(df: DataFrame, name: str) -> Column:
    dtype = df.schema[name].dataType
    src = f"trim(`{name}`)" if isinstance(dtype, T.StringType) else f"`{name}`"
    return F.expr(f"try_cast({src} AS DECIMAL(19,4))")


def _to_timestamp(df: DataFrame, name: str, formats: tuple[str, ...]) -> Column:
    dtype = df.schema[name].dataType
    if isinstance(dtype, (T.TimestampType, T.TimestampNTZType, T.DateType)):
        return F.col(name).cast("timestamp")
    s = F.trim(F.col(name).cast("string"))
    return F.coalesce(*[F.call_function("try_to_timestamp", s, F.lit(fmt)) for fmt in formats])


def normalize_source(df: DataFrame, source_system: str) -> DataFrame:
    """Type one bronze stream into the FactTransaction shape plus lineage and ``reject_reason``."""
    if source_system not in SOURCES:
        raise ValueError(f"unknown source_system {source_system!r}")
    missing = [c for c in BUSINESS_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{source_system} bronze is missing columns {missing}")

    order_col = next((c for c in INGEST_ORDER_CANDIDATES if c in df.columns), None)
    rescued = F.col("_rescued_data") if "_rescued_data" in df.columns else F.lit(None).cast("string")
    raw = {c: _blank_to_null(F.col(c).cast("string")) for c in BUSINESS_COLUMNS}

    typed = df.select(
        F.lit(source_system).alias("source_system"),
        F.lit(SOURCES[source_system]).alias("source_rank"),
        (F.col(order_col).cast("string") if order_col else F.lit(None).cast("string")).alias("_ingest_order"),
        _to_int(df, "transaction_id").alias("transaction_id"),
        _to_int(df, "account_id").alias("account_id"),
        _to_timestamp(df, "transaction_date", DATE_FORMATS[source_system]).alias("transaction_date"),
        _to_amount(df, "amount").alias("amount"),
        F.col("transaction_type").cast("string").alias("transaction_type"),
        _to_int(df, "branch_id").alias("branch_id"),
        F.to_json(F.struct(*[raw[c].alias(c) for c in BUSINESS_COLUMNS], rescued.alias("_rescued_data"))).alias(
            "raw_record"
        ),
        rescued.alias("_raw__rescued_data"),
        *[
            raw[c].alias(f"_raw_{c}")
            for c in ("transaction_id", "account_id", "transaction_date", "amount", "branch_id")
        ],
    )

    def bad(field: str) -> Column:
        return F.col(f"_raw_{field}").isNotNull() & F.col(field).isNull()

    reason = (
        F.when(F.col("_raw_transaction_id").isNull(), F.lit("MISSING_TRANSACTION_ID"))
        .when(bad("transaction_id"), F.lit("INVALID_TRANSACTION_ID"))
        .when(F.col("_raw__rescued_data").isNotNull(), F.lit("RESCUED_DATA"))
        .when(bad("transaction_date"), F.lit("INVALID_TRANSACTION_DATE"))
        .when(bad("amount"), F.lit("INVALID_AMOUNT"))
        .when(bad("account_id"), F.lit("INVALID_ACCOUNT_ID"))
        .when(bad("branch_id"), F.lit("INVALID_BRANCH_ID"))
    )
    return typed.withColumn("reject_reason", reason).drop(*[c for c in typed.columns if c.startswith("_raw_")])


def _reject_frame(df: DataFrame, stage: str, run_id: str | None) -> DataFrame:
    return df.select(
        F.lit(run_id).cast("string").alias("run_id"),
        F.lit(stage).alias("stage"),
        F.col("reject_reason"),
        F.col("source_system"),
        F.col("transaction_id").cast("int").alias("transaction_id"),
        F.col("raw_record"),
        F.current_timestamp().alias("rejected_at"),
    )


def build_silver(
    sqlserver: DataFrame, excel: DataFrame, csv: DataFrame, run_id: str | None = None
) -> tuple[DataFrame, DataFrame]:
    """Return ``(silver, rejects)``. Silver is one row per ``transaction_id``."""
    unioned = reduce(
        lambda a, b: a.unionByName(b),
        [normalize_source(sqlserver, "sqlserver"), normalize_source(excel, "excel"), normalize_source(csv, "csv")],
    )
    invalid = unioned.filter(F.col("reject_reason").isNotNull())
    valid = unioned.filter(F.col("reject_reason").isNull())

    first = Window.partitionBy("transaction_id").orderBy(
        F.col("source_rank").asc(),
        F.col("_ingest_order").asc_nulls_last(),
        *[
            F.col(c).asc_nulls_last()
            for c in ("transaction_date", "amount", "account_id", "branch_id", "transaction_type")
        ],
    )
    ranked = valid.withColumn("_rn", F.row_number().over(first))

    silver = ranked.filter("_rn = 1").select(
        *BUSINESS_COLUMNS,
        "source_system",
        F.lit(run_id).cast("string").alias("_run_id"),
        F.current_timestamp().alias("_processed_at"),
    )
    duplicates = ranked.filter("_rn > 1").withColumn("reject_reason", F.lit("DUPLICATE_TRANSACTION_ID"))
    rejects = _reject_frame(invalid, "silver", run_id).unionByName(_reject_frame(duplicates, "silver", run_id))
    return silver, rejects


def write_rejects(spark: SparkSession, rejects: DataFrame, table: str, stage: str) -> None:
    """Replace only this stage's rows so the silver and gold steps can rerun independently."""
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], REJECTS_SCHEMA).write.format("delta").saveAsTable(table)
    (rejects.write.format("delta").mode("overwrite").option("replaceWhere", f"stage = '{stage}'").saveAsTable(table))


def run_silver(spark: SparkSession, tables: FactTransactionTables, run_id: str | None = None) -> dict[str, int]:
    silver, rejects = build_silver(
        spark.table(tables.bronze_sqlserver), spark.table(tables.bronze_excel), spark.table(tables.bronze_csv), run_id
    )
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables.silver)
    write_rejects(spark, rejects, tables.rejects, "silver")
    return {
        "silver_rows": spark.table(tables.silver).count(),
        "silver_rejects": spark.table(tables.rejects).filter("stage = 'silver'").count(),
    }


def split_fk(silver: DataFrame, dim_account: DataFrame, dim_branch: DataFrame) -> tuple[DataFrame, DataFrame]:
    """``(loadable, fk_rejects)``: NULL FKs pass, like SQL Server's nullable FK columns."""
    acc = dim_account.select(F.col("account_id").cast("int").alias("_acc")).distinct()
    br = dim_branch.select(F.col("branch_id").cast("int").alias("_br")).distinct()
    checked = (
        silver.join(acc, silver["account_id"] == acc["_acc"], "left")
        .join(br, silver["branch_id"] == br["_br"], "left")
        .withColumn(
            "reject_reason",
            F.when(F.col("account_id").isNotNull() & F.col("_acc").isNull(), F.lit("FK_ACCOUNT_NOT_FOUND")).when(
                F.col("branch_id").isNotNull() & F.col("_br").isNull(), F.lit("FK_BRANCH_NOT_FOUND")
            ),
        )
        .drop("_acc", "_br")
    )
    loadable = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejects = checked.filter(F.col("reject_reason").isNotNull()).withColumn(
        "raw_record", F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in BUSINESS_COLUMNS]))
    )
    return loadable, rejects


def ensure_gold_table(spark: SparkSession, table: str) -> None:
    if not spark.catalog.tableExists(table):
        spark.createDataFrame([], FACT_SCHEMA).write.format("delta").saveAsTable(table)


def merge_into_gold(spark: SparkSession, source: DataFrame, table: str, delete_missing: bool = True) -> dict[str, int]:
    """MERGE on ``transaction_id``. Only the six business columns are written, so a target that has
    extra columns (identity surrogate key, audit columns) keeps working."""
    from delta.tables import DeltaTable

    ensure_gold_table(spark, table)
    target_types = {f.name: f.dataType for f in spark.table(table).schema.fields}
    src = source.select(
        *[F.col(c).cast(target_types.get(c, FACT_SCHEMA[c].dataType)).alias(c) for c in BUSINESS_COLUMNS]
    )

    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in BUSINESS_COLUMNS if c != "transaction_id")
    cols = {c: f"s.{c}" for c in BUSINESS_COLUMNS}
    merge = (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(src.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdate(condition=changed, set=cols)
        .whenNotMatchedInsert(values=cols)
    )
    if delete_missing:
        merge = merge.whenNotMatchedBySourceDelete()
    merge.execute()

    last = DeltaTable.forName(spark, table).history(1).select("operation", "operationMetrics").first()
    metrics = last["operationMetrics"] if last and last["operation"] == "MERGE" else {}
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "deleted": int(metrics.get("numTargetRowsDeleted", 0)),
    }


def run_gold(
    spark: SparkSession, tables: FactTransactionTables, run_id: str | None = None, delete_missing: bool = True
) -> dict[str, int]:
    for t in (tables.silver, tables.dim_account, tables.dim_branch):
        if not spark.catalog.tableExists(t):
            raise RuntimeError(f"required table {t} does not exist")
    loadable, fk_rejects = split_fk(
        spark.table(tables.silver), spark.table(tables.dim_account), spark.table(tables.dim_branch)
    )
    write_rejects(spark, _reject_frame(fk_rejects, "gold", run_id), tables.rejects, "gold")
    result = merge_into_gold(spark, loadable, tables.gold, delete_missing=delete_missing)
    result["gold_rows"] = spark.table(tables.gold).count()
    result["gold_rejects"] = spark.table(tables.rejects).filter("stage = 'gold'").count()
    return result


def parity_diff(actual: DataFrame, expected: DataFrame) -> tuple[int, int]:
    """``(missing_from_actual, unexpected_in_actual)`` over the six business columns, exact match."""
    a = actual.select(*[F.col(c).cast(FACT_SCHEMA[c].dataType) for c in BUSINESS_COLUMNS])
    e = expected.select(*[F.col(c).cast(FACT_SCHEMA[c].dataType) for c in BUSINESS_COLUMNS])
    return e.exceptAll(a).count(), a.exceptAll(e).count()


def read_parity_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .schema(T.StructType([T.StructField(c, T.StringType()) for c in BUSINESS_COLUMNS]))
        .csv(path)
        .select(
            F.col("transaction_id").cast("int").alias("transaction_id"),
            F.col("account_id").cast("int").alias("account_id"),
            F.to_timestamp("transaction_date", "yyyy-MM-dd HH:mm:ss.SSS").alias("transaction_date"),
            F.col("amount").cast(AMOUNT_TYPE).alias("amount"),
            F.col("transaction_type"),
            F.col("branch_id").cast("int").alias("branch_id"),
        )
    )
