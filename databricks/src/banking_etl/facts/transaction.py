"""Port of Talend ``Load_FactTransaction``: bronze transactions -> silver.transaction -> gold.fact_transaction.

Talend job (``talend_jobs/Load_FactTransaction.zip``, ``process/Load_FactTransaction_0.1.item``)::

    tDBInput_1 (tMSSqlInput, dbo.transaction_db)          --row1, mergeOrder=1--\\
    tFileInputExcel_1 (Sheet1, header 1, DIE_ON_ERROR=false) --row2, mergeOrder=2-- tUnite_1
    tFileInputDelimited_1 (",", header 1, DIE_ON_ERROR=false) --row3, mergeOrder=3--/
      -> tUniqRow_1 (key transaction_id, ONLY_ONCE_EACH_DUPLICATED_KEY=false; UNIQUE -> tMap,
                     DUPLICATE not connected)
      -> tMap_1 (to_FactTransaction: pure rename row5.<col> -> <Col>)
      -> tDBOutput_1 (tMSSqlOutput, DWH.FactTransaction, TABLE_ACTION=TRUNCATE, DATA_ACTION=INSERT,
                      USE_BATCH_SIZE=true, DIE_ON_ERROR=false, no REJECT link)

Ported semantics:

- Conform: transaction_date is a SQL ``datetime2`` (bronze TIMESTAMP), an Excel date cell (bronze
  TIMESTAMP_NTZ; text cells are parsed with the schema pattern) or CSV text parsed with
  ``dd-MM-yyyy HH:mm:ss``; amount (Talend ``Integer``) -> ``DECIMAL(19,4)`` (``MONEY``).
- Input rejects: the file inputs have DIE_ON_ERROR=false and no REJECT link, so a row whose
  transaction_id is null, whose date does not parse or that has a type mismatch is dropped *before*
  tUnite (it cannot shadow a valid duplicate). Here those rows go to ``ops.fact_transaction_rejects``.
- tUnite + tUniqRow: tUnite emits its inputs in mergeOrder (SQL Server, Excel, CSV) and tUniqRow
  keeps the first row per transaction_id, so SQL Server beats Excel beats CSV. Duplicates are
  dropped silently, as in Talend.
- Output: TRUNCATE + INSERT becomes a full overwrite of gold.fact_transaction. SQL Server rejected
  rows violating ``FK_FactTransaction_DimAccount`` / ``FK_FactTransaction_DimBranch``; with batch
  mode + DIE_ON_ERROR=false Talend only logged the BatchUpdateException and committed the other
  rows, so the failing rows were silently lost. Here they go to ``ops.fact_transaction_rejects``.
  A NULL AccountID/BranchID passes an FK in SQL Server, so it is loaded with a NULL surrogate key.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from banking_etl.config import (
    BRONZE_FILE_TRANSACTION_CSV,
    BRONZE_FILE_TRANSACTION_EXCEL,
    BRONZE_SQLSERVER,
    GOLD_DIM_ACCOUNT,
    GOLD_DIM_BRANCH,
    GOLD_FACT_TRANSACTION,
    SILVER_TRANSACTION,
    Settings,
)
from banking_etl.gold.ddl import OPS_FACT_TRANSACTION_REJECTS

# Talend schema pattern of transaction_date in tFileInputDelimited_1 / tFileInputExcel_1.
FILE_DATE_PATTERN = "dd-MM-yyyy HH:mm:ss"

SQLSERVER, EXCEL, CSV = "sqlserver", "excel", "csv"
# tUnite_1 mergeOrder: row1 (tDBInput_1) = 1, row2 (tFileInputExcel_1) = 2, row3 (tFileInputDelimited_1) = 3.
SOURCE_ORDER = {SQLSERVER: 1, EXCEL: 2, CSV: 3}

SOURCE_COLUMNS = ("transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id")
SILVER_SCHEMA = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("account_id", IntegerType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("amount", DecimalType(19, 4), True),
    StructField("transaction_type", StringType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("_source_system", StringType(), True),
    StructField("_source_order", IntegerType(), True),
    StructField("_source", StringType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
])
SILVER_COLUMNS = tuple(f.name for f in SILVER_SCHEMA.fields)
REASON_COL = "_reject_reason"

# tMap_1 output ``to_FactTransaction``: legacy column -> silver column.
TMAP_TO_FACT = {
    "TransactionID": "transaction_id",
    "AccountID": "account_id",
    "TransactionDate": "transaction_date",
    "Amount": "amount",
    "TransactionType": "transaction_type",
    "BranchID": "branch_id",
}
FACT_SCHEMA = StructType([
    StructField("TransactionID", IntegerType()),
    StructField("AccountID", IntegerType()),
    StructField("TransactionDate", TimestampType()),
    StructField("Amount", DecimalType(19, 4)),
    StructField("TransactionType", StringType()),
    StructField("BranchID", IntegerType()),
    StructField("AccountKey", LongType()),
    StructField("BranchKey", LongType()),
])
FACT_COLUMNS = tuple(f.name for f in FACT_SCHEMA.fields)

# reject_reason codes. Several codes for one row are joined with ';'.
REJECT_NULL_TRANSACTION_ID = "NULL_TRANSACTION_ID"
REJECT_INVALID_TRANSACTION_DATE = "INVALID_TRANSACTION_DATE"
REJECT_MALFORMED_RECORD = "MALFORMED_RECORD"
REJECT_MISSING_DIM_ACCOUNT = "MISSING_DIM_ACCOUNT"
REJECT_MISSING_DIM_BRANCH = "MISSING_DIM_BRANCH"
# Every reason the gold step can write; the silver step owns all other reasons. Each step
# replaces only its own rows in ops.fact_transaction_rejects, so both stay idempotent.
FK_REJECT_REASONS = (
    REJECT_MISSING_DIM_ACCOUNT,
    REJECT_MISSING_DIM_BRANCH,
    f"{REJECT_MISSING_DIM_ACCOUNT};{REJECT_MISSING_DIM_BRANCH}",
)


# --------------------------------------------------------------------------- table names


def bronze_tables(settings: Settings) -> dict[str, str]:
    return {
        SQLSERVER: settings.table("bronze", BRONZE_SQLSERVER["transaction_db"]),
        EXCEL: settings.table("bronze", BRONZE_FILE_TRANSACTION_EXCEL),
        CSV: settings.table("bronze", BRONZE_FILE_TRANSACTION_CSV),
    }


def silver_transaction_table(settings: Settings) -> str:
    return settings.table("silver", SILVER_TRANSACTION)


def fact_transaction_table(settings: Settings) -> str:
    return settings.table("gold", GOLD_FACT_TRANSACTION)


def rejects_table(settings: Settings) -> str:
    return settings.table("ops", OPS_FACT_TRANSACTION_REJECTS)


def dim_tables(settings: Settings) -> dict[str, str]:
    return {"account": settings.table("gold", GOLD_DIM_ACCOUNT), "branch": settings.table("gold", GOLD_DIM_BRANCH)}


# --------------------------------------------------------------------------- conform (per input)


def _reasons(*checks: tuple[Column, str]) -> Column:
    """';'-joined codes of the failing checks, or NULL when all pass."""
    parts = [F.when(cond, F.lit(code)) for cond, code in checks]
    joined = F.concat_ws(";", *parts)
    return F.when(joined == "", F.lit(None).cast(StringType())).otherwise(joined)


def parse_file_date(col: Column, dtype) -> tuple[Column, Column]:
    """(parsed TIMESTAMP, invalid flag) for a file transaction_date of bronze type ``dtype``.

    Text uses the Talend pattern, strictly (``try_to_timestamp``; no ``Column.try_cast``, which the
    serverless Spark Connect client lacks); empty text is NULL, as Talend's ParserUtils does.
    Date-typed values (Excel date cells: TIMESTAMP_NTZ) keep their wall-clock time.
    """
    if isinstance(dtype, StringType):
        text = F.when(F.trim(col) != "", col)
        parsed = F.call_function("try_to_timestamp", text, F.lit(FILE_DATE_PATTERN))
        return parsed, text.isNotNull() & parsed.isNull()
    return col.cast(TimestampType()), F.lit(False)


def _conform(df: DataFrame, source: str, *, date_col: Column, invalid_date: Column) -> DataFrame:
    missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{source} transactions are missing columns {missing}")
    malformed = F.col("_rescued_data").isNotNull() if "_rescued_data" in df.columns else F.lit(False)
    optional = {c: (F.col(c) if c in df.columns else F.lit(None)) for c in ("_source", "_source_file", "_ingested_at")}
    return df.select(
        F.col("transaction_id").cast(IntegerType()).alias("transaction_id"),
        F.col("account_id").cast(IntegerType()).alias("account_id"),
        date_col.alias("transaction_date"),
        F.col("amount").cast(DecimalType(19, 4)).alias("amount"),
        F.col("transaction_type").cast(StringType()).alias("transaction_type"),
        F.col("branch_id").cast(IntegerType()).alias("branch_id"),
        F.lit(source).alias("_source_system"),
        F.lit(SOURCE_ORDER[source]).cast(IntegerType()).alias("_source_order"),
        optional["_source"].cast(StringType()).alias("_source"),
        optional["_source_file"].cast(StringType()).alias("_source_file"),
        optional["_ingested_at"].cast(TimestampType()).alias("_ingested_at"),
        _reasons(
            (F.col("transaction_id").isNull(), REJECT_NULL_TRANSACTION_ID),
            (invalid_date, REJECT_INVALID_TRANSACTION_DATE),
            (malformed, REJECT_MALFORMED_RECORD),
        ).alias(REASON_COL),
    )


def conform_sqlserver(df: DataFrame) -> DataFrame:
    """tDBInput_1: datetime2 -> TIMESTAMP, int amount -> DECIMAL(19,4)."""
    return _conform(df, SQLSERVER, date_col=F.col("transaction_date").cast(TimestampType()), invalid_date=F.lit(False))


def conform_excel(df: DataFrame) -> DataFrame:
    """tFileInputExcel_1: date cells are taken as-is, text cells parsed with ``dd-MM-yyyy HH:mm:ss``."""
    parsed, invalid = parse_file_date(F.col("transaction_date"), df.schema["transaction_date"].dataType)
    return _conform(df, EXCEL, date_col=parsed, invalid_date=invalid)


def conform_csv(df: DataFrame) -> DataFrame:
    """tFileInputDelimited_1: transaction_date text parsed with ``dd-MM-yyyy HH:mm:ss``."""
    parsed, invalid = parse_file_date(F.col("transaction_date"), df.schema["transaction_date"].dataType)
    return _conform(df, CSV, date_col=parsed, invalid_date=invalid)


# --------------------------------------------------------------------------- tUnite + tUniqRow


def unite(sqlserver: DataFrame, excel: DataFrame, csv: DataFrame) -> DataFrame:
    """tUnite_1: conformed inputs in mergeOrder (each row tagged with ``_source_order``)."""
    return conform_sqlserver(sqlserver).unionByName(conform_excel(excel)).unionByName(conform_csv(csv))


def dedup_first(df: DataFrame) -> DataFrame:
    """tUniqRow_1 UNIQUE output: first row per transaction_id in tUnite order.

    Ties inside one source (impossible for the SQL Server PK, and Talend reads a single file per
    input) are broken deterministically: earliest ``_ingested_at``, then ``_source_file``, then values.
    """
    order = Window.partitionBy("transaction_id").orderBy(
        F.col("_source_order").asc(),
        F.col("_ingested_at").asc_nulls_first(),
        F.col("_source_file").asc_nulls_first(),
        *[F.col(c).asc_nulls_first() for c in SOURCE_COLUMNS[1:]],
    )
    return df.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1).drop("_rn")


def transform_transaction(sqlserver: DataFrame, excel: DataFrame, csv: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(silver rows, input rejects). Input rejects carry ``_reject_reason`` and are removed before dedup."""
    united = unite(sqlserver, excel, csv)
    accepted = united.where(F.col(REASON_COL).isNull()).drop(REASON_COL)
    rejects = united.where(F.col(REASON_COL).isNotNull())
    silver = dedup_first(accepted).select([F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields])
    return silver, rejects


# --------------------------------------------------------------------------- dim lookups (tMSSqlOutput FKs)


def to_fact_rows(silver: DataFrame) -> DataFrame:
    """tMap_1 ``to_FactTransaction``: rename to the legacy PascalCase columns."""
    return silver.select([F.col(src).alias(dst) for dst, src in TMAP_TO_FACT.items()])


def lookup_dim_keys(fact_rows: DataFrame, dim_account: DataFrame, dim_branch: DataFrame) -> DataFrame:
    """Left-join AccountKey/BranchKey on the natural keys and flag FK misses in ``reject_reason``.

    A NULL natural key is not a miss: SQL Server does not check FKs on NULL.
    """
    acct = dim_account.select(F.col("AccountID").alias("_acct_id"), F.col("AccountKey"))
    branch = dim_branch.select(F.col("BranchID").alias("_branch_id"), F.col("BranchKey"))
    joined = (
        fact_rows.join(acct, fact_rows["AccountID"] == acct["_acct_id"], "left")
        .join(branch, fact_rows["BranchID"] == branch["_branch_id"], "left")
    )
    reason = _reasons(
        (F.col("AccountID").isNotNull() & F.col("_acct_id").isNull(), REJECT_MISSING_DIM_ACCOUNT),
        (F.col("BranchID").isNotNull() & F.col("_branch_id").isNull(), REJECT_MISSING_DIM_BRANCH),
    )
    return joined.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in FACT_SCHEMA.fields], reason.alias("reject_reason"))


def build_fact(silver: DataFrame, dim_account: DataFrame, dim_branch: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(gold.fact_transaction rows, FK rejects with reject_reason)."""
    looked_up = lookup_dim_keys(to_fact_rows(silver), dim_account, dim_branch)
    fact = looked_up.where(F.col("reject_reason").isNull()).select(*FACT_COLUMNS)
    rejects = looked_up.where(F.col("reject_reason").isNotNull())
    return fact, rejects


def to_reject_rows(df: DataFrame, reason: Column) -> DataFrame:
    """Shape rows into ops.fact_transaction_rejects (legacy fact columns + reject_reason, rejected_at)."""
    cols = []
    for f in FACT_SCHEMA.fields:
        src = f.name if f.name in df.columns else TMAP_TO_FACT.get(f.name)
        value = F.col(src) if src in df.columns else F.lit(None)
        cols.append(value.cast(f.dataType).alias(f.name))
    return df.select(*cols, reason.alias("reject_reason"), F.current_timestamp().alias("rejected_at"))


def _sql_list(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def write_rejects(df: DataFrame, table: str, *, fk: bool) -> None:
    """Atomically replace this step's rows in ops.fact_transaction_rejects (``replaceWhere``)."""
    predicate = f"reject_reason {'IN' if fk else 'NOT IN'} ({_sql_list(FK_REJECT_REASONS)})"
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)


def ensure_fact_tables(spark: SparkSession, settings: Settings) -> bool:
    """Create the gold star schema (ticket 4 DDL) if the fact or rejects table is missing."""
    if spark.catalog.tableExists(fact_transaction_table(settings)) and spark.catalog.tableExists(rejects_table(settings)):
        return False
    from banking_etl.gold.ddl import apply_star_schema

    apply_star_schema(spark, settings)
    return True


def _counts(df: DataFrame, col: str) -> dict[str, int]:
    return {str(r[0]): int(r[1]) for r in df.groupBy(col).count().orderBy(col).collect()}


# --------------------------------------------------------------------------- loads


def load_silver_transaction(spark: SparkSession, settings: Settings) -> dict:
    """Rebuild silver.transaction (overwrite) and the input-level rows of ops.fact_transaction_rejects."""
    sources = bronze_tables(settings)
    ensure_fact_tables(spark, settings)
    frames = {name: spark.table(table) for name, table in sources.items()}
    silver, rejects = transform_transaction(frames[SQLSERVER], frames[EXCEL], frames[CSV])
    target = silver_transaction_table(settings)
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    write_rejects(to_reject_rows(rejects, F.col(REASON_COL)), rejects_table(settings), fk=False)

    input_rows = {name: int(df.count()) for name, df in frames.items()}
    rows = spark.table(target).count()
    input_rejects = _counts(rejects, REASON_COL)
    return {
        "sources": sources,
        "target": target,
        "input_rows": input_rows,
        "input_rejects": input_rejects,
        "duplicates_dropped": sum(input_rows.values()) - sum(input_rejects.values()) - rows,
        "rows": rows,
        "rows_by_source": _counts(spark.table(target), "_source_system"),
    }


def load_fact_transaction(spark: SparkSession, settings: Settings, *, require_dims: bool = True) -> dict:
    """Full refresh of gold.fact_transaction from silver.transaction + dim key lookups.

    FK misses replace the gold-owned rows of ops.fact_transaction_rejects. With ``require_dims``
    an empty dim_account/dim_branch raises instead of rejecting every row (dims load first).
    """
    created = ensure_fact_tables(spark, settings)
    dims = {name: spark.table(table) for name, table in dim_tables(settings).items()}
    if require_dims:
        empty = [dim_tables(settings)[n] for n, df in dims.items() if df.limit(1).count() == 0]
        if empty:
            raise ValueError(f"dimension table(s) {empty} are empty; load dim_branch/dim_account first")
    source, target, rejects_target = silver_transaction_table(settings), fact_transaction_table(settings), rejects_table(settings)
    fact, rejects = build_fact(spark.table(source), dims["account"], dims["branch"])

    target_columns = spark.table(target).columns
    fact.select(*target_columns).write.insertInto(target, overwrite=True)
    write_rejects(to_reject_rows(rejects, F.col("reject_reason")), rejects_target, fk=True)

    all_rejects = spark.table(rejects_target)
    return {
        "source": source,
        "target": target,
        "rejects_table": rejects_target,
        "created": created,
        "rows": spark.table(target).count(),
        "fk_rejects": _counts(all_rejects.where(F.col("reject_reason").isin(*FK_REJECT_REASONS)), "reject_reason"),
        "total_rejects": all_rejects.count(),
    }
