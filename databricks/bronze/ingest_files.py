# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — file sources (CSV + Excel)
# MAGIC
# MAGIC Migrated from the Talend job `Load_FactTransaction` (`talend_jobs/Load_FactTransaction.zip`,
# MAGIC `IDX_INTERNSHIP/process/Load_FactTransaction_0.1.item`), components `tFileInputDelimited_1`
# MAGIC and `tFileInputExcel_1`.
# MAGIC
# MAGIC Targets:
# MAGIC * `dwh.bronze.transaction_csv`   <- `transaction_csv.csv`
# MAGIC * `dwh.bronze.transaction_excel` <- `transaction_excel.xlsx`
# MAGIC
# MAGIC Bronze is **raw fidelity**: no cleansing, no dedup, no derived columns and no type coercion
# MAGIC beyond the declared schema. Deduplication on `transaction_id` (Talend `tUniqRow`) and the
# MAGIC union of the three transaction sources (Talend `tUnite`) belong to the gold fact build
# MAGIC (`databricks/gold/`), not here.
# MAGIC
# MAGIC ## Audit columns (same as the other bronze notebooks)
# MAGIC | column | meaning |
# MAGIC |---|---|
# MAGIC | `_ingested_at` | `current_timestamp()` at write time |
# MAGIC | `_source_system` | `csv` / `excel` |
# MAGIC | `_source_file` | full input file path (`_metadata.file_path` for CSV, the widget path for Excel) |
# MAGIC
# MAGIC ## Cluster requirement
# MAGIC The Excel reader needs the **spark-excel** library installed on the cluster:
# MAGIC Maven coordinate `com.crealytics:spark-excel_2.12:3.5.0_0.20.4`
# MAGIC (Scala 2.12 / Spark 3.5 — pick the artifact matching the DBR Scala+Spark version).
# MAGIC There is **no Auto Loader (`cloudFiles`) support for Excel**, so the Excel source is
# MAGIC batch-only; only the CSV source has an Auto Loader path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How the schemas were derived
# MAGIC
# MAGIC ### `transaction_csv.csv` (inspected in `data_sources/transaction_csv.csv`)
# MAGIC * Header line 1: `transaction_id,account_id,transaction_date,amount,transaction_type,branch_id`
# MAGIC * 12 data rows, comma delimited, no quoting anywhere in the file (Talend still declares
# MAGIC   `"` as both escape char and text enclosure, so the reader keeps `"` as the quote char),
# MAGIC   `\n` row separator, no trailing newline on the last record.
# MAGIC * `transaction_date` values look like `21-01-2024 14:00:00` -> `dd-MM-yyyy HH:mm:ss`,
# MAGIC   which is exactly the pattern declared on the Talend column.
# MAGIC * `amount` values are whole numbers (e.g. `1500000`); Talend types them `id_Integer`.
# MAGIC * Encoding: Talend declares `ISO-8859-15`; the file is pure ASCII so UTF-8 reads identically.
# MAGIC   `ISO-8859-1` is passed explicitly to keep byte-for-byte parity with the legacy job.
# MAGIC
# MAGIC ### `transaction_excel.xlsx` (inspected with `openpyxl`)
# MAGIC * Single sheet `Sheet1`, used range `A1:F8` — header row 1 + 7 data rows, first column `A`.
# MAGIC * Same six column names and order as the CSV.
# MAGIC * `transaction_date` cells are **real Excel datetimes** (number format `m/d/yy h:mm`,
# MAGIC   e.g. `2024-01-18 13:10:00`), not text — so no date pattern is needed, unlike the CSV.
# MAGIC * `transaction_id`, `account_id`, `amount`, `branch_id` are numeric cells; `transaction_type`
# MAGIC   is text (`Deposit` / `Withdrawal` / `Transfer` / `Payment`).
# MAGIC
# MAGIC ### Types
# MAGIC Talend types both file schemas identically:
# MAGIC `transaction_id INT (key, non-null)`, `account_id INT`, `transaction_date DATE
# MAGIC (pattern "dd-MM-yyyy HH:mm:ss")`, `amount INT`, `transaction_type STRING`, `branch_id INT`.
# MAGIC
# MAGIC We keep those types, with two deliberate widenings (documented in the PR):
# MAGIC * `amount` -> `DECIMAL(19,4)`, matching the agreed `MONEY -> DECIMAL(19,4)` mapping of the
# MAGIC   target `FactTransaction.Amount`. Talend's `id_Integer` would silently truncate any future
# MAGIC   fractional amount; `DECIMAL(19,4)` reads today's whole numbers losslessly.
# MAGIC * every column is declared `nullable=True` (including `transaction_id`) so that a bad row is
# MAGIC   rescued/flagged rather than failing the batch — bronze never drops records.

# COMMAND ----------

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

spark: SparkSession = spark  # noqa: F821  (provided by Databricks)
dbutils = dbutils  # noqa: F821  (provided by Databricks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "dwh", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text(
    "input_volume",
    "/Volumes/dwh/bronze/landing",
    "Unity Catalog Volume root for the raw files",
)
dbutils.widgets.text("csv_path", "transactions/csv", "CSV directory, relative to the volume")
dbutils.widgets.text(
    "excel_path", "transactions/transaction_excel.xlsx", "Excel file, relative to the volume"
)
dbutils.widgets.text(
    "checkpoint_root",
    "/Volumes/dwh/bronze/_checkpoints",
    "Volume root for Auto Loader checkpoints + schema locations",
)
dbutils.widgets.dropdown("csv_mode", "autoloader", ["autoloader", "batch"], "CSV read mode")
dbutils.widgets.dropdown("source", "both", ["both", "csv", "excel"], "Source(s) to ingest")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
VOLUME_ROOT = dbutils.widgets.get("input_volume").rstrip("/")
CSV_PATH = f"{VOLUME_ROOT}/{dbutils.widgets.get('csv_path').lstrip('/')}"
EXCEL_PATH = f"{VOLUME_ROOT}/{dbutils.widgets.get('excel_path').lstrip('/')}"
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root").rstrip("/")
CSV_MODE = dbutils.widgets.get("csv_mode")
SOURCE = dbutils.widgets.get("source")

CSV_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.transaction_csv"
EXCEL_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.transaction_excel"

# Auto Loader state. Both locations must be stable across runs: the schema location holds the
# schema/evolution history, the checkpoint holds the file-notification/listing progress. Deleting
# either one re-ingests everything, so they live outside the landing volume path.
CSV_SCHEMA_LOCATION = f"{CHECKPOINT_ROOT}/transaction_csv/_schema"
CSV_CHECKPOINT_LOCATION = f"{CHECKPOINT_ROOT}/transaction_csv/_checkpoint"

# Column that keeps everything the declared schema could not parse (Auto Loader), and the
# equivalent for the batch CSV / Excel readers.
RESCUED_COLUMN = "_rescued_data"
CORRUPT_COLUMN = "_corrupt_record"

# Talend declares ISO-8859-15 on both file inputs; the files are ASCII so this is parity-only.
CSV_ENCODING = "ISO-8859-1"
CSV_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"

EXCEL_MAVEN_COORDINATE = "com.crealytics:spark-excel_2.12:3.5.0_0.20.4"
EXCEL_SHEET = "Sheet1"
# A1 = header cell, F = last populated column; the reader stops at the last non-empty row.
EXCEL_DATA_ADDRESS = f"'{EXCEL_SHEET}'!A1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit schemas
# MAGIC
# MAGIC Never inferred at runtime: Auto Loader is given `cloudFiles.schemaHints`-free explicit
# MAGIC `.schema(...)`, and the batch readers get the same `StructType`.

# COMMAND ----------

TRANSACTION_CSV_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DecimalType(19, 4), True),
        StructField("transaction_type", StringType(), True),
        StructField("branch_id", IntegerType(), True),
        # PERMISSIVE landing zone for anything unparseable — persisted, never dropped.
        StructField(RESCUED_COLUMN, StringType(), True),
    ]
)

# Same logical schema; Excel has no rescued-data support in the datasource, so the corrupt-record
# column is populated by this notebook (see `_add_excel_corrupt_column`).
TRANSACTION_EXCEL_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DecimalType(19, 4), True),
        StructField("transaction_type", StringType(), True),
        StructField("branch_id", IntegerType(), True),
    ]
)

EXCEL_READ_SCHEMA = StructType(
    TRANSACTION_EXCEL_SCHEMA.fields + [StructField(CORRUPT_COLUMN, StringType(), True)]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def with_audit_columns(df: DataFrame, source_system: str, source_file_expr) -> DataFrame:
    """Append the shared bronze audit columns. No other transformation is applied."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_file", source_file_expr)
    )


def log_bad_records(df: DataFrame, column: str, label: str) -> int:
    """Count and log rows that carry rescued/corrupt content. Rows are kept either way."""
    if column not in df.columns:
        print(f"[{label}] no `{column}` column present; bad-record count skipped")
        return 0
    bad = df.filter(F.col(column).isNotNull()).count()
    total = df.count()
    if bad:
        print(f"[{label}] WARNING {bad}/{total} row(s) have non-null `{column}` (kept in bronze)")
    else:
        print(f"[{label}] 0/{total} bad record(s)")
    return bad


# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV — Auto Loader (incremental) or batch fallback
# MAGIC
# MAGIC Both paths use `mode=PERMISSIVE` semantics: Auto Loader via `rescuedDataColumn`, the batch
# MAGIC reader via `columnNameOfCorruptRecord` mapped onto the same `_rescued_data` column so the
# MAGIC bronze table has one stable shape regardless of the mode used.

# COMMAND ----------


def ingest_csv_autoloader() -> None:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", CSV_SCHEMA_LOCATION)
        .option("cloudFiles.includeExistingFiles", "true")
        .option("rescuedDataColumn", RESCUED_COLUMN)
        .option("mode", "PERMISSIVE")
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .option("encoding", CSV_ENCODING)
        .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .schema(TRANSACTION_CSV_SCHEMA)
        .load(CSV_PATH)
    )
    batch = with_audit_columns(stream, "csv", F.col("_metadata.file_path"))

    def _write(micro_batch: DataFrame, batch_id: int) -> None:
        micro_batch.persist()
        try:
            log_bad_records(micro_batch, RESCUED_COLUMN, f"csv/autoloader batch {batch_id}")
            micro_batch.write.format("delta").mode("append").saveAsTable(CSV_TABLE)
        finally:
            micro_batch.unpersist()

    query = (
        batch.writeStream.option("checkpointLocation", CSV_CHECKPOINT_LOCATION)
        .foreachBatch(_write)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(f"[csv/autoloader] wrote to {CSV_TABLE} (checkpoint {CSV_CHECKPOINT_LOCATION})")


def ingest_csv_batch() -> None:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .option("encoding", CSV_ENCODING)
        .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", RESCUED_COLUMN)
        .schema(TRANSACTION_CSV_SCHEMA)
        .load(CSV_PATH)
    )
    df = with_audit_columns(df, "csv", F.col("_metadata.file_path"))
    log_bad_records(df, RESCUED_COLUMN, "csv/batch")
    df.write.format("delta").mode("append").saveAsTable(CSV_TABLE)
    print(f"[csv/batch] wrote to {CSV_TABLE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Excel — batch only
# MAGIC
# MAGIC Auto Loader has no `cloudFiles.format=excel`, so `.xlsx` cannot be streamed; this source is
# MAGIC always a full batch read of the single workbook. Requires the cluster library
# MAGIC `com.crealytics:spark-excel_2.12:3.5.0_0.20.4`.

# COMMAND ----------


def ingest_excel() -> None:
    df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", EXCEL_DATA_ADDRESS)
        .option("treatEmptyValuesAsNulls", "true")
        .option("usePlainNumberFormat", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(EXCEL_READ_SCHEMA)
        .load(EXCEL_PATH)
    )
    # spark-excel exposes no file-metadata column, so the widget path is the source file.
    df = with_audit_columns(df, "excel", F.lit(EXCEL_PATH))
    log_bad_records(df, CORRUPT_COLUMN, "excel/batch")
    df.write.format("delta").mode("append").saveAsTable(EXCEL_TABLE)
    print(f"[excel/batch] wrote to {EXCEL_TABLE} (library {EXCEL_MAVEN_COORDINATE})")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

if SOURCE in ("both", "csv"):
    if CSV_MODE == "autoloader":
        ingest_csv_autoloader()
    else:
        ingest_csv_batch()

if SOURCE in ("both", "excel"):
    ingest_excel()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick verification

# COMMAND ----------

for table in (CSV_TABLE, EXCEL_TABLE):
    if spark.catalog.tableExists(table):
        print(f"{table}: {spark.table(table).count()} row(s)")
        spark.table(table).show(5, truncate=False)
