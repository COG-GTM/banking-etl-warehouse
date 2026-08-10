# Databricks notebook source
# MAGIC %md
# MAGIC # Legacy vs. Databricks parity check
# MAGIC
# MAGIC Compares the Databricks lakehouse output against the legacy SQL Server
# MAGIC `DWH` database so the two can be run in parallel during cutover.
# MAGIC
# MAGIC Checks performed:
# MAGIC
# MAGIC | # | Check | Scope |
# MAGIC |---|-------|-------|
# MAGIC | 1 | Row counts | every dimension + the fact table |
# MAGIC | 2 | `SUM(amount)` | `fact_transaction` |
# MAGIC | 3 | `COUNT(DISTINCT transaction_id)` | `fact_transaction` |
# MAGIC | 4 | Per-`transaction_type` count and amount | `fact_transaction` |
# MAGIC | 5 | `MIN`/`MAX(transaction_date)` | `fact_transaction` |
# MAGIC | 6 | Row-level hash diff on the business key `transaction_id` | `fact_transaction` |
# MAGIC
# MAGIC Results are appended to `<catalog>.analytics.parity_results` and a
# MAGIC pass/fail summary is returned via `dbutils.notebook.exit`.
# MAGIC
# MAGIC The legacy side is read over JDBC; credentials come from
# MAGIC `dbutils.secrets` (scope defaults to `dwh`) — nothing is hard-coded.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

import datetime as dt
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("catalog", "dwh", "Unity Catalog catalog")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("secret_scope", "dwh", "Secret scope holding legacy JDBC creds")
dbutils.widgets.dropdown("fail_on_mismatch", "false", ["true", "false"], "Fail run on mismatch")
dbutils.widgets.text("row_count_tolerance_pct", "0.0", "Row count tolerance (%)")
dbutils.widgets.text("amount_tolerance_pct", "0.01", "Amount tolerance (%)")
dbutils.widgets.text("hash_diff_sample_rows", "50", "Hash-diff sample rows to log")

CATALOG = dbutils.widgets.get("catalog")
ENVIRONMENT = dbutils.widgets.get("environment")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
FAIL_ON_MISMATCH = dbutils.widgets.get("fail_on_mismatch").lower() == "true"
ROW_COUNT_TOL_PCT = float(dbutils.widgets.get("row_count_tolerance_pct"))
AMOUNT_TOL_PCT = float(dbutils.widgets.get("amount_tolerance_pct"))
HASH_DIFF_SAMPLE_ROWS = int(dbutils.widgets.get("hash_diff_sample_rows"))

RESULTS_TABLE = f"{CATALOG}.analytics.parity_results"
RUN_ID = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md ## Legacy JDBC connection
# MAGIC
# MAGIC Expected secrets in the `<secret_scope>` scope:
# MAGIC `legacy_jdbc_host`, `legacy_jdbc_port`, `legacy_jdbc_database`,
# MAGIC `legacy_jdbc_user`, `legacy_jdbc_password`.

# COMMAND ----------


def _secret(key: str, default: Optional[str] = None) -> str:
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:  # noqa: BLE001 - secret absent in some environments
        if default is None:
            raise
        return default


def legacy_jdbc_options() -> Dict[str, str]:
    host = _secret("legacy_jdbc_host")
    port = _secret("legacy_jdbc_port", "1433")
    database = _secret("legacy_jdbc_database", "DWH")
    return {
        "url": (
            f"jdbc:sqlserver://{host}:{port};databaseName={database};"
            "encrypt=true;trustServerCertificate=true"
        ),
        "user": _secret("legacy_jdbc_user"),
        "password": _secret("legacy_jdbc_password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def read_legacy(query: str) -> DataFrame:
    """Run a query against the legacy SQL Server DWH and return a DataFrame."""
    return (
        spark.read.format("jdbc")
        .options(**legacy_jdbc_options())
        .option("query", query)
        .load()
    )


def read_lakehouse(table: str) -> DataFrame:
    return spark.table(table)


# COMMAND ----------

# MAGIC %md ## Table mapping
# MAGIC
# MAGIC Legacy T-SQL names are PascalCase; the lakehouse uses snake_case under
# MAGIC Unity Catalog (`dwh.silver.*` for dimensions, `dwh.gold.*` for the fact).

# COMMAND ----------

TABLE_MAP = [
    ("DimBranch", f"{CATALOG}.silver.dim_branch"),
    ("DimAccount", f"{CATALOG}.silver.dim_account"),
    ("DimCustomer", f"{CATALOG}.silver.dim_customer"),
    ("FactTransaction", f"{CATALOG}.gold.fact_transaction"),
]

FACT_LEGACY = "FactTransaction"
FACT_LAKEHOUSE = f"{CATALOG}.gold.fact_transaction"

# Business-key columns of the fact table, legacy -> lakehouse.
FACT_COLUMNS = {
    "TransactionID": "transaction_id",
    "AccountID": "account_id",
    "TransactionDate": "transaction_date",
    "Amount": "amount",
    "TransactionType": "transaction_type",
    "BranchID": "branch_id",
}

# COMMAND ----------

# MAGIC %md ## Result collection

# COMMAND ----------

RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_ts", TimestampType(), False),
        StructField("environment", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("entity", StringType(), False),
        StructField("legacy_value", StringType(), True),
        StructField("databricks_value", StringType(), True),
        StructField("delta", StringType(), True),
        StructField("delta_pct", StringType(), True),
        StructField("tolerance_pct", StringType(), True),
        StructField("passed", BooleanType(), False),
        StructField("details", StringType(), True),
    ]
)


@dataclass
class ParityResult:
    check_name: str
    entity: str
    legacy_value: Optional[str]
    databricks_value: Optional[str]
    delta: Optional[str] = None
    delta_pct: Optional[str] = None
    tolerance_pct: Optional[str] = None
    passed: bool = False
    details: Optional[str] = None


@dataclass
class ResultCollector:
    results: List[ParityResult] = field(default_factory=list)

    def add(self, result: ParityResult) -> ParityResult:
        self.results.append(result)
        return result

    @property
    def failed(self) -> List[ParityResult]:
        return [r for r in self.results if not r.passed]

    def to_rows(self, run_ts: dt.datetime) -> List[tuple]:
        return [
            (
                RUN_ID,
                run_ts,
                ENVIRONMENT,
                r.check_name,
                r.entity,
                r.legacy_value,
                r.databricks_value,
                r.delta,
                r.delta_pct,
                r.tolerance_pct,
                r.passed,
                r.details,
            )
            for r in self.results
        ]


collector = ResultCollector()


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def compare_numeric(
    check_name: str,
    entity: str,
    legacy: Any,
    databricks: Any,
    tolerance_pct: float,
    details: Optional[str] = None,
) -> ParityResult:
    """Compare two numbers within a relative tolerance (legacy is the baseline)."""
    left, right = _to_decimal(legacy), _to_decimal(databricks)
    if left is None or right is None:
        passed = left == right
        delta = delta_pct = None
    else:
        delta = right - left
        delta_pct = (
            (abs(delta) / abs(left) * Decimal(100)) if left != 0 else (Decimal(0) if delta == 0 else Decimal(100))
        )
        passed = delta_pct <= Decimal(str(tolerance_pct))
    return collector.add(
        ParityResult(
            check_name=check_name,
            entity=entity,
            legacy_value=None if left is None else str(left),
            databricks_value=None if right is None else str(right),
            delta=None if delta is None else str(delta),
            delta_pct=None if delta_pct is None else f"{delta_pct:.6f}",
            tolerance_pct=str(tolerance_pct),
            passed=passed,
            details=details,
        )
    )


def compare_exact(
    check_name: str,
    entity: str,
    legacy: Any,
    databricks: Any,
    details: Optional[str] = None,
) -> ParityResult:
    passed = str(legacy) == str(databricks)
    return collector.add(
        ParityResult(
            check_name=check_name,
            entity=entity,
            legacy_value=None if legacy is None else str(legacy),
            databricks_value=None if databricks is None else str(databricks),
            passed=passed,
            details=details,
        )
    )


# COMMAND ----------

# MAGIC %md ## Check 1 — per-table row counts

# COMMAND ----------


def check_row_counts() -> None:
    for legacy_table, lakehouse_table in TABLE_MAP:
        legacy_count = read_legacy(f"SELECT COUNT(*) AS c FROM {legacy_table}").collect()[0]["c"]
        databricks_count = read_lakehouse(lakehouse_table).count()
        compare_numeric(
            check_name="row_count",
            entity=lakehouse_table,
            legacy=legacy_count,
            databricks=databricks_count,
            tolerance_pct=ROW_COUNT_TOL_PCT,
            details=f"legacy table {legacy_table}",
        )


check_row_counts()

# COMMAND ----------

# MAGIC %md ## Checks 2, 3 & 5 — fact aggregates and date bounds

# COMMAND ----------


def check_fact_aggregates() -> None:
    legacy = read_legacy(
        f"""
        SELECT
            SUM(Amount)                     AS total_amount,
            COUNT(DISTINCT TransactionID)   AS distinct_transactions,
            MIN(TransactionDate)            AS min_transaction_date,
            MAX(TransactionDate)            AS max_transaction_date
        FROM {FACT_LEGACY}
        """
    ).collect()[0]

    databricks = (
        read_lakehouse(FACT_LAKEHOUSE)
        .agg(
            F.sum("amount").alias("total_amount"),
            F.countDistinct("transaction_id").alias("distinct_transactions"),
            F.min("transaction_date").alias("min_transaction_date"),
            F.max("transaction_date").alias("max_transaction_date"),
        )
        .collect()[0]
    )

    compare_numeric(
        "sum_amount",
        FACT_LAKEHOUSE,
        legacy["total_amount"],
        databricks["total_amount"],
        AMOUNT_TOL_PCT,
    )
    compare_numeric(
        "count_distinct_transaction_id",
        FACT_LAKEHOUSE,
        legacy["distinct_transactions"],
        databricks["distinct_transactions"],
        ROW_COUNT_TOL_PCT,
    )
    compare_exact(
        "min_transaction_date",
        FACT_LAKEHOUSE,
        legacy["min_transaction_date"],
        databricks["min_transaction_date"],
    )
    compare_exact(
        "max_transaction_date",
        FACT_LAKEHOUSE,
        legacy["max_transaction_date"],
        databricks["max_transaction_date"],
    )


check_fact_aggregates()

# COMMAND ----------

# MAGIC %md ## Check 4 — per-transaction_type totals

# COMMAND ----------


def check_per_transaction_type() -> None:
    legacy_rows = read_legacy(
        f"""
        SELECT TransactionType   AS transaction_type,
               COUNT(*)          AS txn_count,
               SUM(Amount)       AS total_amount
        FROM {FACT_LEGACY}
        GROUP BY TransactionType
        """
    ).collect()
    databricks_rows = (
        read_lakehouse(FACT_LAKEHOUSE)
        .groupBy("transaction_type")
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum("amount").alias("total_amount"),
        )
        .collect()
    )

    legacy_map = {r["transaction_type"]: r for r in legacy_rows}
    databricks_map = {r["transaction_type"]: r for r in databricks_rows}

    for txn_type in sorted(set(legacy_map) | set(databricks_map), key=lambda v: str(v)):
        legacy_row = legacy_map.get(txn_type)
        databricks_row = databricks_map.get(txn_type)
        entity = f"{FACT_LAKEHOUSE}[transaction_type={txn_type}]"
        compare_numeric(
            "transaction_type_count",
            entity,
            legacy_row["txn_count"] if legacy_row else 0,
            databricks_row["txn_count"] if databricks_row else 0,
            ROW_COUNT_TOL_PCT,
            details=None if legacy_row and databricks_row else "transaction_type missing on one side",
        )
        compare_numeric(
            "transaction_type_amount",
            entity,
            legacy_row["total_amount"] if legacy_row else 0,
            databricks_row["total_amount"] if databricks_row else 0,
            AMOUNT_TOL_PCT,
        )


check_per_transaction_type()

# COMMAND ----------

# MAGIC %md ## Check 6 — row-level hash diff on the fact business key
# MAGIC
# MAGIC Both sides are reduced to `(transaction_id, row_hash)` where `row_hash`
# MAGIC is a SHA-256 of the canonicalised business columns. A full outer join
# MAGIC then classifies every business key as `missing_in_databricks`,
# MAGIC `missing_in_legacy` or `value_mismatch`.

# COMMAND ----------

# Canonicalisation rules, applied identically on both sides so that formatting
# differences (trailing decimals, timestamp precision, casing) do not show up as
# false mismatches:
#   * amount           -> DECIMAL(19,4) rendered with 4 decimal places (T-SQL MONEY)
#   * transaction_date -> 'yyyy-MM-dd HH:mm:ss'
#   * strings          -> trimmed, upper-cased
#   * NULLs            -> the literal '<NULL>'


def _fact_hash_databricks() -> DataFrame:
    df = read_lakehouse(FACT_LAKEHOUSE)
    canonical = F.concat_ws(
        "|",
        F.coalesce(F.col("transaction_id").cast("string"), F.lit("<NULL>")),
        F.coalesce(F.col("account_id").cast("string"), F.lit("<NULL>")),
        F.coalesce(F.date_format(F.col("transaction_date"), "yyyy-MM-dd HH:mm:ss"), F.lit("<NULL>")),
        F.coalesce(F.format_number(F.col("amount").cast("decimal(19,4)"), 4), F.lit("<NULL>")),
        F.coalesce(F.upper(F.trim(F.col("transaction_type"))), F.lit("<NULL>")),
        F.coalesce(F.col("branch_id").cast("string"), F.lit("<NULL>")),
    )
    return df.select(
        F.col("transaction_id").alias("business_key"),
        F.sha2(canonical, 256).alias("row_hash"),
    )


def _fact_hash_legacy() -> DataFrame:
    df = read_legacy(
        f"""
        SELECT {', '.join(FACT_COLUMNS)}
        FROM {FACT_LEGACY}
        """
    )
    canonical = F.concat_ws(
        "|",
        F.coalesce(F.col("TransactionID").cast("string"), F.lit("<NULL>")),
        F.coalesce(F.col("AccountID").cast("string"), F.lit("<NULL>")),
        F.coalesce(F.date_format(F.col("TransactionDate"), "yyyy-MM-dd HH:mm:ss"), F.lit("<NULL>")),
        F.coalesce(F.format_number(F.col("Amount").cast("decimal(19,4)"), 4), F.lit("<NULL>")),
        F.coalesce(F.upper(F.trim(F.col("TransactionType"))), F.lit("<NULL>")),
        F.coalesce(F.col("BranchID").cast("string"), F.lit("<NULL>")),
    )
    return df.select(
        F.col("TransactionID").alias("business_key"),
        F.sha2(canonical, 256).alias("row_hash"),
    )


def check_row_level_hash_diff() -> None:
    legacy = _fact_hash_legacy().alias("l")
    databricks = _fact_hash_databricks().alias("d")

    joined = legacy.join(databricks, on="business_key", how="full_outer").select(
        F.col("business_key"),
        F.col("l.row_hash").alias("legacy_hash"),
        F.col("d.row_hash").alias("databricks_hash"),
    )

    diffs = joined.where(
        F.col("legacy_hash").isNull()
        | F.col("databricks_hash").isNull()
        | (F.col("legacy_hash") != F.col("databricks_hash"))
    ).withColumn(
        "diff_type",
        F.when(F.col("databricks_hash").isNull(), F.lit("missing_in_databricks"))
        .when(F.col("legacy_hash").isNull(), F.lit("missing_in_legacy"))
        .otherwise(F.lit("value_mismatch")),
    )
    diffs.cache()

    total_rows = joined.count()
    diff_counts = {r["diff_type"]: r["c"] for r in diffs.groupBy("diff_type").agg(F.count(F.lit(1)).alias("c")).collect()}
    diff_total = sum(diff_counts.values())

    sample = [r.asDict() for r in diffs.limit(HASH_DIFF_SAMPLE_ROWS).collect()]
    if sample:
        print(f"Hash-diff sample (first {len(sample)} of {diff_total}):")
        for row in sample:
            print(f"  {row['diff_type']}: business_key={row['business_key']}")

    collector.add(
        ParityResult(
            check_name="row_hash_diff",
            entity=FACT_LAKEHOUSE,
            legacy_value=str(total_rows - diff_counts.get("missing_in_legacy", 0)),
            databricks_value=str(total_rows - diff_counts.get("missing_in_databricks", 0)),
            delta=str(diff_total),
            delta_pct=f"{(diff_total / total_rows * 100) if total_rows else 0:.6f}",
            tolerance_pct="0.0",
            passed=diff_total == 0,
            details=json.dumps(
                {
                    "business_key": "transaction_id",
                    "compared_keys": total_rows,
                    "diff_counts": diff_counts,
                    "sample": sample[:10],
                },
                default=str,
            ),
        )
    )
    diffs.unpersist()


check_row_level_hash_diff()

# COMMAND ----------

# MAGIC %md ## Persist results to `<catalog>.analytics.parity_results`

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.analytics")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
        run_id            STRING,
        run_ts            TIMESTAMP,
        environment       STRING,
        check_name        STRING,
        entity            STRING,
        legacy_value      STRING,
        databricks_value  STRING,
        delta             STRING,
        delta_pct         STRING,
        tolerance_pct     STRING,
        passed            BOOLEAN,
        details           STRING
    ) USING DELTA
    """
)

run_ts = dt.datetime.utcnow()
results_df = spark.createDataFrame(collector.to_rows(run_ts), schema=RESULTS_SCHEMA)
results_df.write.mode("append").saveAsTable(RESULTS_TABLE)

display(results_df.orderBy(F.col("passed"), F.col("check_name")))

# COMMAND ----------

# MAGIC %md ## Pass/fail summary

# COMMAND ----------

failed = collector.failed
summary = {
    "run_id": RUN_ID,
    "environment": ENVIRONMENT,
    "catalog": CATALOG,
    "checks_run": len(collector.results),
    "checks_failed": len(failed),
    "status": "PASS" if not failed else "FAIL",
    "failed_checks": [f"{r.check_name}:{r.entity}" for r in failed],
    "results_table": RESULTS_TABLE,
}

print(json.dumps(summary, indent=2))

if failed and FAIL_ON_MISMATCH:
    raise AssertionError(
        f"Parity check FAILED: {len(failed)} of {len(collector.results)} checks "
        f"outside tolerance. See {RESULTS_TABLE} for run_id={RUN_ID}."
    )

dbutils.notebook.exit(json.dumps(summary))
