"""Port of the Talend job ``Load_DimCustomer`` to Spark / Delta.

Legacy behaviour (``talend_jobs/Load_DimCustomer.zip`` -> ``Load_DimCustomer_0.1.item``):

* ``tDBInput_1`` reads ``dbo.customer``; ``tDBInput_2`` / ``tDBInput_3`` read
  ``dbo.city`` / ``dbo.state`` as tMap lookups.
* ``tMap_1`` joins ``customer.city_id = city.city_id`` and
  ``city.state_id = state.state_id``. Neither lookup sets ``innerJoin="true"``,
  so both are Talend's default *left outer* join. Matching mode is
  ``UNIQUE_MATCH``.
* Output expressions: ``UPCASE`` on customer_name, address and gender; email,
  city_name and state_name pass through unchanged; age (a varchar(3) in the
  source) is implicitly converted to ``INT`` by SQL Server on insert.
* ``tDBOutput_1`` does a plain ``INSERT`` into ``DWH.dbo.DimCustomer``.

Here the join + cleansing produce ``silver.customer`` (full snapshot) and
``gold.dim_customer`` is maintained with an idempotent SCD-1 ``MERGE``.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

GOLD_COLUMNS = [
    "customer_id",
    "customer_name",
    "address",
    "city_name",
    "state_name",
    "age",
    "gender",
    "email",
]

SILVER_COLUMNS = [
    "customer_id",
    "customer_name",
    "address",
    "city_id",
    "city_name",
    "state_id",
    "state_name",
    "age",
    "gender",
    "email",
]

GOLD_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.IntegerType(), False),
        T.StructField("customer_name", T.StringType(), True),
        T.StructField("address", T.StringType(), True),
        T.StructField("city_name", T.StringType(), True),
        T.StructField("state_name", T.StringType(), True),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
    ]
)

_INT_COLUMNS = {"customer_id", "city_id", "state_id", "age"}


@dataclass(frozen=True)
class Tables:
    """Fully qualified table names used by the job."""

    bronze_customer: str
    bronze_city: str
    bronze_state: str
    silver_customer: str
    gold_dim_customer: str

    @classmethod
    def build(
        cls,
        catalog: Optional[str] = None,
        schema_prefix: str = "banking_mig_",
        schema_override: Optional[str] = None,
    ) -> "Tables":
        def fq(layer: str, table: str) -> str:
            schema = schema_override or f"{schema_prefix}{layer}"
            return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"

        return cls(
            bronze_customer=fq("bronze", "sqlserver_customer"),
            bronze_city=fq("bronze", "sqlserver_city"),
            bronze_state=fq("bronze", "sqlserver_state"),
            silver_customer=fq("silver", "customer"),
            gold_dim_customer=fq("gold", "dim_customer"),
        )


def _to_int(col_name: str) -> F.Column:
    # F.expr keeps this Spark Connect safe (Column.try_cast fails on serverless).
    return F.expr(f"try_cast(trim(cast(`{col_name}` AS STRING)) AS INT)")


def _latest_per_key(df: DataFrame, key: str) -> DataFrame:
    """Keep one row per key so the MERGE never sees duplicate source matches.

    Prefers the most recent ``_ingested_at`` when bronze carries it, otherwise
    falls back to a deterministic ordering over all columns.
    """
    if "_ingested_at" in df.columns:
        order = [F.col("_ingested_at").desc_nulls_last()]
    else:
        order = [F.col(c).desc_nulls_last() for c in sorted(df.columns) if c != key]
    w = Window.partitionBy(key).orderBy(*order) if order else Window.partitionBy(key).orderBy(F.lit(1))
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_silver_customer(customer: DataFrame, city: DataFrame, state: DataFrame) -> DataFrame:
    """customer LEFT JOIN city LEFT JOIN state + Talend's UPCASE cleansing."""
    c = _latest_per_key(
        customer.select(
            _to_int("customer_id").alias("customer_id"),
            F.col("customer_name").cast("string").alias("customer_name"),
            F.col("address").cast("string").alias("address"),
            _to_int("city_id").alias("city_id"),
            _to_int("age").alias("age"),
            F.col("gender").cast("string").alias("gender"),
            F.col("email").cast("string").alias("email"),
        ).filter(F.col("customer_id").isNotNull()),
        "customer_id",
    ).alias("c")
    ci = _latest_per_key(
        city.select(
            _to_int("city_id").alias("city_id"),
            F.col("city_name").cast("string").alias("city_name"),
            _to_int("state_id").alias("state_id"),
        ).filter(F.col("city_id").isNotNull()),
        "city_id",
    ).alias("ci")
    s = _latest_per_key(
        state.select(
            _to_int("state_id").alias("state_id"),
            F.col("state_name").cast("string").alias("state_name"),
        ).filter(F.col("state_id").isNotNull()),
        "state_id",
    ).alias("s")

    joined = c.join(ci, F.col("c.city_id") == F.col("ci.city_id"), "left").join(
        s, F.col("ci.state_id") == F.col("s.state_id"), "left"
    )
    return joined.select(
        F.col("c.customer_id").alias("customer_id"),
        F.upper(F.col("c.customer_name")).alias("customer_name"),
        F.upper(F.col("c.address")).alias("address"),
        F.col("c.city_id").alias("city_id"),
        F.col("ci.city_name").alias("city_name"),
        F.col("ci.state_id").alias("state_id"),
        F.col("s.state_name").alias("state_name"),
        F.col("c.age").alias("age"),
        F.upper(F.col("c.gender")).alias("gender"),
        F.col("c.email").alias("email"),
    )


def to_gold(silver: DataFrame) -> DataFrame:
    return silver.select(*GOLD_COLUMNS)


def ensure_gold_table(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            customer_id INT NOT NULL,
            customer_name STRING,
            address STRING,
            city_name STRING,
            state_name STRING,
            age INT,
            gender STRING,
            email STRING
        ) USING DELTA
        COMMENT 'Customer dimension (port of Talend Load_DimCustomer -> DWH.dbo.DimCustomer)'
        """
    )


def write_silver(silver: DataFrame, table: str) -> None:
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def merge_gold(spark: SparkSession, source: DataFrame, table: str) -> None:
    """SCD-1 upsert keyed on customer_id; only rewrites rows whose attributes changed."""
    ensure_gold_table(spark, table)
    view = "_dim_customer_src"
    source.select(*GOLD_COLUMNS).createOrReplaceTempView(view)
    attrs = [c for c in GOLD_COLUMNS if c != "customer_id"]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in attrs)
    sets = ", ".join(f"t.{c} = s.{c}" for c in attrs)
    cols = ", ".join(GOLD_COLUMNS)
    vals = ", ".join(f"s.{c}" for c in GOLD_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING {view} AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND ({changed}) THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
        """
    )


def run(spark: SparkSession, tables: Tables) -> dict:
    """bronze -> silver.customer -> gold.dim_customer. Returns row counts."""
    silver = build_silver_customer(
        spark.table(tables.bronze_customer),
        spark.table(tables.bronze_city),
        spark.table(tables.bronze_state),
    )
    write_silver(silver, tables.silver_customer)
    silver_df = spark.table(tables.silver_customer)
    merge_gold(spark, to_gold(silver_df), tables.gold_dim_customer)
    return {
        "bronze_customer": spark.table(tables.bronze_customer).count(),
        "silver_customer": silver_df.count(),
        "gold_dim_customer": spark.table(tables.gold_dim_customer).count(),
    }


def read_parity_csv(spark: SparkSession, path: str) -> DataFrame:
    """Load the legacy DimCustomer baseline (exported from SQL Server) with gold types."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = [
            tuple(
                (None if r[c] == "" else int(r[c])) if c in _INT_COLUMNS else (None if r[c] == "" else r[c])
                for c in GOLD_COLUMNS
            )
            for r in csv.DictReader(f)
        ]
    return spark.createDataFrame(rows, GOLD_SCHEMA)


def parity_diff(actual: DataFrame, expected: DataFrame) -> dict:
    a = actual.select(*GOLD_COLUMNS)
    e = expected.select(*GOLD_COLUMNS)
    missing = e.exceptAll(a)
    unexpected = a.exceptAll(e)
    return {
        "expected_rows": e.count(),
        "actual_rows": a.count(),
        "missing_rows": missing.count(),
        "unexpected_rows": unexpected.count(),
        "missing_sample": [r.asDict() for r in missing.limit(5).collect()],
        "unexpected_sample": [r.asDict() for r in unexpected.limit(5).collect()],
    }


def assert_parity(actual: DataFrame, expected: DataFrame) -> dict:
    diff = parity_diff(actual, expected)
    if diff["missing_rows"] or diff["unexpected_rows"] or diff["expected_rows"] != diff["actual_rows"]:
        raise AssertionError(f"dim_customer parity failed: {diff}")
    return diff
