"""Port of the Talend job ``Load_DimCustomer`` (ticket 7).

bronze.sqlserver_customer + sqlserver_city + sqlserver_state -> silver.customer -> gold.dim_customer.

Talend ``tMap_1`` (``talend_jobs/Load_DimCustomer.zip``, ``process/Load_DimCustomer_0.1.item``):

- main flow ``row1`` = ``dbo.customer``; lookups ``row2`` = ``dbo.city`` on ``row1.city_id`` and
  ``row3`` = ``dbo.state`` on ``row2.state_id``. Both lookups are ``UNIQUE_MATCH`` / ``LOAD_ONCE``
  with no ``innerJoin`` flag, i.e. **left outer** joins: an unmatched city (or state) still emits the
  customer with ``CityName`` (and ``StateName``) = NULL. There are no reject outputs or filters.
- ``StringHandling.UPCASE`` (null-safe) on ``customer_name``, ``address`` and ``gender``; ``email``,
  ``age``, ``city_name`` and ``state_name`` pass through unchanged (no TRIM on any input column).
- ``tDBOutput_1`` inserts into ``DWH.DimCustomer`` (``sql_scripts/01_create_tables.sql``) with
  ``DATA_ACTION=INSERT`` and ``DIE_ON_ERROR=false``: rows SQL Server refuses (NULL/duplicate
  ``CustomerID``, ``Age`` that does not convert to INT, strings longer than the legacy VARCHAR) are
  skipped. Those rows go to ``ops.dim_customer_rejects`` here instead of being silently dropped.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from banking_etl.config import (
    BRONZE_SQLSERVER,
    GOLD_DIM_CUSTOMER,
    SILVER_CUSTOMER,
    Settings,
)

OPS_DIM_CUSTOMER_REJECTS = "dim_customer_rejects"

CUSTOMER_COLUMNS = (
    "customer_id",
    "customer_name",
    "address",
    "city_id",
    "age",
    "gender",
    "email",
)
CITY_COLUMNS = ("city_id", "city_name", "state_id")
STATE_COLUMNS = ("state_id", "state_name")

# Columns StringHandling.UPCASE is applied to in the tMap.
UPPERCASED = ("customer_name", "address", "gender")

SILVER_SCHEMA = (
    ("customer_id", "INT"),
    ("customer_name", "STRING"),
    ("address", "STRING"),
    ("city_id", "INT"),
    ("city_name", "STRING"),
    ("state_id", "INT"),
    ("state_name", "STRING"),
    ("age", "INT"),
    ("gender", "STRING"),
    ("email", "STRING"),
)
SILVER_COLUMNS = tuple(c for c, _ in SILVER_SCHEMA)

# gold.dim_customer column -> silver.customer column (the tMap output mapping).
GOLD_MAPPING = {
    "CustomerID": "customer_id",
    "CustomerName": "customer_name",
    "Address": "address",
    "CityName": "city_name",
    "StateName": "state_name",
    "Age": "age",
    "Gender": "gender",
    "Email": "email",
}
GOLD_KEY = "CustomerID"

# VARCHAR(n) limits of DWH.DimCustomer; longer values make SQL Server reject the INSERT.
LEGACY_MAX_LENGTH = {
    "customer_name": 100,
    "address": 255,
    "city_name": 100,
    "state_name": 100,
    "gender": 10,
    "email": 100,
}
# gold.dim_customer CHECK ck_dim_customer_age; Delta would fail the whole MERGE on a violation.
AGE_MIN, AGE_MAX = 0, 150
_INT_MIN, _INT_MAX = -(2**31), 2**31 - 1

REJECT_NULL_ID = "null_customer_id"
REJECT_DUPLICATE_ID = "duplicate_customer_id"
REJECT_AGE_NOT_INT = "age_not_int"
REJECT_AGE_RANGE = "age_out_of_range"
REJECT_TOO_LONG = "too_long_{}"


def parse_age(age: Column) -> Column:
    """``varchar(3)`` -> INT the way SQL Server converts the string Talend inserts into ``Age INT``.

    Surrounding spaces are ignored, a blank string converts to 0 (``CAST('' AS INT) = 0``),
    NULL stays NULL, anything else that is not an integer yields NULL (flagged as a reject).
    """
    # Regex + range check instead of try_cast: Column.try_cast is missing from the serverless Spark Connect client.
    trimmed = F.trim(age)
    as_long = F.when(trimmed.rlike(r"^[+-]?[0-9]{1,18}$"), trimmed.cast("bigint"))
    return (
        F.when(age.isNull(), F.lit(None).cast("int"))
        .when(trimmed == "", F.lit(0))
        .when(as_long.between(_INT_MIN, _INT_MAX), as_long.cast("int"))
    )


def unique_lookup(df: DataFrame, key: str) -> DataFrame:
    """One row per non-null ``key``, like a Talend ``UNIQUE_MATCH`` lookup.

    Talend keeps the last row loaded for a duplicated key, which depends on source order; Spark has
    no stable order, so the tie-break is deterministic (greatest remaining columns). The source keys
    are primary keys, so this only matters for malformed extracts.
    """
    others = [c for c in df.columns if c != key]
    w = Window.partitionBy(key).orderBy(*[F.col(c).desc_nulls_last() for c in others])
    return (
        df.where(F.col(key).isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
    )


def join_customer(customer: DataFrame, city: DataFrame, state: DataFrame) -> DataFrame:
    """tMap joins + cleansing. Returns the silver columns plus ``age_raw`` (the source string)."""
    c = customer.select(*CUSTOMER_COLUMNS).alias("c")
    ci = unique_lookup(city.select(*CITY_COLUMNS), "city_id").alias("ci")
    st = unique_lookup(state.select(*STATE_COLUMNS), "state_id").alias("st")
    joined = c.join(ci, F.col("c.city_id") == F.col("ci.city_id"), "left").join(
        st, F.col("ci.state_id") == F.col("st.state_id"), "left"
    )
    return joined.select(
        F.col("c.customer_id").cast("int").alias("customer_id"),
        F.upper("c.customer_name").alias("customer_name"),
        F.upper("c.address").alias("address"),
        F.col("c.city_id").cast("int").alias("city_id"),
        F.col("ci.city_name").alias("city_name"),
        F.col("ci.state_id").cast("int").alias("state_id"),
        F.col("st.state_name").alias("state_name"),
        parse_age(F.col("c.age").cast("string")).alias("age"),
        F.upper("c.gender").alias("gender"),
        F.col("c.email").alias("email"),
        F.col("c.age").cast("string").alias("age_raw"),
    )


def _reject_reasons() -> Column:
    dup = F.count(F.lit(1)).over(Window.partitionBy("customer_id")) > 1
    checks = [
        F.when(F.col("customer_id").isNull(), F.lit(REJECT_NULL_ID)),
        F.when(F.col("customer_id").isNotNull() & dup, F.lit(REJECT_DUPLICATE_ID)),
        F.when(
            F.col("age_raw").isNotNull() & F.col("age").isNull(),
            F.lit(REJECT_AGE_NOT_INT),
        ),
        F.when(~F.col("age").between(AGE_MIN, AGE_MAX), F.lit(REJECT_AGE_RANGE)),
    ]
    checks += [
        F.when(F.length(col) > n, F.lit(REJECT_TOO_LONG.format(col)))
        for col, n in LEGACY_MAX_LENGTH.items()
    ]
    return F.concat_ws(";", *checks)


def transform_customer(
    customer: DataFrame, city: DataFrame, state: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """(silver rows, rejected rows). Rejects carry the silver columns, ``age_raw``, ``reject_reason``."""
    flagged = join_customer(customer, city, state).withColumn(
        "reject_reason", _reject_reasons()
    )
    silver = flagged.where(F.col("reject_reason") == "").select(*SILVER_COLUMNS)
    rejects = flagged.where(F.col("reject_reason") != "").select(
        *SILVER_COLUMNS, "age_raw", "reject_reason"
    )
    return silver, rejects


def to_dim_customer(silver: DataFrame) -> DataFrame:
    """silver.customer -> gold.dim_customer columns (no surrogate key; Delta generates it)."""
    return silver.select(*[F.col(src).alias(dst) for dst, src in GOLD_MAPPING.items()])


def silver_table(settings: Settings) -> str:
    return settings.table("silver", SILVER_CUSTOMER)


def gold_table(settings: Settings) -> str:
    return settings.table("gold", GOLD_DIM_CUSTOMER)


def rejects_table(settings: Settings) -> str:
    return settings.table("ops", OPS_DIM_CUSTOMER_REJECTS)


def bronze_tables(settings: Settings) -> dict[str, str]:
    return {
        t: settings.table("bronze", BRONZE_SQLSERVER[t])
        for t in ("customer", "city", "state")
    }


def load_silver_customer(spark: SparkSession, settings: Settings) -> dict:
    """Build silver.customer (overwrite) and ops.dim_customer_rejects (overwrite) from bronze."""
    src = {t: spark.table(name) for t, name in bronze_tables(settings).items()}
    silver, rejects = transform_customer(src["customer"], src["city"], src["state"])
    loaded_at = F.current_timestamp()
    target, rejects_target = silver_table(settings), rejects_table(settings)
    for df, name in (
        (silver.withColumn("_loaded_at", loaded_at), target),
        (rejects.withColumn("rejected_at", loaded_at), rejects_target),
    ):
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(name)
    out = spark.table(target)
    return {
        "target": target,
        "rows": out.count(),
        "rejects": spark.table(rejects_target).count(),
        "unmatched_city": out.where("city_name IS NULL").count(),
        "unmatched_state": out.where("state_name IS NULL").count(),
    }


def ensure_dim_customer(spark: SparkSession, settings: Settings) -> bool:
    """Talend ``TABLE_ACTION=CREATE_IF_NOT_EXISTS``: create the gold star schema (ticket 4 DDL) if
    ``gold.dim_customer`` is missing. Returns True when it had to create it."""
    if spark.catalog.tableExists(gold_table(settings)):
        return False
    from banking_etl.gold.ddl import apply_star_schema

    apply_star_schema(spark, settings)
    return True


def load_dim_customer(spark: SparkSession, settings: Settings) -> dict:
    """SCD-1 MERGE of silver.customer into gold.dim_customer on ``CustomerID``."""
    from banking_etl.gold.merge import merge_scd1

    created = ensure_dim_customer(spark, settings)
    target = gold_table(settings)
    metrics = merge_scd1(
        spark, to_dim_customer(spark.table(silver_table(settings))), target, [GOLD_KEY]
    )
    return {
        "target": target,
        "created": created,
        **metrics,
        "rows": spark.table(target).count(),
    }
