# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `dwh.silver.dim_customer`
# MAGIC
# MAGIC Databricks replacement for the Talend job `Load_DimCustomer`
# MAGIC (`talend_jobs/Load_DimCustomer.zip` -> `IDX_INTERNSHIP/process/Load_DimCustomer_0.1.item`).
# MAGIC
# MAGIC Legacy component graph:
# MAGIC
# MAGIC ```
# MAGIC tMSSqlInput(customer) --row1--> tMap_1 --to_DimCustomer--> tMSSqlOutput(DWH.DimCustomer)
# MAGIC tMSSqlInput(city)     --row2--^   (lookup, key row2.city_id  = row1.city_id)
# MAGIC tMSSqlInput(state)    --row3--^   (lookup, key row3.state_id = row2.state_id)
# MAGIC ```
# MAGIC
# MAGIC tMap expression -> PySpark mapping (authoritative, taken from the `.item`):
# MAGIC
# MAGIC | Output column (Talend) | tMap expression            | Silver column   | PySpark                                    |
# MAGIC | ---------------------- | -------------------------- | --------------- | ------------------------------------------ |
# MAGIC | `CustomerID`           | `row1.customer_id`                     | `customer_id`   | `c.customer_id.cast("int")`                |
# MAGIC | `CustomerName`         | `StringHandling.UPCASE(row1.customer_name)` | `customer_name` | `upper(c.customer_name).cast("string")` |
# MAGIC | `Address`              | `StringHandling.UPCASE(row1.address)`  | `address`       | `upper(c.address).cast("string")`          |
# MAGIC | `CityName`             | `row2.city_name`                       | `city_name`     | `ci.city_name.cast("string")` (no upcase)  |
# MAGIC | `StateName`            | `row3.state_name`                      | `state_name`    | `st.state_name.cast("string")` (no upcase) |
# MAGIC | `Age`                  | `row1.age`                             | `age`           | `c.age.cast("int")`                        |
# MAGIC | `Gender`               | `StringHandling.UPCASE(row1.gender)`   | `gender`        | `upper(c.gender).cast("string")`           |
# MAGIC | `Email`                | `row1.email`                           | `email`         | `c.email.cast("string")` (NOT cleansed)    |
# MAGIC
# MAGIC Notes on fidelity:
# MAGIC * Only `customer_name`, `address` and `gender` are wrapped in `StringHandling.UPCASE`.
# MAGIC   `email`, `age`, `city_name` and `state_name` are passed through verbatim.
# MAGIC * The job sets `TRIM_ALL_COLUMN=false` and every per-column `TRIM=false` on all three
# MAGIC   `tMSSqlInput` components, so **no whitespace trimming** is performed here either.
# MAGIC * `age` is `VARCHAR(3)` in the source and in the tMap flow, but `DimCustomer.Age` is `INT`
# MAGIC   in the DWH DDL, so the implicit Talend string->int conversion on load becomes an
# MAGIC   explicit `cast("int")`.
# MAGIC
# MAGIC Join semantics (`.item`, `nodeData/inputTables`): neither lookup table carries
# MAGIC `innerJoin="true"` and the tMap has no reject output for a lookup inner-join reject, so both
# MAGIC lookups are Talend's default **Left Outer Join / Unique match / Load once**. On a lookup miss
# MAGIC Talend keeps the customer row and emits `null` for the lookup columns; a missing `city`
# MAGIC lookup also nulls `row2.state_id`, which in turn makes the `state` lookup miss and null
# MAGIC `state_name`. This implementation matches that exactly: two broadcast **left** joins,
# MAGIC chained on `city.state_id`, never dropping a customer row.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# `dbutils` only exists in the Databricks runtime; the defaults keep this module
# importable from pytest, which exercises the pure functions below.
ON_DATABRICKS = "dbutils" in globals()

if ON_DATABRICKS:
    dbutils.widgets.text("catalog", "dwh", "Catalog")  # noqa: F821
    dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")  # noqa: F821
    dbutils.widgets.text("silver_schema", "silver", "Silver schema")  # noqa: F821
    dbutils.widgets.text("target_table", "dim_customer", "Target table")  # noqa: F821
    dbutils.widgets.dropdown(  # noqa: F821
        "full_overwrite", "false", ["true", "false"], "Full overwrite"
    )
    dbutils.widgets.text(  # noqa: F821
        "max_unmatched_lookup_rate", "0.05", "Max unmatched lookup rate"
    )

    CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
    BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")  # noqa: F821
    SILVER_SCHEMA = dbutils.widgets.get("silver_schema")  # noqa: F821
    TARGET_TABLE = dbutils.widgets.get("target_table")  # noqa: F821
    FULL_OVERWRITE = dbutils.widgets.get("full_overwrite").lower() == "true"  # noqa: F821
    MAX_UNMATCHED_LOOKUP_RATE = float(
        dbutils.widgets.get("max_unmatched_lookup_rate")  # noqa: F821
    )
else:
    CATALOG = "dwh"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    TARGET_TABLE = "dim_customer"
    FULL_OVERWRITE = False
    MAX_UNMATCHED_LOOKUP_RATE = 0.05

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
TARGET_FQN = f"{CATALOG}.{SILVER_SCHEMA}.{TARGET_TABLE}"

# COMMAND ----------

# MAGIC %md ## Transformation

SILVER_COLUMNS = [
    "customer_id",
    "customer_name",
    "address",
    "city_name",
    "state_name",
    "age",
    "gender",
    "email",
]


def build_dim_customer(
    customer: DataFrame,
    city: DataFrame,
    state: DataFrame,
) -> DataFrame:
    """Reproduce the ``Load_DimCustomer`` tMap.

    Two left-outer, unique-match lookups (city on ``city_id``, state on the *city's*
    ``state_id``) plus ``StringHandling.UPCASE`` on ``customer_name``, ``address`` and
    ``gender`` only. Lookup misses yield ``NULL`` lookup columns and never drop the
    customer row, exactly as Talend's default lookup model does.
    """
    c = customer.alias("c")
    ci = city.select("city_id", "city_name", "state_id").alias("ci")
    st = state.select("state_id", "state_name").alias("st")

    joined = c.join(
        F.broadcast(ci), F.col("c.city_id") == F.col("ci.city_id"), "left"
    ).join(F.broadcast(st), F.col("ci.state_id") == F.col("st.state_id"), "left")

    return joined.select(
        F.col("c.customer_id").cast("int").alias("customer_id"),
        F.upper(F.col("c.customer_name")).cast("string").alias("customer_name"),
        F.upper(F.col("c.address")).cast("string").alias("address"),
        F.col("ci.city_name").cast("string").alias("city_name"),
        F.col("st.state_name").cast("string").alias("state_name"),
        F.col("c.age").cast("int").alias("age"),
        F.upper(F.col("c.gender")).cast("string").alias("gender"),
        F.col("c.email").cast("string").alias("email"),
    )


# COMMAND ----------

# MAGIC %md ## Data quality gates


class DataQualityError(Exception):
    """Raised when a silver DQ gate fails."""


def run_dq_checks(
    dim: DataFrame,
    source_customer_count: int,
    max_unmatched_lookup_rate: float = 0.05,
) -> dict:
    """Fail loudly on null/duplicate keys, row loss, or excessive unmatched lookups."""
    stats = dim.select(
        F.count(F.lit(1)).alias("row_count"),
        F.count_distinct(F.col("customer_id")).alias("distinct_customer_id"),
        F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer_id"),
        F.sum(F.col("city_name").isNull().cast("int")).alias("unmatched_city"),
        F.sum(F.col("state_name").isNull().cast("int")).alias("unmatched_state"),
    ).collect()[0].asDict()

    row_count = stats["row_count"]
    errors = []

    if stats["null_customer_id"]:
        errors.append(
            f"{stats['null_customer_id']} row(s) have a NULL customer_id; "
            f"the DimCustomer primary key does not allow NULLs — fix {BRONZE}.customer."
        )

    duplicates = row_count - stats["distinct_customer_id"]
    if duplicates:
        errors.append(
            f"customer_id is not unique: {row_count} rows but only "
            f"{stats['distinct_customer_id']} distinct customer_id ({duplicates} duplicate(s)). "
            "MERGE INTO would fail with a multi-match error."
        )

    if row_count != source_customer_count:
        errors.append(
            f"Row-count mismatch vs {BRONZE}.customer: source has {source_customer_count} rows, "
            f"dim_customer produced {row_count}. The lookups are LEFT joins with unique match, "
            "so counts must be identical — a difference means duplicate city/state keys or "
            "an accidental inner join."
        )

    for label, unmatched in (
        ("city", stats["unmatched_city"]),
        ("state", stats["unmatched_state"]),
    ):
        rate = (unmatched / row_count) if row_count else 0.0
        print(
            f"[dq] unmatched {label} lookups: {unmatched}/{row_count} ({rate:.2%})"
        )
        if rate > max_unmatched_lookup_rate:
            errors.append(
                f"Unmatched {label} lookup rate {rate:.2%} exceeds the allowed "
                f"{max_unmatched_lookup_rate:.2%} ({unmatched}/{row_count} rows). "
                f"Check referential integrity between {BRONZE}.customer.city_id, "
                f"{BRONZE}.city.state_id and {BRONZE}.state.state_id."
            )

    if errors:
        raise DataQualityError(
            "dim_customer DQ gate failed:\n- " + "\n- ".join(errors)
        )

    return stats


# COMMAND ----------

# MAGIC %md ## Load (idempotent MERGE, or full overwrite)


def write_dim_customer(
    spark: SparkSession,
    dim: DataFrame,
    target_fqn: str,
    full_overwrite: bool = False,
) -> None:
    """Create the Delta table if needed, then MERGE on ``customer_id`` (or overwrite)."""
    if full_overwrite:
        dim.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target_fqn)
        return

    if not spark.catalog.tableExists(target_fqn):
        dim.limit(0).write.format("delta").saveAsTable(target_fqn)

    dim.createOrReplaceTempView("dim_customer_updates")
    set_clause = ",\n            ".join(
        f"t.{col} = s.{col}" for col in SILVER_COLUMNS if col != "customer_id"
    )
    spark.sql(
        f"""
        MERGE INTO {target_fqn} AS t
        USING dim_customer_updates AS s
          ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# COMMAND ----------

# MAGIC %md ## Entry point


def main(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

    customer = spark.table(f"{BRONZE}.customer")
    city = spark.table(f"{BRONZE}.city")
    state = spark.table(f"{BRONZE}.state")

    dim = build_dim_customer(customer, city, state).cache()
    run_dq_checks(dim, customer.count(), MAX_UNMATCHED_LOOKUP_RATE)
    write_dim_customer(spark, dim, TARGET_FQN, FULL_OVERWRITE)
    print(f"[silver] wrote {dim.count()} rows to {TARGET_FQN}")


# COMMAND ----------

if ON_DATABRICKS:
    main(spark)  # noqa: F821 — `spark` is provided by the Databricks runtime
