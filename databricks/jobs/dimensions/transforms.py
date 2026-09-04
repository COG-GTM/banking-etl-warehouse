"""Pure DataFrame-in / DataFrame-out transforms for the DWH dimension tables.

Each function mirrors the tMap of the corresponding Talend job
(Load_DimBranch, Load_DimAccount, Load_DimCustomer) and produces the exact
gold column names and types. No I/O happens here.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DIM_BRANCH_SCHEMA = StructType(
    [
        StructField("BranchID", IntegerType(), False),
        StructField("BranchName", StringType(), True),
        StructField("BranchLocation", StringType(), True),
    ]
)

DIM_ACCOUNT_SCHEMA = StructType(
    [
        StructField("AccountID", IntegerType(), False),
        StructField("CustomerID", IntegerType(), True),
        StructField("AccountType", StringType(), True),
        StructField("Balance", DecimalType(19, 4), True),
        StructField("DateOpened", DateType(), True),
        StructField("Status", StringType(), True),
    ]
)

DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("CustomerID", IntegerType(), False),
        StructField("CustomerName", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("CityName", StringType(), True),
        StructField("StateName", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Email", StringType(), True),
    ]
)


def _conform(df: DataFrame, schema: StructType) -> DataFrame:
    """Cast/select columns so the result matches ``schema`` exactly (incl. nullability)."""
    cols = [F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields]
    out = df.select(*cols)
    non_null = [f.name for f in schema.fields if not f.nullable]
    if non_null:
        out = out.dropna(subset=non_null)
    return out.sparkSession.createDataFrame(out.rdd, schema)


def _upper(col: str):
    # StringHandling.UPCASE(null) yields null in Talend; Spark upper() does too.
    return F.upper(F.col(col))


def build_dim_branch(branch_df: DataFrame) -> DataFrame:
    """Talend Load_DimBranch: branch -> DimBranch (straight column rename)."""
    df = branch_df.select(
        F.col("branch_id").alias("BranchID"),
        F.col("branch_name").alias("BranchName"),
        F.col("branch_location").alias("BranchLocation"),
    )
    return _conform(df, DIM_BRANCH_SCHEMA)


def build_dim_account(account_df: DataFrame) -> DataFrame:
    """Talend Load_DimAccount: account -> DimAccount (rename + type cast)."""
    df = account_df.select(
        F.col("account_id").alias("AccountID"),
        F.col("customer_id").alias("CustomerID"),
        F.col("account_type").alias("AccountType"),
        F.col("balance").alias("Balance"),
        F.col("date_opened").alias("DateOpened"),
        F.col("status").alias("Status"),
    )
    return _conform(df, DIM_ACCOUNT_SCHEMA)


def build_dim_customer(
    customer_df: DataFrame, city_df: DataFrame, state_df: DataFrame
) -> DataFrame:
    """Talend Load_DimCustomer tMap.

    row1 (customer) is the main flow; row2 (city) is looked up on
    ``customer.city_id = city.city_id`` and row3 (state) on
    ``city.state_id = state.state_id``. Both lookups use the tMap default
    (left outer join, unique match) so customers without a city/state are
    kept with null CityName/StateName. customer_name, address and gender go
    through ``StringHandling.UPCASE``; email is passed through unchanged.
    """
    cust = customer_df.alias("c")
    city = city_df.select(
        F.col("city_id").alias("_city_id"),
        F.col("city_name").alias("_city_name"),
        F.col("state_id").alias("_state_id"),
    )
    state = state_df.select(
        F.col("state_id").alias("_st_state_id"),
        F.col("state_name").alias("_state_name"),
    )

    # UNIQUE_MATCH: keep a single lookup row per key.
    city = city.dropDuplicates(["_city_id"])
    state = state.dropDuplicates(["_st_state_id"])

    joined = cust.join(city, F.col("c.city_id") == F.col("_city_id"), "left").join(
        state, F.col("_state_id") == F.col("_st_state_id"), "left"
    )

    df = joined.select(
        F.col("c.customer_id").alias("CustomerID"),
        _upper("c.customer_name").alias("CustomerName"),
        _upper("c.address").alias("Address"),
        F.col("_city_name").alias("CityName"),
        F.col("_state_name").alias("StateName"),
        F.col("c.age").alias("Age"),
        _upper("c.gender").alias("Gender"),
        F.col("c.email").alias("Email"),
    )
    return _conform(df, DIM_CUSTOMER_SCHEMA)
