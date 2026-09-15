"""Port of the Talend job `Load_DimCustomer`.

Legacy flow (IDX_INTERNSHIP/process/Load_DimCustomer_0.1.item):

    tMSSqlInput "customer" (row1) --\
    tMSSqlInput "city"     (row2) ---> tMap_1 (to_DimCustomer) -> tMSSqlOutput DimCustomer
    tMSSqlInput "state"    (row3) --/

tMap join expressions (lookups, no `innerJoin` attribute is set, so both use
Talend's default *Left Outer Join*, with `matchingMode="UNIQUE_MATCH"`):

    row2.city_id  = row1.city_id
    row3.state_id = row2.state_id

tMap output expressions:

    CustomerID   <- row1.customer_id
    CustomerName <- StringHandling.UPCASE(row1.customer_name)
    Address      <- StringHandling.UPCASE(row1.address)
    Age          <- row1.age                (id_String in the tMap, INT in DimCustomer DDL)
    Gender       <- StringHandling.UPCASE(row1.gender)
    Email        <- row1.email              (no UPCASE)
    CityName     <- row2.city_name          (no UPCASE)
    StateName    <- row3.state_name         (no UPCASE)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import (
    DimensionConfig,
    clean_string,
    deduplicate_on_key,
    drop_ingest_metadata,
    to_int,
    upcase,
)

DIM_CUSTOMER_CONFIG = DimensionConfig(
    bronze_tables={
        "customer": "sample_customer",
        "city": "sample_city",
        "state": "sample_state",
    },
    silver_table="dim_customer",
    gold_table="dim_customer",
    business_key="customer_id",
)


def transform_dim_customer(
    bronze_customer: DataFrame,
    bronze_city: DataFrame,
    bronze_state: DataFrame,
    unique_match: bool = True,
) -> DataFrame:
    """Conform the customer x city x state join into silver `dim_customer`.

    `unique_match` reproduces the tMap `UNIQUE_MATCH` lookup mode: at most one
    lookup row per key, so duplicated city/state rows cannot fan the customer
    grain out. Set it to False to see the fan-out instead of hiding it.
    """
    customer = drop_ingest_metadata(bronze_customer).select(
        to_int(F.col("customer_id")).alias("customer_id"),
        upcase(F.col("customer_name")).alias("customer_name"),
        upcase(F.col("address")).alias("address"),
        to_int(F.col("city_id")).alias("city_id"),
        to_int(F.col("age")).alias("age"),
        upcase(F.col("gender")).alias("gender"),
        clean_string(F.col("email")).alias("email"),
    )

    city = drop_ingest_metadata(bronze_city).select(
        to_int(F.col("city_id")).alias("city_id"),
        clean_string(F.col("city_name")).alias("city_name"),
        to_int(F.col("state_id")).alias("state_id"),
    )

    state = drop_ingest_metadata(bronze_state).select(
        to_int(F.col("state_id")).alias("state_id"),
        clean_string(F.col("state_name")).alias("state_name"),
    )

    if unique_match:
        city = deduplicate_on_key(city, "city_id")
        state = deduplicate_on_key(state, "state_id")

    return (
        customer.join(city, on="city_id", how="left")
        .join(state, on="state_id", how="left")
        .select(
            "customer_id",
            "customer_name",
            "address",
            "city_name",
            "state_name",
            "age",
            "gender",
            "email",
        )
    )
