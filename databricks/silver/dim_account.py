"""Port of the Talend job `Load_DimAccount`.

Legacy flow (IDX_INTERNSHIP/process/Load_DimAccount_0.1.item):

    tMSSqlInput "account"  ->  tMap_1 (to_DimAccount)  ->  tMSSqlOutput DimAccount

tMap expressions, all pass-through:

    AccountID   <- row1.account_id   (id_Integer)
    CustomerID  <- row1.customer_id  (id_Integer)
    AccountType <- row1.account_type (id_String)
    Balance     <- row1.balance      (id_Integer in the tMap schema; MONEY in SQL Server)
    DateOpened  <- row1.date_opened  (id_Date)
    Status      <- row1.status       (id_String)

`Balance` is widened to DECIMAL(19,4) here to match the agreed gold contract; the
Talend schema truncated MONEY to a Java Integer. See the migration doc.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import (
    DATE_FORMATS,
    DimensionConfig,
    clean_string,
    drop_ingest_metadata,
    to_date,
    to_decimal,
    to_int,
)

DIM_ACCOUNT_CONFIG = DimensionConfig(
    bronze_tables={"account": "sample_account"},
    silver_table="dim_account",
    gold_table="dim_account",
    business_key="account_id",
)


def transform_dim_account(
    bronze_account: DataFrame,
    date_formats: tuple[str, ...] = DATE_FORMATS,
) -> DataFrame:
    """Conform `bronze.sample_account` into the silver `dim_account` contract."""
    return drop_ingest_metadata(bronze_account).select(
        to_int(F.col("account_id")).alias("account_id"),
        to_int(F.col("customer_id")).alias("customer_id"),
        clean_string(F.col("account_type")).alias("account_type"),
        to_decimal(F.col("balance")).alias("balance"),
        to_date(F.col("date_opened"), date_formats).alias("date_opened"),
        clean_string(F.col("status")).alias("status"),
    )
