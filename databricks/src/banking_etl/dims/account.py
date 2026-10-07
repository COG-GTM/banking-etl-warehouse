"""Port of the Talend job ``Load_DimAccount``.

Legacy job (talend_jobs/Load_DimAccount.zip, ``Load_DimAccount_0.1.item``)::

    tMSSqlInput  sample: SELECT account_id, customer_id, account_type, balance,
                                date_opened, status FROM dbo.account
    tMap         1:1 rename -> AccountID, CustomerID, AccountType, Balance,
                               DateOpened, Status
    tMSSqlOutput DWH.dbo.DimAccount  DATA_ACTION=INSERT  DIE_ON_ERROR=false

Implicit type conversions done by the SQL Server INSERT are made explicit:
``balance`` INT -> MONEY (``DECIMAL(19,4)``) and ``date_opened`` DATETIME2 ->
DATE (time of day dropped).

Databricks flow::

    bronze.sqlserver_account -> silver.account (typed, trimmed, deduped on account_id)
                             -> MERGE (SCD-1 upsert on account_id) -> gold.dim_account
"""

from __future__ import annotations

from typing import Dict, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from banking_etl.dims.branch import (
    _INGEST_ORDER_CANDIDATES,
    Layers,
    dedupe_on_key,
    ensure_table,
    scd1_merge,
    trimmed,
    try_cast,
    validate_steps,
    write_silver,
)

ACCOUNT_KEY = "account_id"
ACCOUNT_COLUMNS = (
    "account_id",
    "customer_id",
    "account_type",
    "balance",
    "date_opened",
    "status",
)
MONEY = "DECIMAL(19,4)"
GOLD_DIM_ACCOUNT_DDL = f"""
    account_id INT NOT NULL COMMENT 'Legacy DimAccount.AccountID (business key, PK)',
    customer_id INT COMMENT 'Legacy DimAccount.CustomerID',
    account_type STRING COMMENT 'Legacy DimAccount.AccountType VARCHAR(50)',
    balance {MONEY} COMMENT 'Legacy DimAccount.Balance MONEY',
    date_opened DATE COMMENT 'Legacy DimAccount.DateOpened DATE',
    status STRING COMMENT 'Legacy DimAccount.Status VARCHAR(50)'
"""


def transform_silver_account(bronze: DataFrame) -> DataFrame:
    """bronze.sqlserver_account -> silver.account columns (typed, trimmed, deduped)."""
    typed = bronze.select(
        try_cast("account_id", "INT").alias("account_id"),
        try_cast("customer_id", "INT").alias("customer_id"),
        trimmed("account_type").alias("account_type"),
        try_cast("balance", MONEY).alias("balance"),
        try_cast("date_opened", "DATE").alias("date_opened"),
        trimmed("status").alias("status"),
        *[F.col(c) for c in _INGEST_ORDER_CANDIDATES if c in bronze.columns],
    )
    return dedupe_on_key(typed, ACCOUNT_KEY, ACCOUNT_COLUMNS)


def build_silver_account(spark: SparkSession, layers: Layers) -> int:
    bronze = spark.table(layers.table("bronze", "sqlserver_account"))
    return write_silver(transform_silver_account(bronze), layers.table("silver", "account"))


def merge_gold_dim_account(spark: SparkSession, layers: Layers) -> Dict[str, int]:
    target = layers.table("gold", "dim_account")
    ensure_table(spark, target, GOLD_DIM_ACCOUNT_DDL, "Port of DWH.dbo.DimAccount (SCD-1)")
    metrics = scd1_merge(
        spark, layers.table("silver", "account"), target, ACCOUNT_KEY, ACCOUNT_COLUMNS
    )
    metrics["gold_rows"] = spark.table(target).count()
    return metrics


def run(spark: SparkSession, layers: Layers, steps: Sequence[str] = ("silver", "gold")) -> Dict:
    validate_steps(steps)
    result: Dict = {"entity": "account"}
    if "silver" in steps:
        result["silver_rows"] = build_silver_account(spark, layers)
    if "gold" in steps:
        result["gold_merge"] = merge_gold_dim_account(spark, layers)
    return result
