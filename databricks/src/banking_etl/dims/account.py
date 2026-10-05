"""Port of Talend ``Load_DimAccount``: bronze.sqlserver_account -> silver.account -> gold.dim_account.

The Talend job is ``tMSSqlInput`` (``SELECT account_id, customer_id, account_type, balance,
date_opened, status FROM dbo.account``) -> ``tMap_1`` (straight 1:1 pass-through, no expressions,
no filters, no lookups) -> ``tMSSqlOutput`` into ``DWH.DimAccount`` (``CREATE_IF_NOT_EXISTS`` +
``INSERT``). The only transformations are the implicit type conversions done by SQL Server on
insert, reproduced here in silver:

- ``balance`` int -> ``DECIMAL(19,4)`` (``DimAccount.Balance MONEY``)
- ``date_opened`` datetime2 -> ``DATE`` (``DimAccount.DateOpened DATE``; time of day dropped)
- strings are not trimmed (``TRIM_ALL_COLUMN=false``) or re-cased: ``status`` stays ``active`` /
  ``terminated`` as in the source, which ``sp_BalancePerCustomer`` filters on.

``account_id`` is non-nullable in the Talend schema, so a NULL makes the Talend job die
("Null value in non-Nullable column"); :func:`transform_silver_account` raises instead.
The gold load is SCD-1 (``merge_scd1`` on ``AccountID``) per the migration conventions.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType

from banking_etl.config import BRONZE_SQLSERVER, GOLD_DIM_ACCOUNT, SILVER_ACCOUNT, Settings
from banking_etl.gold.merge import merge_scd1

SILVER_COLUMNS = (
    ("account_id", IntegerType()),
    ("customer_id", IntegerType()),
    ("account_type", StringType()),
    ("balance", DecimalType(19, 4)),
    ("date_opened", DateType()),
    ("status", StringType()),
)
AUDIT_COLUMNS = ("_ingested_at", "_source")

# tMap_1 output ``to_DimAccount``: silver column -> legacy DimAccount column.
DIM_ACCOUNT_MAPPING = {
    "account_id": "AccountID",
    "customer_id": "CustomerID",
    "account_type": "AccountType",
    "balance": "Balance",
    "date_opened": "DateOpened",
    "status": "Status",
}
DIM_ACCOUNT_KEYS = ("AccountID",)


def bronze_account_table(settings: Settings) -> str:
    return settings.table("bronze", BRONZE_SQLSERVER["account"])


def silver_account_table(settings: Settings) -> str:
    return settings.table("silver", SILVER_ACCOUNT)


def dim_account_table(settings: Settings) -> str:
    return settings.table("gold", GOLD_DIM_ACCOUNT)


def _validate_keys(df: DataFrame) -> None:
    check = df.agg(
        F.sum(F.when(F.col("account_id").isNull(), 1).otherwise(0)).alias("null_keys"),
        (F.count(F.lit(1)) - F.countDistinct("account_id")).alias("dup_keys"),
    ).first()
    if check["null_keys"]:
        raise ValueError(f"account: {check['null_keys']} row(s) with NULL account_id (non-nullable in Load_DimAccount)")
    if check["dup_keys"]:
        raise ValueError(f"account: {check['dup_keys']} duplicate account_id row(s) (DimAccount.AccountID is the PRIMARY KEY)")


def transform_silver_account(bronze_df: DataFrame, *, validate: bool = True) -> DataFrame:
    """Typed, conformed silver.account from bronze.sqlserver_account (one row per account_id)."""
    missing = [c for c, _ in SILVER_COLUMNS if c not in bronze_df.columns]
    if missing:
        raise ValueError(f"bronze account is missing columns {missing}")
    columns = [F.col(name).cast(dtype).alias(name) for name, dtype in SILVER_COLUMNS]
    columns += [F.col(c) for c in AUDIT_COLUMNS if c in bronze_df.columns]
    df = bronze_df.select(columns)
    if validate:
        _validate_keys(df)
    return df


def to_dim_account(silver_df: DataFrame) -> DataFrame:
    """tMap_1 ``to_DimAccount``: rename to the legacy PascalCase columns (no surrogate key)."""
    return silver_df.select([F.col(src).alias(dst) for src, dst in DIM_ACCOUNT_MAPPING.items()])


def load_silver_account(spark: SparkSession, settings: Settings) -> dict:
    """Rebuild silver.account from the latest bronze snapshot (full overwrite, like the bronze snapshot)."""
    source, target = bronze_account_table(settings), silver_account_table(settings)
    df = transform_silver_account(spark.table(source))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    return {"source": source, "target": target, "rows": spark.table(target).count()}


def ensure_dim_account(spark: SparkSession, settings: Settings) -> bool:
    """Talend ``TABLE_ACTION=CREATE_IF_NOT_EXISTS``: create the gold star schema if dim_account is missing.

    Returns True when the DDL had to run.
    """
    if spark.catalog.tableExists(dim_account_table(settings)):
        return False
    from banking_etl.gold.ddl import apply_star_schema

    apply_star_schema(spark, settings)
    return True


def load_dim_account(spark: SparkSession, settings: Settings, *, ensure_table: bool = True) -> dict:
    """SCD-1 MERGE of silver.account into gold.dim_account on AccountID. AccountKey is never written."""
    if ensure_table:
        ensure_dim_account(spark, settings)
    source, target = silver_account_table(settings), dim_account_table(settings)
    metrics = merge_scd1(spark, to_dim_account(spark.table(source)), target, DIM_ACCOUNT_KEYS)
    return {"source": source, "target": target, "rows": spark.table(target).count(), **metrics}
