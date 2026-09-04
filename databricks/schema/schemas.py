"""PySpark ``StructType`` definitions for every bronze / silver / gold table.

This module is the single source of truth for column names and types. The
``*_ddl.sql`` files in this directory must stay in sync with it (enforced by
``tests/schema``).
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# T-SQL -> Spark SQL type mapping used when translating sql_scripts/01_create_tables.sql.
DDL_TYPE_MAP: dict[str, str] = {
    "INT": "INT",
    "VARCHAR(n)": "STRING",
    "MONEY": "DECIMAL(19,4)",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
}

MONEY = DecimalType(19, 4)

_META_BRONZE = [
    StructField("_ingest_ts", TimestampType(), True),
    StructField("_source_file", StringType(), True),
]
_META_SILVER = [StructField("_ingest_ts", TimestampType(), True)]


def _fields(*cols: tuple) -> list[StructField]:
    return [StructField(name, dtype, nullable) for name, dtype, nullable in cols]


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
BRONZE_CUSTOMER = StructType(
    _fields(
        ("customer_id", IntegerType(), True),
        ("customer_name", StringType(), True),
        ("address", StringType(), True),
        ("city_id", IntegerType(), True),
        ("age", IntegerType(), True),
        ("gender", StringType(), True),
        ("email", StringType(), True),
    )
    + _META_BRONZE
)

BRONZE_CITY = StructType(
    _fields(
        ("city_id", IntegerType(), True),
        ("city_name", StringType(), True),
        ("state_id", IntegerType(), True),
    )
    + _META_BRONZE
)

BRONZE_STATE = StructType(
    _fields(
        ("state_id", IntegerType(), True),
        ("state_name", StringType(), True),
    )
    + _META_BRONZE
)

BRONZE_ACCOUNT = StructType(
    _fields(
        ("account_id", IntegerType(), True),
        ("customer_id", IntegerType(), True),
        ("account_type", StringType(), True),
        ("balance", MONEY, True),
        ("date_opened", DateType(), True),
        ("status", StringType(), True),
    )
    + _META_BRONZE
)

BRONZE_BRANCH = StructType(
    _fields(
        ("branch_id", IntegerType(), True),
        ("branch_name", StringType(), True),
        ("branch_location", StringType(), True),
    )
    + _META_BRONZE
)

_TRANSACTION_TYPED = [
    ("transaction_id", IntegerType(), True),
    ("account_id", IntegerType(), True),
    ("transaction_date", TimestampType(), True),
    ("amount", MONEY, True),
    ("transaction_type", StringType(), True),
    ("branch_id", IntegerType(), True),
]

BRONZE_TRANSACTION_DB = StructType(_fields(*_TRANSACTION_TYPED) + _META_BRONZE)

BRONZE_TRANSACTION_CSV = StructType(
    _fields(
        ("transaction_id", StringType(), True),
        ("account_id", StringType(), True),
        ("transaction_date", StringType(), True),
        ("amount", StringType(), True),
        ("transaction_type", StringType(), True),
        ("branch_id", StringType(), True),
    )
    + _META_BRONZE
)

BRONZE_TRANSACTION_EXCEL = StructType(_fields(*_TRANSACTION_TYPED) + _META_BRONZE)

BRONZE_SCHEMAS: dict[str, StructType] = {
    "customer": BRONZE_CUSTOMER,
    "city": BRONZE_CITY,
    "state": BRONZE_STATE,
    "account": BRONZE_ACCOUNT,
    "branch": BRONZE_BRANCH,
    "transaction_db": BRONZE_TRANSACTION_DB,
    "transaction_csv": BRONZE_TRANSACTION_CSV,
    "transaction_excel": BRONZE_TRANSACTION_EXCEL,
}

# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
SILVER_CUSTOMER = StructType(
    _fields(
        ("customer_id", IntegerType(), False),
        ("customer_name", StringType(), True),
        ("address", StringType(), True),
        ("city_id", IntegerType(), True),
        ("age", IntegerType(), True),
        ("gender", StringType(), True),
        ("email", StringType(), True),
    )
    + _META_SILVER
)

SILVER_CITY = StructType(
    _fields(
        ("city_id", IntegerType(), False),
        ("city_name", StringType(), True),
        ("state_id", IntegerType(), True),
    )
    + _META_SILVER
)

SILVER_STATE = StructType(
    _fields(
        ("state_id", IntegerType(), False),
        ("state_name", StringType(), True),
    )
    + _META_SILVER
)

SILVER_ACCOUNT = StructType(
    _fields(
        ("account_id", IntegerType(), False),
        ("customer_id", IntegerType(), True),
        ("account_type", StringType(), True),
        ("balance", MONEY, True),
        ("date_opened", DateType(), True),
        ("status", StringType(), True),
    )
    + _META_SILVER
)

SILVER_BRANCH = StructType(
    _fields(
        ("branch_id", IntegerType(), False),
        ("branch_name", StringType(), True),
        ("branch_location", StringType(), True),
    )
    + _META_SILVER
)

SILVER_TRANSACTION = StructType(
    _fields(
        ("transaction_id", IntegerType(), False),
        ("account_id", IntegerType(), True),
        ("transaction_date", TimestampType(), True),
        ("amount", MONEY, True),
        ("transaction_type", StringType(), True),
        ("branch_id", IntegerType(), True),
        ("_source_system", StringType(), True),
    )
    + _META_SILVER
)

SILVER_SCHEMAS: dict[str, StructType] = {
    "customer": SILVER_CUSTOMER,
    "city": SILVER_CITY,
    "state": SILVER_STATE,
    "account": SILVER_ACCOUNT,
    "branch": SILVER_BRANCH,
    "transaction": SILVER_TRANSACTION,
}

# ---------------------------------------------------------------------------
# Gold (star schema, mirrors sql_scripts/01_create_tables.sql)
# ---------------------------------------------------------------------------
DIM_ACCOUNT = StructType(
    _fields(
        ("AccountID", IntegerType(), False),
        ("CustomerID", IntegerType(), True),
        ("AccountType", StringType(), True),
        ("Balance", MONEY, True),
        ("DateOpened", DateType(), True),
        ("Status", StringType(), True),
    )
)

DIM_BRANCH = StructType(
    _fields(
        ("BranchID", IntegerType(), False),
        ("BranchName", StringType(), True),
        ("BranchLocation", StringType(), True),
    )
)

DIM_CUSTOMER = StructType(
    _fields(
        ("CustomerID", IntegerType(), False),
        ("CustomerName", StringType(), True),
        ("Address", StringType(), True),
        ("CityName", StringType(), True),
        ("StateName", StringType(), True),
        ("Age", IntegerType(), True),
        ("Gender", StringType(), True),
        ("Email", StringType(), True),
    )
)

FACT_TRANSACTION = StructType(
    _fields(
        ("TransactionID", IntegerType(), False),
        ("AccountID", IntegerType(), True),
        ("TransactionDate", TimestampType(), True),
        ("Amount", MONEY, True),
        ("TransactionType", StringType(), True),
        ("BranchID", IntegerType(), True),
    )
)

GOLD_SCHEMAS: dict[str, StructType] = {
    "DimAccount": DIM_ACCOUNT,
    "DimBranch": DIM_BRANCH,
    "DimCustomer": DIM_CUSTOMER,
    "FactTransaction": FACT_TRANSACTION,
}

ALL_SCHEMAS: dict[str, dict[str, StructType]] = {
    "bronze": BRONZE_SCHEMAS,
    "silver": SILVER_SCHEMAS,
    "gold": GOLD_SCHEMAS,
}
