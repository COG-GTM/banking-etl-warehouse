"""Source and warehouse schemas.

Warehouse tables mirror ``sql_scripts/01_create_tables.sql`` with T-SQL types mapped to
Delta/Spark types: INT -> INT, MONEY -> DECIMAL(19,4), DATE -> DATE,
DATETIME -> TIMESTAMP, VARCHAR(n) -> VARCHAR(n) (length enforced by Delta on write).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql.types import (
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY = DecimalType(19, 4)

_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")
_VARCHAR = re.compile(r"^VARCHAR\(\d+\)$")


def spark_type(sql_type: str) -> DataType:
    normalized = sql_type.upper().replace(" ", "")
    if normalized == "INT":
        return IntegerType()
    if normalized == "DATE":
        return DateType()
    if normalized == "TIMESTAMP":
        return TimestampType()
    if normalized in ("STRING",) or _VARCHAR.match(normalized):
        return StringType()
    decimal = _DECIMAL.match(normalized)
    if decimal:
        return DecimalType(int(decimal.group(1)), int(decimal.group(2)))
    raise ValueError(f"Unsupported SQL type: {sql_type}")


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    nullable: bool = True

    @property
    def data_type(self) -> DataType:
        return spark_type(self.sql_type)


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    comment: str = ""

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def struct(self) -> StructType:
        return StructType([StructField(c.name, c.data_type, c.nullable) for c in self.columns])

    def ddl_columns(self) -> str:
        return ", ".join(f"{c.name} {c.sql_type}{'' if c.nullable else ' NOT NULL'}" for c in self.columns)


DIM_ACCOUNT = TableSpec(
    name="dim_account",
    columns=(
        Column("AccountID", "INT", nullable=False),
        Column("CustomerID", "INT"),
        Column("AccountType", "VARCHAR(50)"),
        Column("Balance", "DECIMAL(19,4)"),
        Column("DateOpened", "DATE"),
        Column("Status", "VARCHAR(50)"),
    ),
    primary_key=("AccountID",),
    comment="Account dimension (legacy DWH.dbo.DimAccount)",
)

DIM_BRANCH = TableSpec(
    name="dim_branch",
    columns=(
        Column("BranchID", "INT", nullable=False),
        Column("BranchName", "VARCHAR(100)"),
        Column("BranchLocation", "VARCHAR(255)"),
    ),
    primary_key=("BranchID",),
    comment="Branch dimension (legacy DWH.dbo.DimBranch)",
)

DIM_CUSTOMER = TableSpec(
    name="dim_customer",
    columns=(
        Column("CustomerID", "INT", nullable=False),
        Column("CustomerName", "VARCHAR(100)"),
        Column("Address", "VARCHAR(255)"),
        Column("CityName", "VARCHAR(100)"),
        Column("StateName", "VARCHAR(100)"),
        Column("Age", "INT"),
        Column("Gender", "VARCHAR(10)"),
        Column("Email", "VARCHAR(100)"),
    ),
    primary_key=("CustomerID",),
    comment="Customer dimension denormalised from customer/city/state (legacy DWH.dbo.DimCustomer)",
)

FACT_TRANSACTION = TableSpec(
    name="fact_transaction",
    columns=(
        Column("TransactionID", "INT", nullable=False),
        Column("AccountID", "INT"),
        Column("TransactionDate", "TIMESTAMP"),
        Column("Amount", "DECIMAL(19,4)"),
        Column("TransactionType", "VARCHAR(50)"),
        Column("BranchID", "INT"),
    ),
    primary_key=("TransactionID",),
    comment="Transaction fact unified from SQL Server, Excel and CSV (legacy DWH.dbo.FactTransaction)",
)

WAREHOUSE_TABLES = (DIM_BRANCH, DIM_ACCOUNT, DIM_CUSTOMER, FACT_TRANSACTION)


def _struct(*fields: tuple[str, DataType, bool]) -> StructType:
    return StructType([StructField(name, dtype, nullable) for name, dtype, nullable in fields])


# Source schemas: the explicit column lists of the Talend tMSSqlInput queries.
SRC_BRANCH = _struct(
    ("branch_id", IntegerType(), False),
    ("branch_name", StringType(), True),
    ("branch_location", StringType(), True),
)

SRC_ACCOUNT = _struct(
    ("account_id", IntegerType(), False),
    ("customer_id", IntegerType(), True),
    ("account_type", StringType(), True),
    ("balance", MONEY, True),
    ("date_opened", TimestampType(), True),
    ("status", StringType(), True),
)

SRC_CUSTOMER = _struct(
    ("customer_id", IntegerType(), False),
    ("customer_name", StringType(), True),
    ("address", StringType(), True),
    ("city_id", IntegerType(), True),
    ("age", StringType(), True),
    ("gender", StringType(), True),
    ("email", StringType(), True),
)

SRC_CITY = _struct(
    ("city_id", IntegerType(), False),
    ("city_name", StringType(), True),
    ("state_id", IntegerType(), True),
)

SRC_STATE = _struct(
    ("state_id", IntegerType(), False),
    ("state_name", StringType(), True),
)

# Shared by transaction_db (SQL Server), transaction_excel.xlsx and transaction_csv.csv.
SRC_TRANSACTION = _struct(
    ("transaction_id", IntegerType(), True),
    ("account_id", IntegerType(), True),
    ("transaction_date", TimestampType(), True),
    ("amount", MONEY, True),
    ("transaction_type", StringType(), True),
    ("branch_id", IntegerType(), True),
)
