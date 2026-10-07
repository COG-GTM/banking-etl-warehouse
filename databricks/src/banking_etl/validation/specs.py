"""Table specs shared by the legacy baseline builder, the reference transform and the harness.

Legacy names come from ``sql_scripts/01_create_tables.sql``; gold names follow the
shared migration convention (snake_case of the legacy column names).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import types as T

MONEY = T.DecimalType(19, 4)


@dataclass(frozen=True)
class Column:
    legacy: str
    gold: str
    dtype: T.DataType


@dataclass(frozen=True)
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str
    enforced_in_legacy: bool = True


@dataclass(frozen=True)
class Aggregate:
    """A grouped (or global) aggregate compared between legacy and gold."""

    name: str
    expr: str
    group_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class TableSpec:
    gold: str
    legacy: str
    columns: tuple[Column, ...]
    pk: tuple[str, ...]
    fks: tuple[ForeignKey, ...] = ()
    aggregates: tuple[Aggregate, ...] = field(default_factory=tuple)

    @property
    def gold_columns(self) -> list[str]:
        return [c.gold for c in self.columns]

    @property
    def legacy_to_gold(self) -> dict[str, str]:
        return {c.legacy: c.gold for c in self.columns}

    def gold_schema(self) -> T.StructType:
        return T.StructType([T.StructField(c.gold, c.dtype, True) for c in self.columns])

    def legacy_schema(self) -> T.StructType:
        return T.StructType([T.StructField(c.legacy, c.dtype, True) for c in self.columns])


DIM_BRANCH = TableSpec(
    gold="dim_branch",
    legacy="DimBranch",
    columns=(
        Column("BranchID", "branch_id", T.IntegerType()),
        Column("BranchName", "branch_name", T.StringType()),
        Column("BranchLocation", "branch_location", T.StringType()),
    ),
    pk=("branch_id",),
    aggregates=(Aggregate("row_count", "count(*)"),),
)

DIM_ACCOUNT = TableSpec(
    gold="dim_account",
    legacy="DimAccount",
    columns=(
        Column("AccountID", "account_id", T.IntegerType()),
        Column("CustomerID", "customer_id", T.IntegerType()),
        Column("AccountType", "account_type", T.StringType()),
        Column("Balance", "balance", MONEY),
        Column("DateOpened", "date_opened", T.DateType()),
        Column("Status", "status", T.StringType()),
    ),
    pk=("account_id",),
    fks=(ForeignKey("customer_id", "dim_customer", "customer_id", enforced_in_legacy=False),),
    aggregates=(
        Aggregate("sum_balance", "cast(sum(balance) as decimal(38,4))"),
        Aggregate("min_date_opened", "min(date_opened)"),
        Aggregate("max_date_opened", "max(date_opened)"),
        Aggregate("count_by_status", "count(*)", ("status",)),
        Aggregate("sum_balance_by_account_type", "cast(sum(balance) as decimal(38,4))", ("account_type",)),
    ),
)

DIM_CUSTOMER = TableSpec(
    gold="dim_customer",
    legacy="DimCustomer",
    columns=(
        Column("CustomerID", "customer_id", T.IntegerType()),
        Column("CustomerName", "customer_name", T.StringType()),
        Column("Address", "address", T.StringType()),
        Column("CityName", "city_name", T.StringType()),
        Column("StateName", "state_name", T.StringType()),
        Column("Age", "age", T.IntegerType()),
        Column("Gender", "gender", T.StringType()),
        Column("Email", "email", T.StringType()),
    ),
    pk=("customer_id",),
    aggregates=(
        Aggregate("sum_age", "sum(age)"),
        Aggregate("min_age", "min(age)"),
        Aggregate("max_age", "max(age)"),
        Aggregate("count_by_gender", "count(*)", ("gender",)),
        Aggregate("count_by_state", "count(*)", ("state_name",)),
    ),
)

FACT_TRANSACTION = TableSpec(
    gold="fact_transaction",
    legacy="FactTransaction",
    columns=(
        Column("TransactionID", "transaction_id", T.IntegerType()),
        Column("AccountID", "account_id", T.IntegerType()),
        Column("TransactionDate", "transaction_date", T.TimestampType()),
        Column("Amount", "amount", MONEY),
        Column("TransactionType", "transaction_type", T.StringType()),
        Column("BranchID", "branch_id", T.IntegerType()),
    ),
    pk=("transaction_id",),
    fks=(
        ForeignKey("account_id", "dim_account", "account_id"),
        ForeignKey("branch_id", "dim_branch", "branch_id"),
    ),
    aggregates=(
        Aggregate("sum_amount", "cast(sum(amount) as decimal(38,4))"),
        Aggregate("min_transaction_date", "min(transaction_date)"),
        Aggregate("max_transaction_date", "max(transaction_date)"),
        Aggregate("sum_amount_by_type", "cast(sum(amount) as decimal(38,4))", ("transaction_type",)),
        Aggregate("count_by_type", "count(*)", ("transaction_type",)),
        Aggregate("sum_amount_by_branch", "cast(sum(amount) as decimal(38,4))", ("branch_id",)),
        Aggregate("net_flow_by_account",
                  "cast(sum(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE -amount END) as decimal(38,4))",
                  ("account_id",)),
    ),
)

# Talend run order (README): DimBranch -> DimAccount -> DimCustomer -> FactTransaction.
GOLD_TABLES: tuple[TableSpec, ...] = (DIM_BRANCH, DIM_ACCOUNT, DIM_CUSTOMER, FACT_TRANSACTION)
SPECS: dict[str, TableSpec] = {s.gold: s for s in GOLD_TABLES}


# Source snapshots exported from the `sample` DB plus the two flat files.
SOURCE_SCHEMAS: dict[str, T.StructType] = {
    "sqlserver_branch": T.StructType([
        T.StructField("branch_id", T.IntegerType()),
        T.StructField("branch_name", T.StringType()),
        T.StructField("branch_location", T.StringType()),
    ]),
    "sqlserver_account": T.StructType([
        T.StructField("account_id", T.IntegerType()),
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("account_type", T.StringType()),
        T.StructField("balance", T.IntegerType()),
        T.StructField("date_opened", T.TimestampType()),
        T.StructField("status", T.StringType()),
    ]),
    "sqlserver_customer": T.StructType([
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("customer_name", T.StringType()),
        T.StructField("address", T.StringType()),
        T.StructField("city_id", T.IntegerType()),
        T.StructField("age", T.StringType()),
        T.StructField("gender", T.StringType()),
        T.StructField("email", T.StringType()),
    ]),
    "sqlserver_city": T.StructType([
        T.StructField("city_id", T.IntegerType()),
        T.StructField("city_name", T.StringType()),
        T.StructField("state_id", T.IntegerType()),
    ]),
    "sqlserver_state": T.StructType([
        T.StructField("state_id", T.IntegerType()),
        T.StructField("state_name", T.StringType()),
    ]),
    "sqlserver_transaction_db": T.StructType([
        T.StructField("transaction_id", T.IntegerType()),
        T.StructField("account_id", T.IntegerType()),
        T.StructField("transaction_date", T.TimestampType()),
        T.StructField("amount", T.IntegerType()),
        T.StructField("transaction_type", T.StringType()),
        T.StructField("branch_id", T.IntegerType()),
    ]),
    "file_transaction_excel": T.StructType([
        T.StructField("transaction_id", T.IntegerType()),
        T.StructField("account_id", T.IntegerType()),
        T.StructField("transaction_date", T.TimestampType()),
        T.StructField("amount", T.IntegerType()),
        T.StructField("transaction_type", T.StringType()),
        T.StructField("branch_id", T.IntegerType()),
    ]),
    # Raw CSV keeps its dd-MM-yyyy HH:mm:ss string dates; the transform parses them.
    "file_transaction_csv": T.StructType([
        T.StructField("transaction_id", T.IntegerType()),
        T.StructField("account_id", T.IntegerType()),
        T.StructField("transaction_date", T.StringType()),
        T.StructField("amount", T.IntegerType()),
        T.StructField("transaction_type", T.StringType()),
        T.StructField("branch_id", T.IntegerType()),
    ]),
}

TALEND_REJECTS_SCHEMA = T.StructType([
    T.StructField("job", T.StringType()),
    T.StructField("transaction_id", T.IntegerType()),
    T.StructField("account_id", T.IntegerType()),
    T.StructField("branch_id", T.IntegerType()),
    T.StructField("source", T.StringType()),
    T.StructField("reason", T.StringType()),
])

# Stored-procedure parity: parameter sets executed on SQL Server and replayed on Delta.
DAILY_TRANSACTION_PARAMS: tuple[tuple[str, str], ...] = (
    ("2024-01-18", "2024-01-20"),  # README example
    ("2022-01-01", "2024-12-31"),  # full history
    ("2024-01-22", "2024-01-22"),  # single day (final CSV batch)
    ("2023-01-01", "2023-12-31"),  # empty window
)

BALANCE_PER_CUSTOMER_PARAMS: tuple[str, ...] = (
    "Shelly", "shelly juwita", "Ani", "Rio Wijaya", "a", "Malika", "Ratna", "%", "No Such Customer",
)

DAILY_TRANSACTION_SCHEMA = T.StructType([
    T.StructField("start_date", T.StringType()),
    T.StructField("end_date", T.StringType()),
    T.StructField("Date", T.DateType()),
    T.StructField("TotalTransactions", T.IntegerType()),
    T.StructField("TotalAmount", MONEY),
])

BALANCE_PER_CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_name_param", T.StringType()),
    T.StructField("CustomerName", T.StringType()),
    T.StructField("AccountType", T.StringType()),
    T.StructField("InitialBalance", MONEY),
    T.StructField("CurrentBalance", MONEY),
])
