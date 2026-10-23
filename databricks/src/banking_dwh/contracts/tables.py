"""Schema contracts for every table in the banking_dwh Lakehouse (Ticket 2).

This module is the single source of truth that parallel tickets code against:

* Bronze contracts: the tables Tickets 3/4/5 must produce and Tickets 6/7/8 read.
* Silver contracts: the star schema translated from ``sql_scripts/01_create_tables.sql``
  that Tickets 6/7/8 MERGE into.
* Gold contracts: the analytics objects Ticket 9 must expose.
* Ops contracts: data-quality / reconciliation result tables (Tickets 2 and 10).

Type mapping from SQL Server: MONEY -> DECIMAL(19,4), DATETIME/DATETIME2 -> TIMESTAMP,
DATE -> DATE, VARCHAR(n) -> STRING, INT -> INT.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    comment: str = ""
    legacy_name: str | None = None  # column name in the legacy SQL Server object


@dataclass(frozen=True)
class ForeignKey:
    name: str
    columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableContract:
    layer: str
    name: str
    columns: tuple[Column, ...]
    comment: str
    primary_key: tuple[str, ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    legacy_name: str | None = None
    owner_ticket: int | None = None
    cluster_by: tuple[str, ...] = ()
    properties: dict[str, str] = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def business_columns(self) -> list[str]:
        return [c.name for c in self.columns if not c.name.startswith("_")]

    def ddl_schema(self) -> str:
        """Spark DDL schema string, e.g. for spark.createDataFrame / from_json."""
        return ", ".join(
            f"{c.name} {c.type}{'' if c.nullable else ' NOT NULL'}" for c in self.columns
        )


# --------------------------------------------------------------------------------------
# Bronze: source-shaped, append-only. Metadata columns are prefixed with "_".
# --------------------------------------------------------------------------------------
_JDBC_META = (
    Column("_ingested_at", "TIMESTAMP", False, "Ingestion timestamp (UTC)."),
    Column("_batch_id", "STRING", False, "Extraction batch id; all tables in one run share it."),
)
_FILE_META = (
    Column("_rescued_data", "STRING", True, "Values that failed schema parsing (Auto Loader rescue)."),
    Column("_source_file", "STRING", False, "Full path of the landed source file."),
    Column("_ingested_at", "TIMESTAMP", False, "Ingestion timestamp (UTC)."),
)

# Source DDL verified against a restore of data_sources/sample.bak (SQL Server 2022).
BRONZE_SQLSERVER_BRANCH = TableContract(
    "bronze", "sqlserver_branch",
    (
        Column("branch_id", "INT", False, "dbo.branch.branch_id (int, PK)"),
        Column("branch_name", "STRING", True, "varchar(50)"),
        Column("branch_location", "STRING", True, "varchar(50)"),
        *_JDBC_META,
    ),
    "JDBC extract of sample.dbo.branch (5 rows).", owner_ticket=3,
)
BRONZE_SQLSERVER_ACCOUNT = TableContract(
    "bronze", "sqlserver_account",
    (
        Column("account_id", "INT", False, "dbo.account.account_id (int, PK)"),
        Column("customer_id", "INT", True, "FK -> customer"),
        Column("account_type", "STRING", True, "varchar(10)"),
        Column("balance", "INT", True, "int in source, DECIMAL(19,4) in silver"),
        Column("date_opened", "TIMESTAMP", True, "datetime2 in source; DATE in silver"),
        Column("status", "STRING", True, "varchar(10), e.g. 'active'"),
        *_JDBC_META,
    ),
    "JDBC extract of sample.dbo.account (21 rows).", owner_ticket=3,
)
BRONZE_SQLSERVER_CUSTOMER = TableContract(
    "bronze", "sqlserver_customer",
    (
        Column("customer_id", "INT", False, "dbo.customer.customer_id (int, PK)"),
        Column("customer_name", "STRING", True, "varchar(50)"),
        Column("address", "STRING", True, "varchar(max)"),
        Column("city_id", "INT", True, "FK -> city"),
        Column("age", "STRING", True, "varchar(3) in source; INT in silver"),
        Column("gender", "STRING", True, "varchar(10)"),
        Column("email", "STRING", True, "varchar(50)"),
        *_JDBC_META,
    ),
    "JDBC extract of sample.dbo.customer (20 rows).", owner_ticket=3,
)
BRONZE_SQLSERVER_CITY = TableContract(
    "bronze", "sqlserver_city",
    (
        Column("city_id", "INT", False, "dbo.city.city_id (int, PK)"),
        Column("city_name", "STRING", True, "varchar(50)"),
        Column("state_id", "INT", False, "FK -> state"),
        *_JDBC_META,
    ),
    "JDBC extract of sample.dbo.city (52 rows).", owner_ticket=3,
)
BRONZE_SQLSERVER_STATE = TableContract(
    "bronze", "sqlserver_state",
    (
        Column("state_id", "INT", False, "dbo.state.state_id (int, PK)"),
        Column("state_name", "STRING", True, "varchar(50)"),
        *_JDBC_META,
    ),
    "JDBC extract of sample.dbo.state (9 rows).", owner_ticket=3,
)
BRONZE_SQLSERVER_TRANSACTION = TableContract(
    "bronze", "sqlserver_transaction_db",
    (
        Column("transaction_id", "INT", False, "dbo.transaction_db.transaction_id (int, PK)"),
        Column("account_id", "INT", True, "FK -> account"),
        Column("transaction_date", "TIMESTAMP", True, "datetime2"),
        Column("amount", "INT", True, "int in source, DECIMAL(19,4) in silver"),
        Column("transaction_type", "STRING", True, "varchar(50)"),
        Column("branch_id", "INT", True, "FK -> branch"),
        *_JDBC_META,
    ),
    "JDBC extract of sample.dbo.transaction_db (10 rows).", owner_ticket=3,
)

_FILE_TXN_COLUMNS = (
    Column("transaction_id", "INT", True, "transaction_id"),
    Column("account_id", "INT", True, "account_id"),
    Column("transaction_date", "TIMESTAMP", True, "Parsed source timestamp (CSV format dd-MM-yyyy HH:mm:ss)."),
    Column("amount", "DECIMAL(19,4)", True, "amount"),
    Column("transaction_type", "STRING", True, "Deposit / Withdrawal / Transfer / Payment"),
    Column("branch_id", "INT", True, "branch_id"),
)
BRONZE_FILE_TRANSACTION_CSV = TableContract(
    "bronze", "file_transaction_csv", (*_FILE_TXN_COLUMNS, *_FILE_META),
    "Auto Loader ingestion of data_sources/transaction_csv.csv (12 rows).", owner_ticket=4,
)
BRONZE_FILE_TRANSACTION_EXCEL = TableContract(
    "bronze", "file_transaction_excel", (*_FILE_TXN_COLUMNS, *_FILE_META),
    "Ingestion of data_sources/transaction_excel.xlsx, Sheet1 (7 rows).", owner_ticket=5,
)

# --------------------------------------------------------------------------------------
# Silver: star schema (sql_scripts/01_create_tables.sql). Natural keys from the source
# are kept as primary keys, exactly like the legacy DWH.
# --------------------------------------------------------------------------------------
_SILVER_META = (Column("_loaded_at", "TIMESTAMP", True, "Last time the row was written by the load job."),)

DIM_ACCOUNT = TableContract(
    "silver", "dim_account",
    (
        Column("account_id", "INT", False, "Account natural key.", "AccountID"),
        Column("customer_id", "INT", True, "Owning customer.", "CustomerID"),
        Column("account_type", "STRING", True, "VARCHAR(50)", "AccountType"),
        Column("balance", "DECIMAL(19,4)", True, "MONEY: opening balance.", "Balance"),
        Column("date_opened", "DATE", True, "DATE", "DateOpened"),
        Column("status", "STRING", True, "VARCHAR(50), e.g. active", "Status"),
        *_SILVER_META,
    ),
    "Account dimension (legacy DWH.dbo.DimAccount, Talend Load_DimAccount).",
    primary_key=("account_id",), legacy_name="DimAccount", owner_ticket=6,
)
DIM_BRANCH = TableContract(
    "silver", "dim_branch",
    (
        Column("branch_id", "INT", False, "Branch natural key.", "BranchID"),
        Column("branch_name", "STRING", True, "VARCHAR(100)", "BranchName"),
        Column("branch_location", "STRING", True, "VARCHAR(255)", "BranchLocation"),
        *_SILVER_META,
    ),
    "Branch dimension (legacy DWH.dbo.DimBranch, Talend Load_DimBranch).",
    primary_key=("branch_id",), legacy_name="DimBranch", owner_ticket=6,
)
DIM_CUSTOMER = TableContract(
    "silver", "dim_customer",
    (
        Column("customer_id", "INT", False, "Customer natural key.", "CustomerID"),
        Column("customer_name", "STRING", True, "VARCHAR(100), upper-cased.", "CustomerName"),
        Column("address", "STRING", True, "VARCHAR(255), upper-cased.", "Address"),
        Column("city_name", "STRING", True, "VARCHAR(100), from city (left join).", "CityName"),
        Column("state_name", "STRING", True, "VARCHAR(100), from state (left join).", "StateName"),
        Column("age", "INT", True, "INT", "Age"),
        Column("gender", "STRING", True, "VARCHAR(10), upper-cased.", "Gender"),
        Column("email", "STRING", True, "VARCHAR(100), unchanged.", "Email"),
        *_SILVER_META,
    ),
    "Customer dimension: customer + city + state (legacy DWH.dbo.DimCustomer, Talend Load_DimCustomer).",
    primary_key=("customer_id",), legacy_name="DimCustomer", owner_ticket=7,
)
FACT_TRANSACTION = TableContract(
    "silver", "fact_transaction",
    (
        Column("transaction_id", "INT", False, "Transaction natural key (deduplicated).", "TransactionID"),
        Column("account_id", "INT", True, "FK -> dim_account.", "AccountID"),
        Column("transaction_date", "TIMESTAMP", True, "DATETIME", "TransactionDate"),
        Column("amount", "DECIMAL(19,4)", True, "MONEY", "Amount"),
        Column("transaction_type", "STRING", True, "VARCHAR(50)", "TransactionType"),
        Column("branch_id", "INT", True, "FK -> dim_branch.", "BranchID"),
        Column("_source_system", "STRING", True, "Winning source stream: sqlserver / csv / excel."),
        *_SILVER_META,
    ),
    "Transaction fact: SQL Server + CSV + Excel streams unioned and deduplicated on transaction_id "
    "(legacy DWH.dbo.FactTransaction, Talend Load_FactTransaction).",
    primary_key=("transaction_id",),
    foreign_keys=(
        ForeignKey("FK_FactTransaction_DimAccount", ("account_id",), "dim_account", ("account_id",)),
        ForeignKey("FK_FactTransaction_DimBranch", ("branch_id",), "dim_branch", ("branch_id",)),
    ),
    legacy_name="FactTransaction", owner_ticket=8, cluster_by=("transaction_date",),
)

# --------------------------------------------------------------------------------------
# Ops
# --------------------------------------------------------------------------------------
DQ_RESULTS = TableContract(
    "ops", "dq_results",
    (
        Column("run_id", "STRING", False, "Caller-supplied run id (job run id or uuid)."),
        Column("check_name", "STRING", False, "e.g. FK_FactTransaction_DimAccount, pk_unique.dim_account"),
        Column("check_type", "STRING", False, "referential_integrity / pk_not_null / pk_unique / row_count / reconciliation"),
        Column("table_name", "STRING", False, "Fully qualified table checked."),
        Column("passed", "BOOLEAN", False, ""),
        Column("failed_rows", "BIGINT", False, "Violating rows (0 when passed)."),
        Column("details", "STRING", True, "JSON with check-specific details / samples."),
        Column("checked_at", "TIMESTAMP", False, ""),
    ),
    "Data-quality check results (Ticket 2 RI/PK checks; Ticket 10 adds row-count and reconciliation checks).",
    owner_ticket=2,
)

BRONZE_TABLES = (
    BRONZE_SQLSERVER_BRANCH, BRONZE_SQLSERVER_ACCOUNT, BRONZE_SQLSERVER_CUSTOMER,
    BRONZE_SQLSERVER_CITY, BRONZE_SQLSERVER_STATE, BRONZE_SQLSERVER_TRANSACTION,
    BRONZE_FILE_TRANSACTION_CSV, BRONZE_FILE_TRANSACTION_EXCEL,
)
# Creation order matters: referenced dims before the fact.
SILVER_TABLES = (DIM_BRANCH, DIM_ACCOUNT, DIM_CUSTOMER, FACT_TRANSACTION)
OPS_TABLES = (DQ_RESULTS,)
ALL_TABLES = BRONZE_TABLES + SILVER_TABLES + OPS_TABLES


def get_contract(layer: str, name: str) -> TableContract:
    for t in ALL_TABLES:
        if t.layer == layer and t.name == name:
            return t
    raise KeyError(f"No contract for {layer}.{name}")


# --------------------------------------------------------------------------------------
# Gold: objects Ticket 9 must create (output contracts for sp_DailyTransaction and
# sp_BalancePerCustomer). Column order matches the legacy result sets.
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class GoldObject:
    name: str
    kind: str  # view | table_function
    parameters: tuple[Column, ...]
    output: tuple[Column, ...]
    replaces: str
    comment: str


GOLD_OBJECTS = (
    GoldObject(
        "v_daily_transaction", "view", (),
        (
            Column("transaction_date", "DATE", False, "CAST(TransactionDate AS DATE)", "Date"),
            Column("total_transactions", "BIGINT", False, "COUNT(TransactionID)", "TotalTransactions"),
            Column("total_amount", "DECIMAL(29,4)", True, "SUM(Amount)", "TotalAmount"),
        ),
        "sp_DailyTransaction (unfiltered)", "Daily transaction volume and amount over all dates.",
    ),
    GoldObject(
        "fn_daily_transaction", "table_function",
        (Column("start_date", "DATE", False), Column("end_date", "DATE", False)),
        (
            Column("transaction_date", "DATE", False, "", "Date"),
            Column("total_transactions", "BIGINT", False, "", "TotalTransactions"),
            Column("total_amount", "DECIMAL(29,4)", True, "", "TotalAmount"),
        ),
        "sp_DailyTransaction @start_date, @end_date",
        "Daily summary for an inclusive date range, ordered by date.",
    ),
    GoldObject(
        "fn_balance_per_customer", "table_function",
        (Column("customer_name", "STRING", False),),
        (
            Column("customer_name", "STRING", True, "", "CustomerName"),
            Column("account_type", "STRING", True, "", "AccountType"),
            Column("initial_balance", "DECIMAL(19,4)", True, "a.Balance", "InitialBalance"),
            Column("current_balance", "DECIMAL(20,4)", True, "Balance + coalesce(SUM(CASE Deposit +amt ELSE -amt), 0)", "CurrentBalance"),
        ),
        "sp_BalancePerCustomer @customer_name",
        "Current balance per active account for customers whose name contains the argument.",
    ),
)
