"""Bronze layer configuration: source definitions derived from the Talend jobs.

Column lists and source table/file names come from the committed Talend job
definitions (``talend_jobs/*.zip`` -> ``IDX_INTERNSHIP/process/*.item`` and
``IDX_INTERNSHIP/metadata/connections/Sample_DB_Connection_0.1.item``).
Bronze keeps every column as a raw ``STRING``; typing happens in silver.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql.types import StringType, StructField, StructType

CATALOG = "banking"
BRONZE_SCHEMA = "bronze"

INGEST_TS_COLUMN = "_ingest_ts"
SOURCE_FILE_COLUMN = "_source_file"
SOURCE_TABLE_COLUMN = "_source_table"
BATCH_ID_COLUMN = "_batch_id"
RESCUED_DATA_COLUMN = "_rescued_data"


def raw_schema(columns: list[str]) -> StructType:
    """All-string schema; bronze never casts."""
    return StructType([StructField(name, StringType(), True) for name in columns])


@dataclass(frozen=True)
class FileSource:
    """A flat file consumed by ``Load_FactTransaction``."""

    name: str
    target_table: str
    file_format: str
    columns: list[str]
    glob: str
    header: bool = True
    sheet: str | int = 0

    @property
    def schema(self) -> StructType:
        return raw_schema(self.columns)

    def full_table_name(self, catalog: str = CATALOG, schema: str = BRONZE_SCHEMA) -> str:
        return f"{catalog}.{schema}.{self.target_table}"


@dataclass(frozen=True)
class JdbcSource:
    """A SQL Server table in the ``sample`` source database."""

    name: str
    target_table: str
    source_schema: str
    source_table: str
    columns: list[str]
    partition_column: str | None = None
    watermark_column: str | None = None
    consumed_by: tuple[str, ...] = field(default=())

    @property
    def schema(self) -> StructType:
        return raw_schema(self.columns)

    @property
    def qualified_source(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    def full_table_name(self, catalog: str = CATALOG, schema: str = BRONZE_SCHEMA) -> str:
        return f"{catalog}.{schema}.{self.target_table}"


# tFileInputDelimited_1 / tFileInputExcel_1 in Load_FactTransaction_0.1.item.
TRANSACTION_FILE_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
]

FILE_SOURCES: dict[str, FileSource] = {
    "transaction_csv": FileSource(
        name="transaction_csv",
        target_table="file_transaction_csv",
        file_format="csv",
        columns=TRANSACTION_FILE_COLUMNS,
        glob="transaction_csv*.csv",
    ),
    "transaction_excel": FileSource(
        name="transaction_excel",
        target_table="file_transaction_excel",
        file_format="excel",
        columns=TRANSACTION_FILE_COLUMNS,
        glob="transaction_excel*.xlsx",
    ),
}

# tMSSqlInput components across the four Talend jobs, cross-checked against the
# Sample_DB_Connection catalog metadata.
JDBC_SOURCES: dict[str, JdbcSource] = {
    "transaction_db": JdbcSource(
        name="transaction_db",
        target_table="sqlserver_transaction_db",
        source_schema="dbo",
        source_table="transaction_db",
        columns=[
            "transaction_id",
            "account_id",
            "transaction_date",
            "amount",
            "transaction_type",
            "branch_id",
        ],
        partition_column="transaction_id",
        watermark_column="transaction_date",
        consumed_by=("Load_FactTransaction",),
    ),
    "account": JdbcSource(
        name="account",
        target_table="sqlserver_account",
        source_schema="dbo",
        source_table="account",
        columns=[
            "account_id",
            "customer_id",
            "account_type",
            "balance",
            "date_opened",
            "status",
        ],
        partition_column="account_id",
        watermark_column="date_opened",
        consumed_by=("Load_DimAccount",),
    ),
    "customer": JdbcSource(
        name="customer",
        target_table="sqlserver_customer",
        source_schema="dbo",
        source_table="customer",
        columns=[
            "customer_id",
            "customer_name",
            "address",
            "city_id",
            "age",
            "gender",
            "email",
        ],
        partition_column="customer_id",
        consumed_by=("Load_DimCustomer",),
    ),
    "city": JdbcSource(
        name="city",
        target_table="sqlserver_city",
        source_schema="dbo",
        source_table="city",
        columns=["city_id", "city_name", "state_id"],
        partition_column="city_id",
        consumed_by=("Load_DimCustomer",),
    ),
    "state": JdbcSource(
        name="state",
        target_table="sqlserver_state",
        source_schema="dbo",
        source_table="state",
        columns=["state_id", "state_name"],
        partition_column="state_id",
        consumed_by=("Load_DimCustomer",),
    ),
    "branch": JdbcSource(
        name="branch",
        target_table="sqlserver_branch",
        source_schema="dbo",
        source_table="branch",
        columns=["branch_id", "branch_name", "branch_location"],
        partition_column="branch_id",
        consumed_by=("Load_DimBranch",),
    ),
}
