"""Source registry: the sample.bak tables and the transaction file drops.

The sample.bak column lists and types come from restoring the backup into
SQL Server 2022 (``INFORMATION_SCHEMA.COLUMNS`` of database ``sample``) and
match the ``tMSSqlInput`` queries in ``talend_jobs/*.zip``.  The file
layouts come from the ``tFileInputDelimited`` / ``tFileInputExcel``
components of ``Load_FactTransaction``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

CSV_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"
EXCEL_TIMESTAMP_FORMATS = ("yyyy-MM-dd HH:mm:ss", CSV_TIMESTAMP_FORMAT)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    spark_type: str
    source_type: str = ""
    nullable: bool = True
    timestamp_formats: tuple[str, ...] = ()
    """Explicit patterns for TIMESTAMP columns; empty means ISO ``try_cast``."""
    iso_fallback: bool = True
    """For TIMESTAMP columns with patterns, also accept an ISO-8601 value."""


@dataclass(frozen=True)
class SourceTable:
    """A table in the ``sample`` SQL Server database."""

    name: str
    columns: tuple[ColumnSpec, ...]
    source_schema: str = "dbo"
    source_database: str = "sample"
    primary_key: tuple[str, ...] = field(default=())

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def qualified_source_name(self) -> str:
        return f"{self.source_database}.{self.source_schema}.{self.name}"

    def select_sql(self) -> str:
        cols = ", ".join(f"[{c}]" for c in self.column_names)
        return f"SELECT {cols} FROM [{self.source_schema}].[{self.name}]"


@dataclass(frozen=True)
class FileSource:
    """A landed file drop (CSV or Excel)."""

    name: str
    file_format: str
    file_glob: str
    columns: tuple[ColumnSpec, ...]
    sample_file: str

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def _int(name: str, nullable: bool = True) -> ColumnSpec:
    return ColumnSpec(name, "INT", "int", nullable)


def _varchar(name: str, length: int | None) -> ColumnSpec:
    return ColumnSpec(name, "STRING", f"varchar({'max' if length is None else length})")


def _datetime2(name: str) -> ColumnSpec:
    return ColumnSpec(name, "TIMESTAMP", "datetime2")


SAMPLE_DB_TABLES: tuple[SourceTable, ...] = (
    SourceTable(
        "account",
        (
            _int("account_id", nullable=False),
            _int("customer_id"),
            _varchar("account_type", 10),
            _int("balance"),
            _datetime2("date_opened"),
            _varchar("status", 10),
        ),
        primary_key=("account_id",),
    ),
    SourceTable(
        "branch",
        (
            _int("branch_id", nullable=False),
            _varchar("branch_name", 50),
            _varchar("branch_location", 50),
        ),
        primary_key=("branch_id",),
    ),
    SourceTable(
        "city",
        (
            _int("city_id", nullable=False),
            _varchar("city_name", 50),
            _int("state_id", nullable=False),
        ),
        primary_key=("city_id",),
    ),
    SourceTable(
        "customer",
        (
            _int("customer_id", nullable=False),
            _varchar("customer_name", 50),
            _varchar("address", None),
            _int("city_id"),
            _varchar("age", 3),
            _varchar("gender", 10),
            _varchar("email", 50),
        ),
        primary_key=("customer_id",),
    ),
    SourceTable(
        "state",
        (
            _int("state_id", nullable=False),
            _varchar("state_name", 50),
        ),
        primary_key=("state_id",),
    ),
    SourceTable(
        "transaction_db",
        (
            _int("transaction_id", nullable=False),
            _int("account_id"),
            _datetime2("transaction_date"),
            _int("amount"),
            _varchar("transaction_type", 50),
            _int("branch_id"),
        ),
        primary_key=("transaction_id",),
    ),
)

SAMPLE_DB_TABLES_BY_NAME = {t.name: t for t in SAMPLE_DB_TABLES}


def _transaction_columns(timestamp_formats: tuple[str, ...], iso_fallback: bool) -> tuple[ColumnSpec, ...]:
    return (
        ColumnSpec("transaction_id", "INT", nullable=False),
        ColumnSpec("account_id", "INT"),
        ColumnSpec(
            "transaction_date",
            "TIMESTAMP",
            timestamp_formats=timestamp_formats,
            iso_fallback=iso_fallback,
        ),
        ColumnSpec("amount", "INT"),
        ColumnSpec("transaction_type", "STRING"),
        ColumnSpec("branch_id", "INT"),
    )


TRANSACTION_CSV = FileSource(
    name="transaction_csv",
    file_format="csv",
    file_glob="*.csv",
    columns=_transaction_columns((CSV_TIMESTAMP_FORMAT,), iso_fallback=False),
    sample_file="transaction_csv.csv",
)

TRANSACTION_EXCEL = FileSource(
    name="transaction_excel",
    file_format="xlsx",
    file_glob="*.xlsx",
    columns=_transaction_columns(EXCEL_TIMESTAMP_FORMATS, iso_fallback=True),
    sample_file="transaction_excel.xlsx",
)

FILE_SOURCES: tuple[FileSource, ...] = (TRANSACTION_CSV, TRANSACTION_EXCEL)
FILE_SOURCES_BY_NAME = {s.name: s for s in FILE_SOURCES}

SAMPLE_DB_LANDING_DIR = "sample_db"
"""Landing sub-directory for the sample.bak extract-to-files fallback."""
