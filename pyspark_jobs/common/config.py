"""Environment-driven configuration for the PySpark ETL jobs.

Every setting has a default that matches the original Talend project
(source database ``sample``, target database ``DWH``, files under
``data_sources/``) so the jobs run against a local SQL Server with only
credentials supplied.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# Falls back to ./data_sources when the package is shipped as a --py-files zip.
DEFAULT_DATA_DIR = REPO_ROOT / "data_sources" if (REPO_ROOT / "data_sources").is_dir() else Path.cwd() / "data_sources"

MSSQL_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = _env(name)
    if value is None:
        return default
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class JdbcConfig:
    host: str
    port: int
    database: str
    user: str | None
    password: str | None
    encrypt: bool
    trust_server_certificate: bool
    extra_properties: str | None = None

    @property
    def url(self) -> str:
        url = (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};"
            f"encrypt={str(self.encrypt).lower()};"
            f"trustServerCertificate={str(self.trust_server_certificate).lower()}"
        )
        if self.extra_properties:
            url += ";" + self.extra_properties.strip(";")
        return url

    @property
    def properties(self) -> dict[str, str]:
        props = {"driver": MSSQL_DRIVER_CLASS}
        if self.user is not None:
            props["user"] = self.user
        if self.password is not None:
            props["password"] = self.password
        return props


def _jdbc_from_env(prefix: str, default_database: str) -> JdbcConfig:
    """Read ``<prefix>_*`` variables, falling back to shared ``MSSQL_*`` ones."""

    def get(key: str, default: str | None = None) -> str | None:
        return _env(f"{prefix}_{key}", _env(f"MSSQL_{key}", default))

    return JdbcConfig(
        host=get("HOST", "localhost"),
        port=int(get("PORT", "1433")),
        database=_env(f"{prefix}_DATABASE", default_database),
        user=get("USER", "sa"),
        password=get("PASSWORD"),
        encrypt=_env_bool(f"{prefix}_ENCRYPT", _env_bool("MSSQL_ENCRYPT", True)),
        trust_server_certificate=_env_bool(
            f"{prefix}_TRUST_SERVER_CERTIFICATE",
            _env_bool("MSSQL_TRUST_SERVER_CERTIFICATE", True),
        ),
        extra_properties=get("JDBC_PROPERTIES"),
    )


@dataclass(frozen=True)
class FileSources:
    transaction_csv: str
    transaction_excel: str
    excel_sheet: str
    csv_timestamp_format: str
    excel_reader: str


@dataclass(frozen=True)
class EtlConfig:
    source: JdbcConfig
    target: JdbcConfig
    files: FileSources
    source_schema: str = "dbo"
    target_schema: str = "dbo"
    transaction_table: str = "transaction_db"
    dim_write_mode: str = "overwrite"
    fact_write_mode: str = "overwrite"
    enforce_fact_foreign_keys: bool = True
    dim_customer_uppercase_columns: tuple[str, ...] = field(
        default=("CustomerName", "Address", "CityName", "StateName", "Gender")
    )

    @property
    def dim_jdbc_mode(self) -> str:
        """Dimensions are FK targets, so a full reload uses DELETE + INSERT, not TRUNCATE."""
        return "delete_insert" if self.dim_write_mode == "overwrite" else self.dim_write_mode

    def source_table(self, table: str) -> str:
        return f"{self.source_schema}.{table}"

    def target_table(self, table: str) -> str:
        return f"{self.target_schema}.{table}"


def load_config() -> EtlConfig:
    data_dir = Path(_env("DATA_SOURCES_DIR", str(DEFAULT_DATA_DIR)))
    files = FileSources(
        transaction_csv=_env("TRANSACTION_CSV_PATH", str(data_dir / "transaction_csv.csv")),
        transaction_excel=_env("TRANSACTION_EXCEL_PATH", str(data_dir / "transaction_excel.xlsx")),
        excel_sheet=_env("TRANSACTION_EXCEL_SHEET", "Sheet1"),
        csv_timestamp_format=_env("TRANSACTION_CSV_TIMESTAMP_FORMAT", "dd-MM-yyyy HH:mm:ss"),
        excel_reader=_env("EXCEL_READER", "pandas").lower(),
    )
    return EtlConfig(
        source=_jdbc_from_env("SOURCE_DB", "sample"),
        target=_jdbc_from_env("TARGET_DB", "DWH"),
        files=files,
        source_schema=_env("SOURCE_DB_SCHEMA", "dbo"),
        target_schema=_env("TARGET_DB_SCHEMA", "dbo"),
        transaction_table=_env("SOURCE_TRANSACTION_TABLE", "transaction_db"),
        dim_write_mode=_env("DIM_WRITE_MODE", "overwrite").lower(),
        fact_write_mode=_env("FACT_WRITE_MODE", "overwrite").lower(),
        enforce_fact_foreign_keys=_env_bool("ENFORCE_FACT_FOREIGN_KEYS", True),
        dim_customer_uppercase_columns=_env_list(
            "DIM_CUSTOMER_UPPERCASE_COLUMNS",
            ("CustomerName", "Address", "CityName", "StateName", "Gender"),
        ),
    )
