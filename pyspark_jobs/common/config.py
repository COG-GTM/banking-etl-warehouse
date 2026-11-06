"""Environment-driven configuration for the PySpark ETL jobs and reports."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
DEFAULT_SPARK_EXCEL_PACKAGE = "com.crealytics:spark-excel_2.12:3.5.1_0.20.4"
MSSQL_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    database: str
    user: str | None
    password: str | None
    encrypt: bool = True
    trust_server_certificate: bool = False
    schema: str = "dbo"
    url_override: str | None = None

    @property
    def jdbc_url(self) -> str:
        if self.url_override:
            return self.url_override
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};"
            f"encrypt={str(self.encrypt).lower()};"
            f"trustServerCertificate={str(self.trust_server_certificate).lower()}"
        )

    def qualified(self, table: str) -> str:
        return table if "." in table else f"{self.schema}.{table}"

    @classmethod
    def from_env(cls, prefix: str, default_database: str, fallback: DbConfig | None = None) -> DbConfig:
        """Reads ``<PREFIX>_DB_*`` variables, falling back to ``fallback`` (or built-in defaults)."""

        def pick(suffix: str, default):
            return _env(f"{prefix}_DB_{suffix}", default)

        fb = fallback
        return cls(
            host=pick("HOST", fb.host if fb else "localhost"),
            port=int(pick("PORT", str(fb.port) if fb else "1433")),
            database=pick("NAME", default_database),
            user=pick("USER", fb.user if fb else None),
            password=pick("PASSWORD", fb.password if fb else None),
            encrypt=_env_bool(f"{prefix}_DB_ENCRYPT", fb.encrypt if fb else True),
            trust_server_certificate=_env_bool(
                f"{prefix}_DB_TRUST_SERVER_CERTIFICATE", fb.trust_server_certificate if fb else False
            ),
            schema=pick("SCHEMA", "dbo"),
            url_override=_env(f"{prefix}_JDBC_URL"),
        )


@dataclass(frozen=True)
class FileSources:
    transaction_csv: str
    transaction_excel: str
    excel_sheet: str | None
    excel_reader: str
    csv_timestamp_format: str


@dataclass(frozen=True)
class SparkSettings:
    app_name_prefix: str = "banking-etl"
    master: str | None = None
    jdbc_packages: str | None = DEFAULT_MSSQL_JDBC_PACKAGE
    excel_package: str = DEFAULT_SPARK_EXCEL_PACKAGE
    jars: str | None = None
    repositories: str | None = None
    session_timezone: str | None = None


@dataclass(frozen=True)
class EtlConfig:
    source_db: DbConfig
    dwh_db: DbConfig
    files: FileSources
    spark: SparkSettings = field(default_factory=SparkSettings)

    @classmethod
    def from_env(cls) -> EtlConfig:
        source = DbConfig.from_env("SOURCE", default_database="sample")
        dwh = DbConfig.from_env("DWH", default_database="DWH", fallback=source)
        data_dir = Path(_env("DATA_SOURCES_DIR", str(REPO_ROOT / "data_sources")))
        files = FileSources(
            transaction_csv=_env("TRANSACTION_CSV_PATH", str(data_dir / "transaction_csv.csv")),
            transaction_excel=_env("TRANSACTION_EXCEL_PATH", str(data_dir / "transaction_excel.xlsx")),
            excel_sheet=_env("TRANSACTION_EXCEL_SHEET"),
            excel_reader=_env("EXCEL_READER", "pandas").lower(),
            csv_timestamp_format=_env("CSV_TIMESTAMP_FORMAT", "dd-MM-yyyy HH:mm:ss"),
        )
        spark = SparkSettings(
            master=_env("SPARK_MASTER"),
            jdbc_packages=_env("SPARK_JDBC_PACKAGES", DEFAULT_MSSQL_JDBC_PACKAGE),
            excel_package=_env("SPARK_EXCEL_PACKAGE", DEFAULT_SPARK_EXCEL_PACKAGE),
            jars=_env("SPARK_JDBC_JARS"),
            repositories=_env("SPARK_JARS_REPOSITORIES"),
            session_timezone=_env("SPARK_SQL_TIMEZONE"),
        )
        return cls(source_db=source, dwh_db=dwh, files=files, spark=spark)
