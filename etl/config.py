"""Runtime configuration for the PySpark ETL, read from environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_PACKAGES = (
    "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11,com.crealytics:spark-excel_2.12:3.5.1_0.20.4"
)
MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
WRITE_MODES = ("overwrite", "append")


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _default_jdbc_url(database: str) -> str:
    host = _env("MSSQL_HOST", "localhost")
    port = _env("MSSQL_PORT", "1433")
    return (
        f"jdbc:sqlserver://{host}:{port};databaseName={database};"
        "encrypt=true;trustServerCertificate=true"
    )


@dataclass(frozen=True)
class JdbcConfig:
    url: str
    user: str
    password: str
    driver: str = MSSQL_DRIVER

    def options(self) -> dict[str, str]:
        return {
            "url": self.url,
            "user": self.user,
            "password": self.password,
            "driver": self.driver,
        }


@dataclass(frozen=True)
class SourceTables:
    branch: str
    account: str
    customer: str
    city: str
    state: str
    transaction: str


@dataclass(frozen=True)
class TargetTables:
    dim_branch: str = "dbo.DimBranch"
    dim_account: str = "dbo.DimAccount"
    dim_customer: str = "dbo.DimCustomer"
    fact_transaction: str = "dbo.FactTransaction"


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str
    packages: str
    jars: str
    repositories: str


@dataclass(frozen=True)
class EtlConfig:
    source: JdbcConfig
    target: JdbcConfig
    source_tables: SourceTables
    transaction_csv_path: str
    transaction_excel_path: str
    excel_sheet: str
    write_mode: str
    enforce_foreign_keys: bool
    spark: SparkConfig
    target_tables: TargetTables = field(default_factory=TargetTables)


def load_config() -> EtlConfig:
    write_mode = _env("ETL_WRITE_MODE", "overwrite").lower()
    if write_mode not in WRITE_MODES:
        raise ValueError(f"ETL_WRITE_MODE must be one of {WRITE_MODES}, got {write_mode!r}")

    default_user = _env("MSSQL_USER", "sa")
    default_password = _env("MSSQL_PASSWORD")

    return EtlConfig(
        source=JdbcConfig(
            url=_env("SAMPLE_JDBC_URL", _default_jdbc_url(_env("SAMPLE_DB_NAME", "sample"))),
            user=_env("SAMPLE_DB_USER", default_user),
            password=_env("SAMPLE_DB_PASSWORD", default_password),
        ),
        target=JdbcConfig(
            url=_env("DWH_JDBC_URL", _default_jdbc_url(_env("DWH_DB_NAME", "DWH"))),
            user=_env("DWH_DB_USER", default_user),
            password=_env("DWH_DB_PASSWORD", default_password),
        ),
        source_tables=SourceTables(
            branch=_env("SOURCE_BRANCH_TABLE", "dbo.branch"),
            account=_env("SOURCE_ACCOUNT_TABLE", "dbo.account"),
            customer=_env("SOURCE_CUSTOMER_TABLE", "dbo.customer"),
            city=_env("SOURCE_CITY_TABLE", "dbo.city"),
            state=_env("SOURCE_STATE_TABLE", "dbo.state"),
            transaction=_env("SOURCE_TRANSACTION_TABLE", "dbo.transaction_db"),
        ),
        transaction_csv_path=_env(
            "TRANSACTION_CSV_PATH", str(REPO_ROOT / "data_sources" / "transaction_csv.csv")
        ),
        transaction_excel_path=_env(
            "TRANSACTION_EXCEL_PATH", str(REPO_ROOT / "data_sources" / "transaction_excel.xlsx")
        ),
        excel_sheet=_env("TRANSACTION_EXCEL_SHEET", "Sheet1"),
        write_mode=write_mode,
        enforce_foreign_keys=_env("ETL_ENFORCE_FOREIGN_KEYS", "true").lower() == "true",
        spark=SparkConfig(
            app_name=_env("SPARK_APP_NAME", "banking-etl-warehouse"),
            master=_env("SPARK_MASTER", "local[*]"),
            packages=_env("SPARK_JARS_PACKAGES", DEFAULT_PACKAGES),
            jars=_env("SPARK_JARS"),
            repositories=_env("SPARK_JARS_REPOSITORIES"),
        ),
    )
