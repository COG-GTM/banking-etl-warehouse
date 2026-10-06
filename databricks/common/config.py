"""Shared configuration for the banking ETL Databricks jobs.

Values come from notebook widgets (so jobs can be parameterized per run/environment)
and SQL Server credentials come from a Databricks secret scope.
"""

from __future__ import annotations

from dataclasses import dataclass, field

TARGET_SCHEMA = "dwh"

SOURCE_TABLES = ("customer", "city", "state", "account", "branch", "transaction")

# Logical source name -> physical table in the `sample` database (restored from sample.bak).
SOURCE_TABLE_OVERRIDES = {"transaction": "transaction_db"}

JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

DEFAULT_SECRET_SCOPE = "banking-etl"
SECRET_KEY_HOST = "sqlserver-host"
SECRET_KEY_USER = "sqlserver-user"
SECRET_KEY_PASSWORD = "sqlserver-password"

WIDGET_DEFAULTS = {
    "catalog": "main",
    "target_schema": TARGET_SCHEMA,
    "secret_scope": DEFAULT_SECRET_SCOPE,
    "jdbc_host": "",
    "jdbc_port": "1433",
    "jdbc_database": "sample",
    "source_schema": "dbo",
    "jdbc_url_options": "encrypt=true;trustServerCertificate=true",
    "staging_path": "/Volumes/main/dwh/staging",
}


def get_dbutils(spark):
    """Return the Databricks ``dbutils`` handle for the active session."""
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except ImportError:
        import IPython

        return IPython.get_ipython().user_ns["dbutils"]


def get_widget(dbutils, name: str, default: str | None = None) -> str:
    """Declare a text widget (idempotent) and return its current value."""
    default = WIDGET_DEFAULTS.get(name, "") if default is None else default
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    value = dbutils.widgets.get(name)
    return default if value in (None, "") else value


@dataclass(frozen=True)
class EtlConfig:
    catalog: str
    target_schema: str
    secret_scope: str
    jdbc_host: str
    jdbc_port: str
    jdbc_database: str
    source_schema: str
    jdbc_url_options: str
    staging_path: str
    jdbc_user: str = field(repr=False)
    jdbc_password: str = field(repr=False)

    @property
    def jdbc_url(self) -> str:
        url = f"jdbc:sqlserver://{self.jdbc_host}:{self.jdbc_port};databaseName={self.jdbc_database}"
        return f"{url};{self.jdbc_url_options}" if self.jdbc_url_options else url

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {"user": self.jdbc_user, "password": self.jdbc_password, "driver": JDBC_DRIVER}

    def source_table(self, name: str) -> str:
        if name not in SOURCE_TABLES:
            raise ValueError(f"Unknown source table {name!r}; expected one of {SOURCE_TABLES}")
        return f"{self.source_schema}.[{SOURCE_TABLE_OVERRIDES.get(name, name)}]"

    def target_table(self, name: str) -> str:
        return f"{self.catalog}.{self.target_schema}.{name}" if self.catalog else f"{self.target_schema}.{name}"


def load_config(spark, dbutils=None) -> EtlConfig:
    """Build the job config from widgets + secrets."""
    dbutils = dbutils or get_dbutils(spark)
    widgets = {name: get_widget(dbutils, name) for name in WIDGET_DEFAULTS}
    scope = widgets["secret_scope"]
    host = widgets["jdbc_host"] or dbutils.secrets.get(scope=scope, key=SECRET_KEY_HOST)
    return EtlConfig(
        catalog=widgets["catalog"],
        target_schema=widgets["target_schema"],
        secret_scope=scope,
        jdbc_host=host,
        jdbc_port=widgets["jdbc_port"],
        jdbc_database=widgets["jdbc_database"],
        source_schema=widgets["source_schema"],
        jdbc_url_options=widgets["jdbc_url_options"],
        staging_path=widgets["staging_path"],
        jdbc_user=dbutils.secrets.get(scope=scope, key=SECRET_KEY_USER),
        jdbc_password=dbutils.secrets.get(scope=scope, key=SECRET_KEY_PASSWORD),
    )
