"""Runtime configuration for the Databricks jobs.

SQL Server credentials come from a Databricks secret scope (``dbutils.secrets``). When
``dbutils`` is unavailable (local Spark, CI) the same keys are read from environment
variables, upper-cased with ``-`` replaced by ``_`` (``sqlserver-jdbc-url`` ->
``SQLSERVER_JDBC_URL``).
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from pyspark.sql import SparkSession

DEFAULT_SECRET_SCOPE = "banking-etl"
SECRET_KEYS = ("sqlserver-jdbc-url", "sqlserver-user", "sqlserver-password")
MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


@dataclass(frozen=True)
class JdbcConfig:
    url: str
    user: str
    password: str
    driver: str = MSSQL_DRIVER

    def options(self) -> dict[str, str]:
        return {"url": self.url, "user": self.user, "password": self.password, "driver": self.driver}


@dataclass(frozen=True)
class JobConfig:
    catalog: str | None
    schema: str
    landing_path: str
    secret_scope: str
    table_format: str = "delta"

    def table(self, name: str) -> str:
        """Fully-qualified target table, e.g. ``main.dwh.dim_branch`` or ``dwh.dim_branch``."""
        parts = [self.catalog, self.schema, name] if self.catalog else [self.schema, name]
        return ".".join(parts)

    def landing_file(self, filename: str) -> str:
        return f"{self.landing_path.rstrip('/')}/{filename}"


def parse_args(argv: list[str] | None = None) -> JobConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.environ.get("ETL_CATALOG") or None)
    parser.add_argument("--schema", default=os.environ.get("ETL_SCHEMA", "dwh"))
    parser.add_argument(
        "--landing-path",
        default=os.environ.get("ETL_LANDING_PATH", "/Volumes/main/dwh/landing"),
        help="Directory holding transaction_csv.csv and transaction_excel.xlsx",
    )
    parser.add_argument("--secret-scope", default=os.environ.get("ETL_SECRET_SCOPE", DEFAULT_SECRET_SCOPE))
    parser.add_argument("--table-format", default=os.environ.get("ETL_TABLE_FORMAT", "delta"))
    args, _ = parser.parse_known_args(argv)
    return JobConfig(
        catalog=args.catalog or None,
        schema=args.schema,
        landing_path=args.landing_path,
        secret_scope=args.secret_scope,
        table_format=args.table_format,
    )


def _dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils  # only present on Databricks clusters
    except ImportError:
        return None
    return DBUtils(spark)


def get_secret(spark: SparkSession, scope: str, key: str) -> str:
    dbutils = _dbutils(spark)
    if dbutils is not None:
        return dbutils.secrets.get(scope=scope, key=key)
    env_name = key.upper().replace("-", "_")
    value = os.environ.get(env_name)
    if value is None:
        raise KeyError(f"Secret {scope}/{key} not found (no dbutils and ${env_name} unset)")
    return value


def source_jdbc(spark: SparkSession, cfg: JobConfig) -> JdbcConfig:
    url, user, password = (get_secret(spark, cfg.secret_scope, k) for k in SECRET_KEYS)
    return JdbcConfig(url=url, user=user, password=password)
