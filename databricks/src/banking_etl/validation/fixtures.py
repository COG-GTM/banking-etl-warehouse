"""Load the legacy DWH parity fixtures (``databricks/fixtures/legacy_dwh``) into Spark."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from banking_etl.validation import specs

DEFAULT_FIXTURES = str(Path(__file__).resolve().parents[3] / "fixtures" / "legacy_dwh")

CSV_OPTIONS = {
    "header": "true",
    "mode": "FAILFAST",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss[.SSS]",
    "dateFormat": "yyyy-MM-dd",
    "encoding": "UTF-8",
}


def _path(base: str, rel: str) -> str:
    return f"{base.rstrip('/')}/{rel}"


def read_csv(spark: SparkSession, path: str, schema: T.StructType) -> DataFrame:
    return spark.read.options(**CSV_OPTIONS).schema(schema).csv(path)


def load_sources(spark: SparkSession, base: str = DEFAULT_FIXTURES) -> dict[str, DataFrame]:
    """Source snapshots: ``sqlserver_<table>`` from the `sample` DB plus the two flat files."""
    return {
        name: read_csv(spark, _path(base, f"source/{name}.csv"), schema)
        for name, schema in specs.SOURCE_SCHEMAS.items()
    }


def load_legacy_dwh(spark: SparkSession, base: str = DEFAULT_FIXTURES) -> dict[str, DataFrame]:
    """Legacy DWH snapshots keyed by gold table name, columns renamed to gold names."""
    out = {}
    for spec in specs.GOLD_TABLES:
        df = read_csv(spark, _path(base, f"dwh/{spec.legacy}.csv"), spec.legacy_schema())
        out[spec.gold] = df.select([F.col(c.legacy).alias(c.gold) for c in spec.columns])
    return out


def load_legacy_procs(spark: SparkSession, base: str = DEFAULT_FIXTURES) -> dict[str, DataFrame]:
    return {
        "sp_DailyTransaction": read_csv(
            spark, _path(base, "procs/sp_DailyTransaction.csv"), specs.DAILY_TRANSACTION_SCHEMA),
        "sp_BalancePerCustomer": read_csv(
            spark, _path(base, "procs/sp_BalancePerCustomer.csv"), specs.BALANCE_PER_CUSTOMER_SCHEMA),
    }


def load_talend_rejects(spark: SparkSession, base: str = DEFAULT_FIXTURES) -> DataFrame:
    return read_csv(spark, _path(base, "talend_rejects.csv"), specs.TALEND_REJECTS_SCHEMA)
