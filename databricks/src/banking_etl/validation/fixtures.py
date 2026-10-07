"""Load the legacy DWH parity fixtures (``databricks/fixtures/legacy_dwh``) into Spark."""

from __future__ import annotations

import hashlib
import json
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


class FixtureIntegrityError(RuntimeError):
    pass


def verify_manifest(base: str = DEFAULT_FIXTURES) -> dict:
    """Check every snapshot file against ``manifest.json`` (SHA-256) before it is used as the baseline.

    ``base`` must be a filesystem path (local checkout or a ``/Volumes/...`` FUSE path).
    """
    root = Path(base)
    manifest = json.loads((root / "manifest.json").read_text())
    bad = {rel: "missing" for rel in manifest["sha256"] if not (root / rel).is_file()}
    for rel, digest in manifest["sha256"].items():
        if rel not in bad and hashlib.sha256((root / rel).read_bytes()).hexdigest() != digest:
            bad[rel] = "sha256 mismatch"
    if bad:
        raise FixtureIntegrityError(f"legacy baseline at {base} does not match manifest.json: {bad}")
    return manifest


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
