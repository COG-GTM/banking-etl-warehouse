"""Small table-naming / IO helpers private to the validation package."""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

_IDENT = re.compile(r"[A-Za-z0-9_]+")


@dataclass(frozen=True)
class Schemas:
    """Resolves fully-qualified names for the schemas the harness touches.

    ``catalog=None`` gives two-part names (local metastore in tests).
    """

    catalog: str | None = "migration_demo"
    schema_prefix: str = "banking_mig_"
    work_suffix: str = "t10"
    ops_suffix: str = "ops"

    def __post_init__(self):
        # Names are interpolated into SQL (CREATE SCHEMA / VIEW / MERGE), so only plain identifiers are allowed.
        for field_name in ("catalog", "schema_prefix", "work_suffix", "ops_suffix"):
            value = getattr(self, field_name)
            if value is not None and not _IDENT.fullmatch(value):
                raise ValueError(f"Schemas.{field_name}={value!r} is not a plain identifier [A-Za-z0-9_]")
        if self.work_suffix == self.ops_suffix:
            raise ValueError("work and ops schemas must differ")

    @property
    def work(self) -> str:
        return self._schema(self.work_suffix)

    @property
    def ops(self) -> str:
        return self._schema(self.ops_suffix)

    def schema(self, suffix: str) -> str:
        return self._schema(suffix)

    def _schema(self, suffix: str) -> str:
        name = f"{self.schema_prefix}{suffix}"
        return f"{self.catalog}.{name}" if self.catalog else name

    def table(self, suffix: str, name: str) -> str:
        return f"{self._schema(suffix)}.{name}"

    def work_table(self, name: str) -> str:
        return f"{self.work}.{name}"

    def ops_table(self, name: str) -> str:
        return f"{self.ops}.{name}"


def ensure_schemas(spark: SparkSession, *schemas: str) -> None:
    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")


def write_table(df: DataFrame, name: str, mode: str = "overwrite") -> None:
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(name)


def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {name}").collect()
        return True
    except Exception:  # noqa: BLE001 - AnalysisException differs between classic and Connect
        return False
