"""Catalog / schema / path configuration for bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_CATALOG = "banking_dwh"
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_LANDING_VOLUME = "landing"


def quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


@dataclass(frozen=True)
class BronzeConfig:
    """Where bronze tables, landed files and stream checkpoints live.

    ``landing_root`` / ``checkpoint_root`` default to a Unity Catalog volume
    ``<catalog>.<bronze_schema>.landing``; pass explicit paths to use any
    other location (local tests use temp directories).
    """

    catalog: str = DEFAULT_CATALOG
    bronze_schema: str = DEFAULT_BRONZE_SCHEMA
    landing_root: str | None = None
    checkpoint_root: str | None = None
    landing_volume: str = DEFAULT_LANDING_VOLUME

    @property
    def schema_fqn(self) -> str:
        return f"{quote_identifier(self.catalog)}.{quote_identifier(self.bronze_schema)}"

    def table(self, name: str) -> str:
        return f"{self.schema_fqn}.{quote_identifier(name)}"

    @property
    def uses_default_volume(self) -> bool:
        return not self.landing_root or not self.checkpoint_root

    @property
    def landing(self) -> str:
        if self.landing_root:
            return self.landing_root.rstrip("/")
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.landing_volume}"

    @property
    def checkpoints(self) -> str:
        if self.checkpoint_root:
            return self.checkpoint_root.rstrip("/")
        return f"{self.landing}/_checkpoints"

    def landing_path(self, source_name: str) -> str:
        return f"{self.landing}/{source_name}"

    def checkpoint_path(self, table_name: str) -> str:
        return f"{self.checkpoints}/{self.bronze_schema}/{table_name}"


def ensure_bronze_objects(spark, config: BronzeConfig) -> None:
    """Create the bronze schema (and the landing volume when it is used)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema_fqn}")
    if config.uses_default_volume:
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {config.schema_fqn}.{quote_identifier(config.landing_volume)}"
        )
