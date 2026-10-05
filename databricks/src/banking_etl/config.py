"""Single source of truth for catalog, schema, table and volume names.

On Databricks every name is three-part (catalog.schema.table). Local tests run
against the Spark session catalog, so ``Settings(catalog=None)`` yields
two-part names and the schemas are plain databases.
"""
from __future__ import annotations

from dataclasses import dataclass

LAYERS = ("bronze", "silver", "gold", "ops")

SQLSERVER_SOURCE_TABLES = ("customer", "city", "state", "account", "branch", "transaction_db")

BRONZE_SQLSERVER = {t: f"sqlserver_{t}" for t in SQLSERVER_SOURCE_TABLES}
BRONZE_FILE_TRANSACTION_CSV = "file_transaction_csv"
BRONZE_FILE_TRANSACTION_EXCEL = "file_transaction_excel"

SILVER_BRANCH = "branch"
SILVER_ACCOUNT = "account"
SILVER_CUSTOMER = "customer"
SILVER_TRANSACTION = "transaction"

GOLD_DIM_BRANCH = "dim_branch"
GOLD_DIM_ACCOUNT = "dim_account"
GOLD_DIM_CUSTOMER = "dim_customer"
GOLD_FACT_TRANSACTION = "fact_transaction"

LANDING_VOLUME = "landing"
CHECKPOINT_VOLUME = "checkpoints"


@dataclass(frozen=True)
class Settings:
    catalog: str | None = None
    schema_prefix: str = ""
    secret_scope: str = "banking-etl-sqlserver"
    source_mode: str = "fixture"

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        name = f"{self.schema_prefix}{layer}"
        return f"{self.catalog}.{name}" if self.catalog else name

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    def volume_path(self, layer: str, volume: str, *parts: str) -> str:
        if not self.catalog:
            raise ValueError("volume paths require a Unity Catalog catalog")
        base = f"/Volumes/{self.catalog}/{self.schema_prefix}{layer}/{volume}"
        return "/".join((base, *parts)) if parts else base

    def landing_path(self, *parts: str) -> str:
        return self.volume_path("bronze", LANDING_VOLUME, *parts)

    def checkpoint_path(self, *parts: str) -> str:
        return self.volume_path("ops", CHECKPOINT_VOLUME, *parts)


def settings_from_widgets(dbutils) -> Settings:
    """Build Settings from notebook widgets (populated by bundle job parameters)."""
    def get(name: str, default: str) -> str:
        try:
            dbutils.widgets.text(name, default)
            return dbutils.widgets.get(name) or default
        except Exception:  # noqa: BLE001 - widgets unavailable outside notebooks
            return default

    return Settings(
        catalog=get("catalog", "banking_etl_dev"),
        schema_prefix=get("schema_prefix", ""),
        secret_scope=get("secret_scope", "banking-etl-sqlserver"),
        source_mode=get("source_mode", "fixture"),
    )
