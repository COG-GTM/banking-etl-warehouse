"""Where the analytics functions live and which gold tables they read.

Self-contained (no dependency on another ticket's config module). On Databricks names are three-part
(``migration_demo.banking_mig_gold.fact_transaction``); local tests pass ``catalog=None`` and get
two-part names in the Spark session catalog.
"""
from __future__ import annotations

from dataclasses import dataclass

DEFAULT_CATALOG = "migration_demo"
DEFAULT_SCHEMA_PREFIX = "banking_mig_"

GOLD_DIM_ACCOUNT = "dim_account"
GOLD_DIM_CUSTOMER = "dim_customer"
GOLD_FACT_TRANSACTION = "fact_transaction"
GOLD_TABLES = (GOLD_FACT_TRANSACTION, GOLD_DIM_ACCOUNT, GOLD_DIM_CUSTOMER)


@dataclass(frozen=True)
class AnalyticsTarget:
    """``<catalog>.<schema_prefix><layer>`` holds both the gold tables and the functions.

    ``layer`` is ``gold`` for the shared star schema, ``t9`` for the ticket-scoped seed schema.
    ``name_prefix`` prefixes table and function names, so several datasets can share one schema
    (e.g. ``edge_dim_customer`` + ``edge_fn_balance_per_customer``).
    """

    catalog: str | None = DEFAULT_CATALOG
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX
    layer: str = "gold"
    name_prefix: str = ""

    @property
    def schema(self) -> str:
        name = f"{self.schema_prefix}{self.layer}"
        return f"{self.catalog}.{name}" if self.catalog else name

    @property
    def unity_catalog(self) -> bool:
        return self.catalog is not None

    def table(self, name: str) -> str:
        return f"{self.schema}.{self.name_prefix}{name}"


def target_from_widgets(dbutils) -> AnalyticsTarget:
    """``catalog`` / ``schema_prefix`` notebook widgets (job parameters) -> shared gold target."""

    def get(name: str, default: str) -> str:
        try:
            dbutils.widgets.text(name, default)
            return dbutils.widgets.get(name) or default
        except Exception:  # noqa: BLE001 - widgets unavailable outside notebooks
            return default

    return AnalyticsTarget(
        catalog=get("catalog", DEFAULT_CATALOG), schema_prefix=get("schema_prefix", DEFAULT_SCHEMA_PREFIX)
    )
