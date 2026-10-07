"""Environment configuration shared by every banking_etl job.

One place that turns (catalog, schema_prefix) into fully-qualified Unity
Catalog names, so jobs never hard-code ``migration_demo.banking_mig_*``.

Resolution order for each setting: explicit argument > environment variable
(``BANKING_ETL_*``) > default.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from typing import Mapping, Optional

DEFAULT_CATALOG = "migration_demo"
DEFAULT_SCHEMA_PREFIX = "banking_mig_"
DEFAULT_LANDING_VOLUME = "landing"
DEFAULT_GRANT_PRINCIPAL = "account users"

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
OPS = "ops"
LAYERS = (BRONZE, SILVER, GOLD, OPS)

LAYER_COMMENTS = {
    BRONZE: "Bronze: raw, append-only copies of the SQL Server `sample` DB and the transaction Excel/CSV files.",
    SILVER: "Silver: cleansed, typed and deduplicated entities (branch, account, customer, transaction).",
    GOLD: "Gold: star schema replacing the legacy DWH (dim_branch, dim_account, dim_customer, fact_transaction) and analytics functions.",
    OPS: "Ops: run audit, reject and data-quality tables for the banking ETL migration.",
}

# Folders pre-created inside the landing volume (``databricks fs cp`` fails
# when the target folder does not exist yet).
LANDING_SUBDIRS = ("transactions/csv", "transactions/excel", "sample_db")

ENV_CATALOG = "BANKING_ETL_CATALOG"
ENV_SCHEMA_PREFIX = "BANKING_ETL_SCHEMA_PREFIX"
ENV_LANDING_VOLUME = "BANKING_ETL_LANDING_VOLUME"
ENV_GRANT_PRINCIPAL = "BANKING_ETL_GRANT_PRINCIPAL"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PREFIX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$|^$")
_PRINCIPAL = re.compile(r"^[A-Za-z0-9_.@\- ]+$")


def quote_identifier(name: str) -> str:
    """Backtick-quote a single identifier part."""
    return "`" + name.replace("`", "``") + "`"


def _check_identifier(value: str, what: str) -> str:
    if not _IDENTIFIER.match(value or ""):
        raise ValueError(f"Invalid {what} {value!r}: expected [A-Za-z_][A-Za-z0-9_]*")
    return value


@dataclass(frozen=True)
class EnvConfig:
    """Names for one environment (e.g. dev) of the banking ETL lakehouse."""

    catalog: str = DEFAULT_CATALOG
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX
    landing_volume: str = DEFAULT_LANDING_VOLUME
    grant_principal: str = DEFAULT_GRANT_PRINCIPAL
    layers: tuple = field(default=LAYERS)

    def __post_init__(self) -> None:
        _check_identifier(self.catalog, "catalog")
        if not _PREFIX.match(self.schema_prefix or ""):
            raise ValueError(f"Invalid schema_prefix {self.schema_prefix!r}")
        _check_identifier(self.landing_volume, "landing_volume")
        if not _PRINCIPAL.match(self.grant_principal or ""):
            raise ValueError(f"Invalid grant_principal {self.grant_principal!r}")
        for layer in self.layers:
            _check_identifier(layer, "layer")

    @classmethod
    def from_env(
        cls, environ: Optional[Mapping[str, str]] = None, **overrides: Optional[str]
    ) -> "EnvConfig":
        env = os.environ if environ is None else environ
        values = {
            "catalog": env.get(ENV_CATALOG) or DEFAULT_CATALOG,
            "schema_prefix": env.get(ENV_SCHEMA_PREFIX, DEFAULT_SCHEMA_PREFIX),
            "landing_volume": env.get(ENV_LANDING_VOLUME) or DEFAULT_LANDING_VOLUME,
            "grant_principal": env.get(ENV_GRANT_PRINCIPAL) or DEFAULT_GRANT_PRINCIPAL,
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)

    @classmethod
    def from_widgets(cls, dbutils, **defaults: str) -> "EnvConfig":
        """Build from Databricks notebook widgets / job base_parameters."""
        base = cls.from_env(**defaults)
        values = {}
        for name in ("catalog", "schema_prefix", "landing_volume", "grant_principal"):
            dbutils.widgets.text(name, getattr(base, name))
            values[name] = dbutils.widgets.get(name)
        return replace(base, **values)

    def schema_name(self, layer: str) -> str:
        """Bare schema name, e.g. ``banking_mig_bronze``."""
        return f"{self.schema_prefix}{_check_identifier(layer, 'layer')}"

    def schema(self, layer: str) -> str:
        """Two-part schema name, e.g. ``migration_demo.banking_mig_bronze``."""
        return f"{self.catalog}.{self.schema_name(layer)}"

    def table(self, layer: str, name: str) -> str:
        """Three-part table name, e.g. ``migration_demo.banking_mig_gold.dim_branch``."""
        return f"{self.schema(layer)}.{_check_identifier(name, 'table')}"

    @property
    def bronze(self) -> str:
        return self.schema(BRONZE)

    @property
    def silver(self) -> str:
        return self.schema(SILVER)

    @property
    def gold(self) -> str:
        return self.schema(GOLD)

    @property
    def ops(self) -> str:
        return self.schema(OPS)

    @property
    def schemas(self) -> dict:
        return {layer: self.schema(layer) for layer in self.layers}

    @property
    def landing_volume_name(self) -> str:
        """Three-part volume name, e.g. ``migration_demo.banking_mig_bronze.landing``."""
        return f"{self.bronze}.{self.landing_volume}"

    @property
    def landing_path(self) -> str:
        """POSIX path of the landing volume, e.g. ``/Volumes/migration_demo/banking_mig_bronze/landing``."""
        return f"/Volumes/{self.catalog}/{self.schema_name(BRONZE)}/{self.landing_volume}"

    def landing_subpath(self, *parts: str) -> str:
        return "/".join([self.landing_path, *[p.strip("/") for p in parts]])

    def ticket_schema(self, ticket: int | str) -> str:
        """Ticket-scoped scratch schema, e.g. ``migration_demo.banking_mig_t3``."""
        return self.schema(f"t{ticket}")
