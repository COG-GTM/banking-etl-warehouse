"""Shared environment configuration and naming conventions (Ticket 1).

Every notebook and module resolves catalog / schema / table / volume names through
``EnvConfig`` so the same code runs against the prod namespace (``banking_dwh.bronze``)
and the dev namespace (``migration_demo.bdwh_bronze``) without edits.

Naming conventions
------------------
* Schemas:  ``<schema_prefix><layer>`` for layer in bronze / silver / gold / ops.
* Bronze tables: ``sqlserver_<source_table>`` for JDBC extracts, ``file_transaction_csv``
  and ``file_transaction_excel`` for file ingestion.
* Silver tables: legacy DWH table names in snake_case (``dim_account``, ``fact_transaction``).
* Gold objects: ``v_<name>`` for views, ``fn_<name>`` for SQL table functions.
* Columns: snake_case; ingestion metadata columns start with ``_``.
* Volumes: ``/Volumes/<catalog>/<schema>/<volume>/<sub-path>``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

LAYERS = ("bronze", "silver", "gold", "ops")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PREFIX = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)?$")

CONFIG_FILE = Path(__file__).resolve().parents[3] / "config" / "environments.yml"


def validate_identifier(value: str, what: str = "identifier") -> str:
    """Reject anything that is not a plain SQL identifier (names are spliced into SQL)."""
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ValueError(f"Invalid {what}: {value!r} (expected [A-Za-z_][A-Za-z0-9_]*)")
    return value


def load_raw_config(path: Path | str | None = None) -> dict[str, Any]:
    import yaml  # pyyaml ships with Databricks Runtime and serverless

    with open(path or CONFIG_FILE, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@dataclass(frozen=True)
class JdbcConfig:
    secret_scope: str
    keys: Mapping[str, str]
    driver: str
    url_options: str
    source_schema: str
    source_tables: tuple[str, ...]
    legacy_dwh_database: str

    def url(self, host: str, port: str | int, database: str) -> str:
        return f"jdbc:sqlserver://{host}:{port};databaseName={database};{self.url_options}"


@dataclass(frozen=True)
class EnvConfig:
    target: str
    catalog: str
    schema_prefix: str = ""
    create_catalog: bool = False
    catalog_comment: str = ""
    layer_comments: Mapping[str, str] = field(default_factory=dict)
    volumes: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    landing: Mapping[str, str] = field(default_factory=dict)
    jdbc: JdbcConfig | None = None
    readers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, "catalog")
        if not _PREFIX.match(self.schema_prefix or ""):
            raise ValueError(f"Invalid schema_prefix: {self.schema_prefix!r}")

    # ---------- construction ----------
    @classmethod
    def from_target(
        cls,
        target: str | None = None,
        *,
        catalog: str | None = None,
        schema_prefix: str | None = None,
        config_path: Path | str | None = None,
    ) -> "EnvConfig":
        raw = load_raw_config(config_path)
        target = target or raw.get("default_target", "dev")
        if target not in raw["targets"]:
            raise ValueError(f"Unknown target {target!r}; expected one of {sorted(raw['targets'])}")
        t = raw["targets"][target]
        j = raw["jdbc"]
        return cls(
            target=target,
            catalog=catalog or t["catalog"],
            schema_prefix=t.get("schema_prefix", "") if schema_prefix is None else schema_prefix,
            create_catalog=bool(t.get("create_catalog", False)),
            catalog_comment=t.get("catalog_comment", ""),
            layer_comments=raw.get("layers", {}),
            volumes=raw.get("volumes", {}),
            landing=raw.get("landing", {}),
            jdbc=JdbcConfig(
                secret_scope=j["secret_scope"],
                keys=j["keys"],
                driver=j["driver"],
                url_options=j["url_options"],
                source_schema=j["source_schema"],
                source_tables=tuple(j["source_tables"]),
                legacy_dwh_database=j["legacy_dwh_database"],
            ),
            readers=tuple(raw.get("grants", {}).get("readers", [])),
        )

    @classmethod
    def from_widgets(cls, dbutils: Any) -> "EnvConfig":
        """Build from the standard notebook widgets: target, catalog, schema_prefix."""

        def _get(name: str) -> str | None:
            try:
                value = dbutils.widgets.get(name)
            except Exception:  # widget not defined in this notebook/job
                return None
            return value if value != "" else None

        prefix = _get("schema_prefix")
        return cls.from_target(
            _get("target"),
            catalog=_get("catalog"),
            schema_prefix=None if prefix is None else ("" if prefix == "-" else prefix),
        )

    # ---------- names ----------
    def schema_name(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")
        return f"{self.schema_prefix}{layer}"

    def schema(self, layer: str) -> str:
        return f"{self.catalog}.{self.schema_name(layer)}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{validate_identifier(name, 'table name')}"

    def ticket_schema(self, ticket: int) -> str:
        """Scratch schema for a ticket's live validation, e.g. migration_demo.bdwh_t4."""
        if not isinstance(ticket, int) or ticket <= 0:
            raise ValueError(f"Invalid ticket number: {ticket!r}")
        return f"{self.catalog}.{self.schema_prefix}t{ticket}"

    def volume_path(self, layer: str, volume: str, *parts: str) -> str:
        validate_identifier(volume, "volume")
        path = f"/Volumes/{self.catalog}/{self.schema_name(layer)}/{volume}"
        for p in parts:
            if not p:
                continue
            if ".." in p.split("/"):
                raise ValueError(f"Path traversal not allowed: {p!r}")
            path = f"{path}/{p.strip('/')}"
        return path

    def landing_path(self, source: str, *parts: str) -> str:
        """Landing folder for a source key in config (csv_transaction, excel_transaction, sqlserver)."""
        if source not in self.landing:
            raise ValueError(f"Unknown landing source {source!r}; expected one of {sorted(self.landing)}")
        return self.volume_path("bronze", "landing", self.landing[source], *parts)

    def checkpoint_path(self, stream: str, kind: str = "checkpoint") -> str:
        """Checkpoint (kind=checkpoint) or schema location (kind=schema) for a stream."""
        validate_identifier(stream, "stream name")
        if kind not in ("checkpoint", "schema"):
            raise ValueError("kind must be 'checkpoint' or 'schema'")
        return self.volume_path("ops", "checkpoints", stream, kind)

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "catalog": self.catalog,
            "schema_prefix": self.schema_prefix,
            "schemas": {layer: self.schema(layer) for layer in LAYERS},
            "landing": {k: self.landing_path(k) for k in self.landing},
        }


def jdbc_options(cfg: EnvConfig, dbutils: Any, database: str | None = None) -> dict[str, str]:
    """Spark JDBC reader options for the legacy SQL Server, credentials from the secret scope."""
    assert cfg.jdbc is not None
    j = cfg.jdbc

    def secret(key: str) -> str:
        return dbutils.secrets.get(scope=j.secret_scope, key=j.keys[key])

    db = database or secret("database")
    return {
        "url": j.url(secret("host"), secret("port"), db),
        "user": secret("user"),
        "password": secret("password"),
        "driver": j.driver,
    }
