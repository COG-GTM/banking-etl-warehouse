"""Catalog/schema/volume naming shared by every banking_etl module.

Nothing downstream should hard-code ``banking_dwh`` or ``bronze``: build an
:class:`EtlConfig` (from job args, environment or defaults) and ask it for
table names and paths.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass, fields, replace
from typing import Mapping, Sequence

DEFAULT_CATALOG = "banking_dwh"
LAYERS = ("bronze", "silver", "gold")
ENV_PREFIX = "BANKING_ETL_"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PREFIX = re.compile(r"^[A-Za-z0-9_]*$")
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off", ""}


def validate_identifier(value: str, what: str = "identifier") -> str:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ValueError(
            f"Invalid {what} {value!r}: use letters, digits and underscores, not starting with a digit"
        )
    return value


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lowered = value.strip().lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ValueError(f"Not a boolean: {value!r}")


@dataclass(frozen=True)
class EtlConfig:
    """Unity Catalog locations for the warehouse.

    ``schema_prefix`` is prepended to every schema name so that an isolated
    copy (CI run, personal sandbox) can live in a shared catalog, e.g.
    ``schema_prefix="ci_42_"`` gives ``ci_42_bronze``.
    """

    catalog: str = DEFAULT_CATALOG
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    landing_volume: str = "landing"
    schema_prefix: str = ""

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, "catalog")
        for layer in LAYERS:
            validate_identifier(getattr(self, f"{layer}_schema"), f"{layer} schema")
        validate_identifier(self.landing_volume, "landing volume")
        if not _PREFIX.match(self.schema_prefix):
            raise ValueError(
                f"Invalid schema prefix {self.schema_prefix!r}: use letters, digits and underscores"
            )

    def schema(self, layer: str) -> str:
        """Schema name (with prefix) for ``bronze``/``silver``/``gold``."""
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")
        return self.schema_prefix + getattr(self, f"{layer}_schema")

    def schema_fqn(self, layer: str) -> str:
        """``catalog.schema`` for a layer."""
        return f"{self.catalog}.{self.schema(layer)}"

    def table(self, layer: str, name: str) -> str:
        """Fully qualified ``catalog.schema.table`` name."""
        validate_identifier(name, "table name")
        return f"{self.schema_fqn(layer)}.{name}"

    @property
    def schemas(self) -> dict[str, str]:
        return {layer: self.schema(layer) for layer in LAYERS}

    @property
    def landing_schema(self) -> str:
        """The landing volume for raw source files lives in the bronze schema."""
        return self.schema("bronze")

    @property
    def landing_volume_fqn(self) -> str:
        return f"{self.catalog}.{self.landing_schema}.{self.landing_volume}"

    @property
    def landing_path(self) -> str:
        """``/Volumes/<catalog>/<bronze>/<landing>`` – where raw files are uploaded."""
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}"

    def landing_file(self, *parts: str) -> str:
        return "/".join([self.landing_path, *(p.strip("/") for p in parts)])

    def with_overrides(self, **overrides: str) -> "EtlConfig":
        return replace(self, **{k: v for k, v in overrides.items() if v is not None})

    @classmethod
    def from_mapping(cls, values: Mapping[str, str | None]) -> "EtlConfig":
        """Build from a dict (e.g. job parameters / notebook widgets); unknown keys are ignored."""
        known = {f.name for f in fields(cls)}
        return cls().with_overrides(**{k: v for k, v in values.items() if k in known})

    @classmethod
    def _env_values(cls, environ: Mapping[str, str] | None = None) -> dict[str, str | None]:
        environ = os.environ if environ is None else environ
        return {f.name: environ.get(ENV_PREFIX + f.name.upper()) for f in fields(cls)}

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "EtlConfig":
        """Read ``BANKING_ETL_CATALOG``, ``BANKING_ETL_SCHEMA_PREFIX``, ... ."""
        return cls.from_mapping(cls._env_values(environ))

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """Add ``--catalog``, ``--schema-prefix``, ``--bronze-schema``... to a parser."""
        for f in fields(cls):
            parser.add_argument(
                "--" + f.name.replace("_", "-"),
                dest=f.name,
                default=None,
                help=f"Override {f.name} (env {ENV_PREFIX}{f.name.upper()}, default {f.default!r})",
            )
        return parser

    @classmethod
    def from_namespace(
        cls, namespace: argparse.Namespace, environ: Mapping[str, str] | None = None
    ) -> "EtlConfig":
        """CLI args win over environment variables, which win over defaults."""
        values = cls._env_values(environ)
        for f in fields(cls):
            if getattr(namespace, f.name, None) is not None:
                values[f.name] = getattr(namespace, f.name)
        return cls.from_mapping(values)

    @classmethod
    def from_args(
        cls, argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None
    ) -> "EtlConfig":
        parser = cls.add_arguments(argparse.ArgumentParser(add_help=False))
        namespace, _ = parser.parse_known_args(argv)
        return cls.from_namespace(namespace, environ)
