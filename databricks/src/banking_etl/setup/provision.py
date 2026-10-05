"""Idempotent Unity Catalog provisioning driven by the templates in ``sql/setup``.

``plan()`` renders the templates into ordered steps; ``provision()`` runs them through
any ``execute(sql)`` callable (``spark.sql`` in a notebook, the SQL warehouse
statement API from a laptop/CI). Every statement is ``IF NOT EXISTS`` or a ``GRANT``,
so re-running is safe.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Callable, Iterable

from banking_etl.config import CHECKPOINT_VOLUME, LANDING_VOLUME, LAYERS, Settings

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "setup"

# Folders pre-created in the landing volume (see databricks/README.md).
LANDING_DIRS = (("transactions", "csv"), ("transactions", "excel"), ("sample_db",))

_IDENT = re.compile(r"^[^`\x00-\x1f]+$")


@dataclass(frozen=True)
class ProvisionOptions:
    create_catalog: bool = False
    data_engineers: str = ""
    jobs_principal: str = ""


@dataclass(frozen=True)
class Step:
    name: str
    statements: tuple[str, ...]
    # Optional steps log a warning on failure instead of aborting (e.g. CREATE CATALOG without admin rights).
    optional: bool = False


def quote(*parts: str) -> str:
    """Backtick-quote a (multi-part) identifier or principal name."""
    for p in parts:
        if not p or not _IDENT.match(p):
            raise ValueError(f"invalid identifier part {p!r}")
    return ".".join(f"`{p}`" for p in parts)


def _schema_parts(settings: Settings, layer: str) -> tuple[str, ...]:
    name = f"{settings.schema_prefix}{layer}"
    return (settings.catalog, name) if settings.catalog else (name,)


def template_vars(settings: Settings, options: ProvisionOptions) -> dict[str, str]:
    v = {f"{layer}_schema": quote(*_schema_parts(settings, layer)) for layer in LAYERS}
    if settings.catalog:
        v["catalog"] = quote(settings.catalog)
        v["landing_volume"] = quote(*_schema_parts(settings, "bronze"), LANDING_VOLUME)
        v["checkpoint_volume"] = quote(*_schema_parts(settings, "ops"), CHECKPOINT_VOLUME)
    if options.data_engineers:
        v["data_engineers"] = quote(options.data_engineers)
    if options.jobs_principal:
        v["jobs_principal"] = quote(options.jobs_principal)
    return v


def split_statements(sql: str) -> list[str]:
    """Strip ``--`` line comments and split on ``;`` (templates contain no string literals with ';')."""
    lines = [re.sub(r"--.*$", "", line) for line in sql.splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def render(path: Path, variables: dict[str, str]) -> tuple[str, ...]:
    return tuple(split_statements(Template(path.read_text()).substitute(variables)))


def plan(settings: Settings, options: ProvisionOptions = ProvisionOptions(), sql_dir: Path = SQL_DIR) -> list[Step]:
    """Ordered provisioning steps. Without a catalog (local tests) only schemas are created."""
    sql_dir = Path(sql_dir)
    v = template_vars(settings, options)
    steps: list[Step] = []
    if settings.catalog and options.create_catalog:
        steps.append(Step("catalog", render(sql_dir / "00_catalog.sql", v), optional=True))
    steps.append(Step("schemas", render(sql_dir / "01_schemas.sql", v)))
    if not settings.catalog:
        return steps
    steps.append(Step("volumes", render(sql_dir / "02_volumes.sql", v)))
    if options.data_engineers:
        if options.create_catalog:
            steps.append(Step("grants_catalog", render(sql_dir / "10_grants_catalog.sql", v), optional=True))
        steps.append(Step("grants_data_engineers", render(sql_dir / "11_grants_data_engineers.sql", v)))
    if options.jobs_principal:
        steps.append(Step("grants_jobs_principal", render(sql_dir / "12_grants_jobs_principal.sql", v)))
    return steps


def provision(
    execute: Callable[[str], object],
    settings: Settings,
    options: ProvisionOptions = ProvisionOptions(),
    sql_dir: Path = SQL_DIR,
) -> list[tuple[str, str]]:
    """Run every step; returns ``[(step, status)]`` with status ``ok`` / ``skipped: <error>``."""
    results: list[tuple[str, str]] = []
    for step in plan(settings, options, sql_dir):
        try:
            for stmt in step.statements:
                log.info("[%s] %s", step.name, stmt)
                execute(stmt)
        except Exception as exc:  # noqa: BLE001 - optional steps degrade to a warning
            if not step.optional:
                raise
            msg = str(exc).splitlines()[0][:300]
            log.warning("optional step %s failed: %s", step.name, msg)
            results.append((step.name, f"skipped: {msg}"))
            continue
        results.append((step.name, "ok"))
    return results


def landing_dirs(settings: Settings) -> list[str]:
    return [settings.landing_path(*parts) for parts in LANDING_DIRS]


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def summarize(results: Iterable[tuple[str, str]]) -> str:
    return "\n".join(f"{name:<24} {status}" for name, status in results)
