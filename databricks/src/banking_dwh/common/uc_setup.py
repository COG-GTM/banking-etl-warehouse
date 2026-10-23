"""Unity Catalog namespace setup statements (Ticket 1).

``setup_statements`` returns idempotent SQL for the catalog, the four layer schemas and
the managed volumes. ``notebooks/00_setup/01_setup_unity_catalog.py`` executes them.
"""

from __future__ import annotations

from .config import LAYERS, EnvConfig, validate_identifier


def _q(text: str) -> str:
    return text.replace("\\", "\\\\").replace("'", "\\'")


def setup_statements(cfg: EnvConfig, *, apply_grants: bool = False) -> list[str]:
    stmts: list[str] = []
    if cfg.create_catalog:
        stmts.append(
            f"CREATE CATALOG IF NOT EXISTS {cfg.catalog} COMMENT '{_q(cfg.catalog_comment)}'"
        )
    for layer in LAYERS:
        comment = cfg.layer_comments.get(layer, "")
        stmts.append(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)} COMMENT '{_q(comment)}'")
    for layer, vols in cfg.volumes.items():
        for name, comment in vols.items():
            validate_identifier(name, "volume")
            stmts.append(
                f"CREATE VOLUME IF NOT EXISTS {cfg.schema(layer)}.{name} COMMENT '{_q(comment)}'"
            )
    if apply_grants:
        for principal in cfg.readers:
            p = principal.replace("`", "")
            if cfg.create_catalog:
                stmts.append(f"GRANT USE CATALOG ON CATALOG {cfg.catalog} TO `{p}`")
            for layer in ("silver", "gold"):
                stmts.append(f"GRANT USE SCHEMA, SELECT ON SCHEMA {cfg.schema(layer)} TO `{p}`")
    return stmts


def landing_folders(cfg: EnvConfig) -> list[str]:
    """Volume folders to pre-create (``fs cp`` into a missing folder fails)."""
    folders = [cfg.landing_path(k) for k in cfg.landing if k != "sqlserver"]
    assert cfg.jdbc is not None
    folders += [cfg.landing_path("sqlserver", t) for t in cfg.jdbc.source_tables]
    return folders
