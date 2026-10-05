"""Idempotent creation of the SQL Server JDBC secret scope. Values come from env vars only."""
from __future__ import annotations

import os
from typing import Mapping, Sequence

from banking_etl.setup.cli import Runner, run_cli

SECRET_KEYS = ("jdbc-host", "jdbc-port", "jdbc-database", "jdbc-user", "jdbc-password")


def env_var_for(key: str) -> str:
    """``jdbc-host`` -> ``BANKING_ETL_JDBC_HOST``."""
    return "BANKING_ETL_" + key.upper().replace("-", "_")


def read_values(env: Mapping[str, str] = os.environ) -> dict[str, str]:
    missing = [env_var_for(k) for k in SECRET_KEYS if not env.get(env_var_for(k))]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return {k: env[env_var_for(k)] for k in SECRET_KEYS}


def ensure_scope(scope: str, runner: Runner = run_cli) -> bool:
    """Create the scope if absent. Returns True if it was created."""
    existing = {s["name"] for s in (runner(["secrets", "list-scopes"]) or [])}
    if scope in existing:
        return False
    runner(["secrets", "create-scope", scope], parse_json=False)
    return True


def put_values(scope: str, values: Mapping[str, str], runner: Runner = run_cli) -> None:
    # Values go through stdin so they never appear in argv / process listings.
    for key, value in values.items():
        runner(["secrets", "put-secret", scope, key], stdin=value, parse_json=False)


def grant_read(scope: str, principals: Sequence[str], runner: Runner = run_cli) -> None:
    for principal in principals:
        runner(["secrets", "put-acl", scope, principal, "READ"], parse_json=False)
