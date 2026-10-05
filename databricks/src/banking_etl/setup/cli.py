"""Thin wrapper around the ``databricks`` CLI (reuses its auth: DATABRICKS_HOST + OAuth M2M/PAT/profile)."""
from __future__ import annotations

import json
import subprocess
import time
from typing import Any, Callable, Sequence

Runner = Callable[..., Any]


def run_cli(args: Sequence[str], *, stdin: str | None = None, parse_json: bool = True) -> Any:
    """Run ``databricks <args>`` and return parsed JSON (or raw stdout). Raises RuntimeError on failure."""
    cmd = ["databricks", *args]
    if parse_json:
        cmd += ["-o", "json"]
    proc = subprocess.run(cmd, input=stdin, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        # Never echo stdin: it may carry secret values.
        raise RuntimeError(f"databricks {args[0]} {args[1] if len(args) > 1 else ''} failed: {proc.stderr.strip()}")
    out = proc.stdout.strip()
    if parse_json:
        return json.loads(out) if out else None
    return out


def warehouse_executor(warehouse_id: str, runner: Runner = run_cli, poll_seconds: float = 2.0) -> Callable[[str], dict]:
    """``execute(sql)`` backed by the SQL Statement Execution API on a SQL warehouse."""

    def execute(statement: str) -> dict:
        body = {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s", "on_wait_timeout": "CONTINUE"}
        resp = runner(["api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)])
        while resp["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(poll_seconds)
            resp = runner(["api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}"])
        if resp["status"]["state"] != "SUCCEEDED":
            err = resp["status"].get("error", {})
            raise RuntimeError(f"{err.get('error_code', resp['status']['state'])}: {err.get('message', '')}")
        return resp

    return execute
