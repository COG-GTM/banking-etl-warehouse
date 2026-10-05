"""Cluster policy definitions (``resources/policies/*.json``) and an idempotent create-or-edit."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

from banking_etl.setup.cli import Runner, run_cli

POLICY_DIR = Path(__file__).resolve().parents[3] / "resources" / "policies"


def load_policies(policy_dir: Path = POLICY_DIR) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted(Path(policy_dir).glob("*.json"))]


def apply_policy(policy: dict, runner: Runner = run_cli) -> tuple[str, str]:
    """Create the policy, or edit it in place if one with the same name exists. Returns (action, policy_id)."""
    body = {
        "name": policy["name"],
        "description": policy.get("description", ""),
        "definition": json.dumps(policy["definition"]),
    }
    existing = {p["name"]: p["policy_id"] for p in (runner(["cluster-policies", "list"]) or [])}
    if policy["name"] in existing:
        policy_id = existing[policy["name"]]
        runner(["cluster-policies", "edit", "--json", json.dumps({**body, "policy_id": policy_id})], parse_json=False)
        return "updated", policy_id
    created = runner(["cluster-policies", "create", "--json", json.dumps(body)])
    return "created", created["policy_id"]


def grant_can_use(policy_id: str, groups: Sequence[str] = (), service_principals: Sequence[str] = (),
                  runner: Runner = run_cli) -> None:
    acl = [{"group_name": g, "permission_level": "CAN_USE"} for g in groups]
    acl += [{"service_principal_name": sp, "permission_level": "CAN_USE"} for sp in service_principals]
    if acl:
        runner(["permissions", "update", "cluster-policies", policy_id, "--json",
                json.dumps({"access_control_list": acl})])
