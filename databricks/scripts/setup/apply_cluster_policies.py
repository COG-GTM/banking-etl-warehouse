#!/usr/bin/env python3
"""Create or update every cluster policy in resources/policies/*.json (needs workspace admin).

  python scripts/setup/apply_cluster_policies.py [--group data-engineers] [--service-principal <app-id>]
"""
import argparse

import _bootstrap  # noqa: F401

from banking_etl.setup.policies import apply_policy, grant_can_use, load_policies


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--group", action="append", default=[], help="group granted CAN_USE (repeatable)")
    ap.add_argument("--service-principal", action="append", default=[], help="SP application id granted CAN_USE")
    a = ap.parse_args()
    for policy in load_policies():
        action, policy_id = apply_policy(policy)
        grant_can_use(policy_id, a.group, a.service_principal)
        print(f"{policy['name']}: {action} ({policy_id})")


if __name__ == "__main__":
    main()
