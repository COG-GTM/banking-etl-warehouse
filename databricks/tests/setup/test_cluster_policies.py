import json

from banking_etl.setup.policies import apply_policy, grant_can_use, load_policies


def _policy():
    (policy,) = load_policies()
    return policy


def test_job_cluster_policy_definition():
    p = _policy()
    d = p["definition"]
    assert p["name"] == "banking-etl-job-cluster"
    assert d["cluster_type"] == {"type": "fixed", "value": "job"}
    assert all(v.startswith("17.3.") for v in d["spark_version"]["values"])
    assert d["spark_version"]["defaultValue"] == "17.3.x-scala2.13"
    assert {"i3.xlarge", "m5d.large"} <= set(d["node_type_id"]["values"])
    assert d["driver_node_type_id"]["values"] == d["node_type_id"]["values"]
    assert set(d["data_security_mode"]["values"]) <= {"SINGLE_USER", "DATA_SECURITY_MODE_DEDICATED"}
    assert d["autotermination_minutes"]["maxValue"] <= 60
    assert d["custom_tags.project"] == {"type": "fixed", "value": "banking-etl-warehouse"}
    for attr in d.values():
        assert attr["type"] in {"fixed", "forbidden", "allowlist", "blocklist", "regex", "range", "unlimited"}


class FakeCli:
    def __init__(self, existing):
        self.existing = existing
        self.calls = []

    def __call__(self, args, **kw):
        self.calls.append(list(args))
        if args[:2] == ["cluster-policies", "list"]:
            return self.existing
        if args[:2] == ["cluster-policies", "create"]:
            return {"policy_id": "NEW"}
        return None


def test_apply_policy_creates_then_edits():
    cli = FakeCli([{"name": "Personal Compute", "policy_id": "P0"}])
    assert apply_policy(_policy(), runner=cli) == ("created", "NEW")
    body = json.loads(cli.calls[-1][-1])
    assert json.loads(body["definition"])["cluster_type"]["value"] == "job"

    cli = FakeCli([{"name": "banking-etl-job-cluster", "policy_id": "P1"}])
    assert apply_policy(_policy(), runner=cli) == ("updated", "P1")
    assert cli.calls[-1][:2] == ["cluster-policies", "edit"]
    assert json.loads(cli.calls[-1][-1])["policy_id"] == "P1"


def test_grant_can_use():
    cli = FakeCli([])
    grant_can_use("P1", ["data-engineers"], ["app-1"], runner=cli)
    acl = json.loads(cli.calls[0][-1])["access_control_list"]
    assert acl == [
        {"group_name": "data-engineers", "permission_level": "CAN_USE"},
        {"service_principal_name": "app-1", "permission_level": "CAN_USE"},
    ]
    cli = FakeCli([])
    grant_can_use("P1", runner=cli)
    assert cli.calls == []
