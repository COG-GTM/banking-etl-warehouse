"""Ticket 10: structure of the banking_dwh_pipeline bundle job."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
JOB_FILE = ROOT / "resources" / "jobs" / "banking_dwh_pipeline.yml"

ORDER = [
    ("setup_unity_catalog", "00_setup/01_setup_unity_catalog", []),
    ("create_star_schema", "00_setup/02_create_star_schema", ["setup_unity_catalog"]),
    ("bronze_ingest_sqlserver", "bronze/01_ingest_sqlserver", ["create_star_schema"]),
    ("bronze_ingest_csv_autoloader", "bronze/02_ingest_csv_autoloader", ["create_star_schema"]),
    ("bronze_ingest_excel", "bronze/03_ingest_excel", ["create_star_schema"]),
    ("load_dim_branch", "silver/01_load_dim_branch",
     ["bronze_ingest_sqlserver", "bronze_ingest_csv_autoloader", "bronze_ingest_excel"]),
    ("load_dim_account", "silver/02_load_dim_account", ["load_dim_branch"]),
    ("load_dim_customer", "silver/03_load_dim_customer", ["load_dim_account"]),
    ("load_fact_transaction", "silver/04_load_fact_transaction", ["load_dim_customer"]),
    ("dq_star_schema", "00_setup/03_dq_star_schema", ["load_fact_transaction"]),
    ("gold_objects", "gold/", ["dq_star_schema"]),
    ("reconcile", "orchestration/01_reconcile", ["gold_objects"]),
]


@pytest.fixture(scope="module")
def doc():
    return yaml.safe_load(JOB_FILE.read_text())


@pytest.fixture(scope="module")
def job(doc):
    return doc["resources"]["jobs"]["banking_dwh_pipeline"]


@pytest.fixture(scope="module")
def tasks(job):
    return {t["task_key"]: t for t in job["tasks"]}


def _path(doc, task) -> str:
    p = task["notebook_task"]["notebook_path"]
    if p.startswith("${var."):
        p = doc["variables"][p[len("${var."):-1]]["default"]
    return p


def test_bundle_includes_job_file():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "resources/jobs/*.yml" in bundle["include"]


def test_dependency_order_matches_runbook(doc, tasks):
    assert list(tasks) == [k for k, _, _ in ORDER]
    for key, nb, deps in ORDER:
        t = tasks[key]
        assert sorted(d["task_key"] for d in t.get("depends_on", [])) == sorted(deps), key
        assert _path(doc, t).startswith(f"../../notebooks/{nb}"), key


def test_job_parameters_default_to_bundle_variables(job):
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params == {"target": "${var.target_env}", "catalog": "${var.catalog}",
                      "schema_prefix": "${var.schema_prefix}", "parity_gate": "true"}


@pytest.mark.parametrize("key", [k for k, _, _ in ORDER])
def test_every_task_passes_target_catalog_and_prefix(tasks, key):
    # 00_config's target widget defaults to dev: a task without `target` would run prod jobs against dev.
    params = tasks[key]["notebook_task"]["base_parameters"]
    for name in ("target", "catalog", "schema_prefix"):
        assert params[name] == f"{{{{job.parameters.{name}}}}}", (key, name)


def test_serverless_only(job, tasks):
    assert job["environments"] == [{"environment_key": "default",
                                    "spec": {"client": "3", "dependencies": ["pyyaml"]}}]
    assert "job_clusters" not in job
    for key, t in tasks.items():
        assert t["environment_key"] == "default", key
        assert not {"new_cluster", "existing_cluster_id", "job_cluster_key"} & set(t), key
        assert set(t) & {"notebook_task"}, key


def test_retries_timeouts_and_alerts(job, tasks):
    assert job["timeout_seconds"] > 0 and job["max_concurrent_runs"] == 1
    assert job["email_notifications"]["on_failure"] == ["${var.failure_email}"]
    assert job["schedule"]["pause_status"] == "PAUSED"
    for key, t in tasks.items():
        assert 0 < t["timeout_seconds"] <= job["timeout_seconds"], key
        assert "max_retries" in t, key
    for key in ("bronze_ingest_sqlserver", "bronze_ingest_csv_autoloader", "bronze_ingest_excel"):
        assert tasks[key]["max_retries"] >= 1 and tasks[key]["retry_on_timeout"] is True
    for gate in ("dq_star_schema", "reconcile"):
        assert tasks[gate]["max_retries"] == 0
    assert tasks["dq_star_schema"]["notebook_task"]["base_parameters"]["fail_on_error"] == "true"
    assert tasks["reconcile"]["notebook_task"]["base_parameters"]["fail_on_error"] == "{{job.parameters.parity_gate}}"


def test_owned_notebooks_exist(doc, tasks):
    for key in ("setup_unity_catalog", "create_star_schema", "dq_star_schema", "reconcile"):
        assert (JOB_FILE.parent / _path(doc, tasks[key])).resolve().is_file(), key
