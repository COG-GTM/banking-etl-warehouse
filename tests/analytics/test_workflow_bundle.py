"""Structural checks for the Databricks Asset Bundle in databricks/workflows."""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / "databricks" / "workflows"

EXPECTED_ORDER = [
    "load_dim_branch",
    "load_dim_account",
    "load_dim_customer",
    "load_fact_transaction",
    "run_analytics",
]


def load(name: str) -> dict:
    with (WORKFLOWS / name).open() as fh:
        return yaml.safe_load(fh)


def test_bundle_root_parses():
    bundle = load("databricks.yml")
    assert bundle["bundle"]["name"] == "banking_dwh"
    assert "banking_dwh_job.yml" in bundle["include"]
    assert set(bundle["targets"]) == {"dev", "prod"}
    for target in bundle["targets"].values():
        assert {"catalog", "schema"} <= set(target["variables"])
    assert (
        bundle["targets"]["dev"]["variables"]["catalog"]
        != bundle["targets"]["prod"]["variables"]["catalog"]
    )


def test_job_tasks_form_linear_chain():
    job = load("banking_dwh_job.yml")["resources"]["jobs"]["banking_dwh_job"]
    tasks = job["tasks"]
    keys = [t["task_key"] for t in tasks]
    assert keys == EXPECTED_ORDER
    assert "depends_on" not in tasks[0]
    for prev, task in pairwise(tasks):
        assert [d["task_key"] for d in task["depends_on"]] == [prev["task_key"]]


def test_job_task_entry_points():
    job = load("banking_dwh_job.yml")["resources"]["jobs"]["banking_dwh_job"]
    files = {t["task_key"]: t["spark_python_task"]["python_file"] for t in job["tasks"]}
    assert files["load_dim_branch"].endswith(
        "databricks/jobs/dimensions/load_dimensions.py"
    )
    assert files["load_dim_account"].endswith(
        "databricks/jobs/dimensions/load_dimensions.py"
    )
    assert files["load_dim_customer"].endswith(
        "databricks/jobs/dimensions/load_dimensions.py"
    )
    assert files["load_fact_transaction"].endswith(
        "databricks/jobs/fact_transaction/load_fact_transaction.py"
    )
    assert files["run_analytics"].endswith("databricks/analytics/run_analytics.py")
    assert (WORKFLOWS.parents[1] / "databricks/analytics/run_analytics.py").exists()


def test_job_is_parameterised_by_catalog_and_schema():
    job = load("banking_dwh_job.yml")["resources"]["jobs"]["banking_dwh_job"]
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params == {"catalog": "${var.catalog}", "schema": "${var.schema}"}
    for task in job["tasks"]:
        args = task["spark_python_task"]["parameters"]
        assert "{{job.parameters.catalog}}" in args
        assert "{{job.parameters.schema}}" in args
