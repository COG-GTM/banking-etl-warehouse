"""Static checks on the banking_etl_pipeline Databricks Workflow definition.

Parses databricks/resources/jobs/banking_etl_pipeline.job.yml (no Spark / workspace needed) and asserts
that the DAG preserves the legacy Talend order DimBranch -> DimAccount -> DimCustomer -> FactTransaction.
"""

from __future__ import annotations

from graphlib import TopologicalSorter
from pathlib import Path

import pytest
import yaml

DATABRICKS_ROOT = Path(__file__).resolve().parents[2]
JOB_FILE = DATABRICKS_ROOT / "resources" / "jobs" / "banking_etl_pipeline.job.yml"
JOB_KEY = "banking_etl_pipeline"

EXPECTED_TASKS = [
    "bronze_sqlserver",
    "bronze_files",
    "dim_branch",
    "dim_account",
    "dim_customer",
    "fact_transaction",
    "analytics",
    "validation",
]

EXPECTED_DEPENDS_ON = {
    "bronze_sqlserver": set(),
    "bronze_files": set(),
    "dim_branch": {"bronze_sqlserver", "bronze_files"},
    "dim_account": {"dim_branch"},
    "dim_customer": {"dim_account"},
    "fact_transaction": {"dim_customer"},
    "analytics": {"fact_transaction"},
    "validation": {"analytics"},
}

LEGACY_TALEND_ORDER = ["dim_branch", "dim_account", "dim_customer", "fact_transaction"]


@pytest.fixture(scope="module")
def bundle_doc() -> dict:
    with JOB_FILE.open() as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="module")
def job(bundle_doc: dict) -> dict:
    return bundle_doc["resources"]["jobs"][JOB_KEY]


@pytest.fixture(scope="module")
def tasks(job: dict) -> dict[str, dict]:
    return {t["task_key"]: t for t in job["tasks"]}


@pytest.fixture(scope="module")
def graph(tasks: dict[str, dict]) -> dict[str, set[str]]:
    return {key: {d["task_key"] for d in t.get("depends_on", [])} for key, t in tasks.items()}


def _ancestors(graph: dict[str, set[str]], node: str) -> set[str]:
    seen: set[str] = set()
    stack = list(graph[node])
    while stack:
        cur = stack.pop()
        if cur not in seen:
            seen.add(cur)
            stack.extend(graph[cur])
    return seen


def test_task_keys_are_unique_and_complete(job: dict, tasks: dict[str, dict]) -> None:
    keys = [t["task_key"] for t in job["tasks"]]
    assert len(keys) == len(set(keys))
    assert sorted(keys) == sorted(EXPECTED_TASKS)


def test_dependencies_match_expected_edges(graph: dict[str, set[str]]) -> None:
    assert graph == EXPECTED_DEPENDS_ON


def test_every_dependency_references_a_defined_task(graph: dict[str, set[str]]) -> None:
    for key, deps in graph.items():
        assert deps <= graph.keys(), f"{key} depends on undefined task(s) {deps - graph.keys()}"


def test_dag_is_acyclic_and_topologically_ordered(graph: dict[str, set[str]]) -> None:
    order = list(TopologicalSorter(graph).static_order())
    assert set(order[:2]) == {"bronze_sqlserver", "bronze_files"}
    assert order[2:] == EXPECTED_TASKS[2:]


def test_bronze_tasks_run_in_parallel(graph: dict[str, set[str]]) -> None:
    assert graph["bronze_sqlserver"] == set()
    assert graph["bronze_files"] == set()
    assert "bronze_files" not in _ancestors(graph, "bronze_sqlserver")
    assert "bronze_sqlserver" not in _ancestors(graph, "bronze_files")


@pytest.mark.parametrize("earlier, later", list(zip(LEGACY_TALEND_ORDER, LEGACY_TALEND_ORDER[1:])))
def test_legacy_talend_dim_before_fact_order(graph: dict[str, set[str]], earlier: str, later: str) -> None:
    assert earlier in _ancestors(graph, later)


def test_fact_waits_for_every_dimension_and_both_bronze_sources(graph: dict[str, set[str]]) -> None:
    assert {"bronze_sqlserver", "bronze_files", "dim_branch", "dim_account", "dim_customer"} <= _ancestors(
        graph, "fact_transaction"
    )


def test_validation_is_the_single_terminal_task(graph: dict[str, set[str]]) -> None:
    downstream_of_something = {d for deps in graph.values() for d in deps}
    sinks = set(graph) - downstream_of_something
    assert sinks == {"validation"}
    assert _ancestors(graph, "validation") == set(graph) - {"validation"}


@pytest.mark.parametrize("task_key", EXPECTED_TASKS)
def test_notebook_paths_follow_agreed_layout(tasks: dict[str, dict], task_key: str) -> None:
    rel = tasks[task_key]["notebook_task"]["notebook_path"]
    resolved = (JOB_FILE.parent / rel).resolve()
    assert resolved == DATABRICKS_ROOT / "notebooks" / f"{task_key}.py"


def test_tasks_are_serverless(job: dict, tasks: dict[str, dict]) -> None:
    assert "job_clusters" not in job
    for key, task in tasks.items():
        for field in ("new_cluster", "existing_cluster_id", "job_cluster_key"):
            assert field not in task, f"{key} must run on serverless compute, found {field}"
    envs = job["environments"]
    assert envs and all("environment_key" in e and "spec" in e for e in envs)


def test_job_parameters(job: dict) -> None:
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params == {
        "catalog": "migration_demo",
        "schema_prefix": "banking_mig_",
        "run_date": "{{job.start_time.iso_date}}",
    }


@pytest.mark.parametrize("task_key", EXPECTED_TASKS)
def test_every_task_has_timeout_and_retry_policy(tasks: dict[str, dict], task_key: str) -> None:
    task = tasks[task_key]
    assert 0 < task["timeout_seconds"] <= 3600
    assert task["max_retries"] >= 0
    if task["max_retries"] > 0:
        assert task["min_retry_interval_millis"] >= 30000


def test_bronze_tasks_retry_for_auto_loader_restarts(tasks: dict[str, dict]) -> None:
    assert tasks["bronze_files"]["max_retries"] >= 2
    assert tasks["bronze_sqlserver"]["max_retries"] >= 1


def test_job_level_timeout_concurrency_and_schedule(job: dict, tasks: dict[str, dict]) -> None:
    assert job["max_concurrent_runs"] == 1
    assert job["schedule"]["pause_status"] == "PAUSED"


def test_job_timeout_covers_critical_path_of_task_timeouts(job: dict, tasks: dict[str, dict], graph) -> None:
    finish: dict[str, int] = {}
    for key in TopologicalSorter(graph).static_order():
        finish[key] = tasks[key]["timeout_seconds"] + max((finish[d] for d in graph[key]), default=0)
    critical_path = max(finish.values())
    assert critical_path == 4.5 * 3600
    assert job["timeout_seconds"] >= critical_path


def test_failure_notifications(bundle_doc: dict, job: dict) -> None:
    assert job["email_notifications"]["on_failure"] == ["${var.banking_etl_alert_email}"]
    assert "default" in bundle_doc["variables"]["banking_etl_alert_email"]
    assert job["notification_settings"]["no_alert_for_canceled_runs"] is True
    rule = job["health"]["rules"][0]
    assert rule["metric"] == "RUN_DURATION_SECONDS" and rule["value"] < job["timeout_seconds"]
