from pathlib import Path

import pytest

import run_all

REPO = Path(__file__).resolve().parents[2]
DIMS = {"load_dim_branch", "load_dim_account", "load_dim_customer"}


@pytest.fixture(scope="module")
def workflow():
    return run_all.load_workflow()


def test_dims_parallel_then_fact(workflow):
    assert run_all.dependency_waves(workflow["tasks"]) == [
        sorted(DIMS),
        ["load_fact_transaction"],
    ]
    fact = next(t for t in workflow["tasks"] if t["task_key"] == "load_fact_transaction")
    assert {d["task_key"] for d in fact["depends_on"]} == DIMS
    assert fact["run_if"] == "ALL_SUCCESS"


def test_task_files_exist_and_import(workflow):
    for task in workflow["tasks"]:
        assert (REPO / task["spark_python_task"]["python_file"]).is_file()
        module = __import__(run_all.task_module(task), fromlist=["run"])
        assert callable(module.run)


def test_task_parameters_reference_job_parameters(workflow):
    params = {p["name"]: p["default"] for p in workflow["parameters"]}
    for task in workflow["tasks"]:
        resolved = run_all.resolve_parameters(task, params)
        assert not any("{{" in p for p in resolved)


def test_dependency_waves_rejects_cycles():
    tasks = [
        {"task_key": "a", "depends_on": [{"task_key": "b"}]},
        {"task_key": "b", "depends_on": [{"task_key": "a"}]},
    ]
    with pytest.raises(ValueError, match="cycle"):
        run_all.dependency_waves(tasks)
