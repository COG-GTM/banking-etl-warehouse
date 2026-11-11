from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

BUNDLE_ROOT = Path(__file__).resolve().parents[2]


def test_job_definition_is_serverless_and_parameterised():
    spec = yaml.safe_load((BUNDLE_ROOT / "resources" / "bronze_ingestion.yml").read_text())
    job = spec["resources"]["jobs"]["bronze_ingestion"]
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params["catalog"] == "${var.catalog}"
    assert params["bronze_schema"] == "bronze"
    assert params["sample_db_mode"] in {"files", "jdbc", "federation"}
    assert "job_clusters" not in job
    sources = set()
    for task in job["tasks"]:
        assert "new_cluster" not in task and "existing_cluster_id" not in task
        nb = (BUNDLE_ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve()
        assert nb.is_file()
        sources.add(task["notebook_task"]["base_parameters"]["sources"])
    assert sources == {"sample_db", "transaction_csv", "transaction_excel"}


def test_notebook_widgets_cover_job_parameters():
    spec = yaml.safe_load((BUNDLE_ROOT / "resources" / "bronze_ingestion.yml").read_text())
    job = spec["resources"]["jobs"]["bronze_ingestion"]
    notebook = (BUNDLE_ROOT / "src" / "notebooks" / "bronze_ingestion.py").read_text()
    for name in [p["name"] for p in job["parameters"]] + ["sources", "run_id"]:
        assert f'"{name}":' in notebook
