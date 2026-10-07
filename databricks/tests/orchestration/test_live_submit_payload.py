"""The one-time live smoke run must mirror the bundle job's DAG exactly."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_submit  # noqa: E402


def test_submit_payload_mirrors_job_dag() -> None:
    doc, job = live_submit.load_job()
    payload = live_submit.build_submit_payload(doc, job, "2026-10-07", 1)

    def edges(tasks: list[dict]) -> dict[str, set[str]]:
        return {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tasks}

    assert edges(payload["tasks"]) == edges(job["tasks"])
    for task in payload["tasks"]:
        assert not live_submit.SUBMIT_UNSUPPORTED_TASK_FIELDS & task.keys()
        nb = task["notebook_task"]
        assert nb["notebook_path"] == f"{live_submit.WORKSPACE_DIR}/{task['task_key']}"
        assert nb["base_parameters"]["task_name"] == task["task_key"]
        assert nb["base_parameters"]["run_date"] == "2026-10-07"
        assert nb["base_parameters"]["catalog"] == "migration_demo"
    assert payload["environments"] == job["environments"]
    assert payload["email_notifications"]["on_failure"] == ["data-platform-alerts@example.com"]
