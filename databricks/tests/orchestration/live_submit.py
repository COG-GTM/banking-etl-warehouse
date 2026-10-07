"""Submit a one-time Databricks run that mirrors banking_etl_pipeline.job.yml with placeholder notebooks.

The real task notebooks are delivered by other tickets, so this uploads placeholder_notebook.py once per
task under WORKSPACE_DIR and submits the DAG (task keys, depends_on, timeouts, environments, notifications)
from the job YAML via `databricks jobs submit`. It then prints the task order and timings.

Usage (needs DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET):
    python databricks/tests/orchestration/live_submit.py [--run-date 2026-10-07]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
JOB_FILE = HERE.parents[1] / "resources" / "jobs" / "banking_etl_pipeline.job.yml"
PLACEHOLDER = HERE / "placeholder_notebook.py"
WORKSPACE_DIR = "/Workspace/Shared/banking_etl_migration_v2/ticket_8"

# runs/submit does not accept these job-level / task-level settings (they are saved-job features).
SUBMIT_UNSUPPORTED_TASK_FIELDS = {"max_retries", "min_retry_interval_millis", "retry_on_timeout", "description"}


def _cli(*args: str) -> str:
    return subprocess.run(["databricks", *args], check=True, capture_output=True, text=True).stdout


def load_job() -> tuple[dict, dict]:
    doc = yaml.safe_load(JOB_FILE.read_text())
    return doc, doc["resources"]["jobs"]["banking_etl_pipeline"]


def resolve_vars(value, variables: dict):
    if isinstance(value, str):
        for name, spec in variables.items():
            value = value.replace("${var.%s}" % name, str(spec.get("default", "")))
        return value
    if isinstance(value, list):
        return [resolve_vars(v, variables) for v in value]
    if isinstance(value, dict):
        return {k: resolve_vars(v, variables) for k, v in value.items()}
    return value


def build_submit_payload(doc: dict, job: dict, run_date: str, sleep_seconds: int) -> dict:
    variables = doc.get("variables", {})
    params = {p["name"]: p["default"] for p in job["parameters"]}
    params["run_date"] = run_date
    tasks = []
    for task in job["tasks"]:
        key = task["task_key"]
        submit_task = {k: v for k, v in task.items() if k not in SUBMIT_UNSUPPORTED_TASK_FIELDS}
        submit_task["notebook_task"] = {
            "notebook_path": f"{WORKSPACE_DIR}/{key}",
            "source": "WORKSPACE",
            "base_parameters": {**params, "task_name": key, "sleep_seconds": str(sleep_seconds)},
        }
        tasks.append(submit_task)
    payload = {
        "run_name": f"ticket_8_banking_etl_pipeline_dag_{run_date}",
        "timeout_seconds": job["timeout_seconds"],
        "tasks": tasks,
        "environments": job.get("environments", []),
        "email_notifications": resolve_vars(
            {k: v for k, v in job["email_notifications"].items() if k != "on_duration_warning_threshold_exceeded"},
            variables,
        ),
        "notification_settings": job.get("notification_settings", {}),
        "queue": job.get("queue", {"enabled": True}),
    }
    return payload


def upload_placeholders(task_keys: list[str]) -> None:
    _cli("workspace", "mkdirs", WORKSPACE_DIR)
    for key in task_keys:
        _cli(
            "workspace",
            "import",
            f"{WORKSPACE_DIR}/{key}",
            "--file",
            str(PLACEHOLDER),
            "--format",
            "SOURCE",
            "--language",
            "PYTHON",
            "--overwrite",
        )


def _ts(ms: int | None) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S") if ms else "-"


def report(run: dict) -> None:
    print(f"run_id={run['run_id']} state={run['state'].get('result_state')} url={run.get('run_page_url')}")
    rows = sorted(run["tasks"], key=lambda t: (t.get("start_time") or 0, t["task_key"]))
    print(f"{'task_key':<18} {'depends_on':<34} {'start':>9} {'end':>9} {'secs':>6} result")
    for t in rows:
        deps = ",".join(d["task_key"] for d in t.get("depends_on", [])) or "-"
        secs = ((t.get("end_time") or 0) - (t.get("start_time") or 0)) / 1000
        print(
            f"{t['task_key']:<18} {deps:<34} {_ts(t.get('start_time')):>9} {_ts(t.get('end_time')):>9} "
            f"{secs:>6.1f} {t['state'].get('result_state')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=date.today().isoformat())
    parser.add_argument("--sleep-seconds", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    doc, job = load_job()
    payload = build_submit_payload(doc, job, args.run_date, args.sleep_seconds)
    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return
    upload_placeholders([t["task_key"] for t in job["tasks"]])
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(payload, fh)
    run = json.loads(_cli("jobs", "submit", "--json", f"@{fh.name}", "--timeout", "60m", "-o", "json"))
    run = json.loads(_cli("jobs", "get-run", str(run["run_id"]), "-o", "json"))
    report(run)


if __name__ == "__main__":
    main()
