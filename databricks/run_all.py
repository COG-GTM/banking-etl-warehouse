"""Run the ETL DAG from workflows/etl_workflow.json without the Databricks Jobs service.

Use this from a notebook / local Spark, or where deploying the JSON workflow is not an
option. Tasks run in dependency waves (the three dims in parallel, then the fact); a task
only starts once all of its upstream tasks succeeded, matching ``run_if: ALL_SUCCESS``.

    python databricks/run_all.py --catalog main --schema dwh --landing-path /Volumes/main/dwh/landing
"""

from __future__ import annotations

import importlib
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import parse_args  # noqa: E402
from common.io import ensure_schema  # noqa: E402
from common.runner import get_spark  # noqa: E402

WORKFLOW = ROOT / "workflows" / "etl_workflow.json"
log = logging.getLogger("run_all")


def load_workflow(path: Path = WORKFLOW) -> dict:
    return json.loads(path.read_text())


def dependency_waves(tasks: list[dict]) -> list[list[str]]:
    """Group task keys into waves; every task's dependencies are in an earlier wave."""
    deps = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tasks}
    unknown = {d for ds in deps.values() for d in ds} - deps.keys()
    if unknown:
        raise ValueError(f"Unknown depends_on task keys: {sorted(unknown)}")
    done: set[str] = set()
    waves = []
    while len(done) < len(deps):
        ready = sorted(k for k, ds in deps.items() if k not in done and ds <= done)
        if not ready:
            raise ValueError(f"Dependency cycle among: {sorted(deps.keys() - done)}")
        waves.append(ready)
        done.update(ready)
    return waves


def task_module(task: dict) -> str:
    """databricks/jobs/load_dim_branch.py -> jobs.load_dim_branch"""
    rel = Path(task["spark_python_task"]["python_file"]).relative_to("databricks")
    return ".".join(rel.with_suffix("").parts)


def resolve_parameters(task: dict, job_params: dict[str, str]) -> list[str]:
    def sub(value: str) -> str:
        return re.sub(r"\{\{job\.parameters\.(\w+)\}\}", lambda m: job_params[m.group(1)], value)

    return [sub(p) for p in task["spark_python_task"].get("parameters", [])]


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    workflow = load_workflow()
    overrides = parse_args(argv)
    job_params = {p["name"]: p["default"] for p in workflow.get("parameters", [])}
    job_params.update(
        catalog=overrides.catalog or "",
        schema=overrides.schema,
        landing_path=overrides.landing_path,
        secret_scope=overrides.secret_scope,
    )
    tasks = {t["task_key"]: t for t in workflow["tasks"]}
    spark = get_spark()
    ensure_schema(spark, overrides)

    def run_task(key: str) -> int:
        task = tasks[key]
        cfg = parse_args(resolve_parameters(task, job_params) + ["--table-format", overrides.table_format])
        rows = importlib.import_module(task_module(task)).run(spark, cfg)
        log.info("Task %s succeeded (%d rows)", key, rows)
        return rows

    for i, wave in enumerate(dependency_waves(workflow["tasks"]), start=1):
        log.info("Wave %d: %s", i, ", ".join(wave))
        with ThreadPoolExecutor(max_workers=len(wave)) as pool:
            futures = {key: pool.submit(run_task, key) for key in wave}
        failed = [key for key, f in futures.items() if f.exception() is not None]
        for key in failed:
            log.error("Task %s failed", key, exc_info=futures[key].exception())
        if failed:
            log.error("Skipping downstream tasks because %s failed", ", ".join(failed))
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
