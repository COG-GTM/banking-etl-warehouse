# Orchestration: `banking_etl_pipeline` Databricks Workflow

The legacy solution ran four Talend jobs by hand in a fixed order
(`Load_DimBranch` -> `Load_DimAccount` -> `Load_DimCustomer` -> `Load_FactTransaction`, README "Modular ETL
Pipelines in Talend"), then the reporting stored procedures were called ad hoc. On Databricks this is a single
Lakeflow Job declared in the asset bundle:
[`databricks/resources/jobs/banking_etl_pipeline.job.yml`](../databricks/resources/jobs/banking_etl_pipeline.job.yml).

## DAG

```
bronze_sqlserver ─┐
                  ├─> dim_branch -> dim_account -> dim_customer -> fact_transaction -> analytics -> validation
bronze_files ─────┘
```

| Task | Notebook | Legacy equivalent | Depends on | Retries | Timeout |
|---|---|---|---|---|---|
| `bronze_sqlserver` | `notebooks/bronze_sqlserver.py` | Talend `tDBInput` reads from SQL Server `sample` | - | 2 (60 s, retry on timeout) | 1 h |
| `bronze_files` | `notebooks/bronze_files.py` | Talend `tFileInputExcel` / `tFileInputDelimited` | - | 3 (30 s, retry on timeout) | 1 h |
| `dim_branch` | `notebooks/dim_branch.py` | `Load_DimBranch` | both bronze tasks | 1 (60 s) | 30 min |
| `dim_account` | `notebooks/dim_account.py` | `Load_DimAccount` | `dim_branch` | 1 (60 s) | 30 min |
| `dim_customer` | `notebooks/dim_customer.py` | `Load_DimCustomer` | `dim_account` | 1 (60 s) | 30 min |
| `fact_transaction` | `notebooks/fact_transaction.py` | `Load_FactTransaction` | `dim_customer` | 1 (60 s) | 1 h |
| `analytics` | `notebooks/analytics.py` | `sp_DailyTransaction`, `sp_BalancePerCustomer` | `fact_transaction` | 1 (60 s) | 30 min |
| `validation` | `notebooks/validation.py` | (new) parity checks vs legacy DWH | `analytics` | 0 | 30 min |

Notes on the design:

- **Dim-before-fact order is kept strictly linear.** The three dims don't depend on each other's data, so they could
  run in parallel. They stay serial to match the Talend run order one-to-one, which keeps parity comparisons and
  run logs easy to read. `fact_transaction` therefore only starts after every dim and both bronze sources are done.
- **Both bronze tasks run in parallel** because they read independent sources (JDBC vs. landing volume).
- **Retries.** The bronze tasks get the most retries: JDBC hiccups are transient, and Auto Loader *stops the stream
  by design* when `addNewColumns` schema evolution finds a new column, so a task retry resumes it. Validation has no
  retry because a parity failure is deterministic.
- **Serverless only.** No `new_cluster` / `job_clusters` (the workspace rejects them). Notebook tasks run on
  serverless notebook compute. A job-level `environments` entry (`banking_etl_serverless`, environment version 2)
  is declared for non-notebook tasks (e.g. a future `python_wheel_task` for `banking_etl`). The notebook tasks in this job
  don't reference an `environment_key`; notebooks install their own dependencies.
- **Job parameters** (pushed to every notebook as widgets):
  - `catalog`, default `migration_demo`
  - `schema_prefix`, default `banking_mig_`. Schemas are `<prefix>bronze|silver|gold|ops`.
  - `run_date`, default `{{job.start_time.iso_date}}`

  Override them per run with `databricks jobs run-now --json '{"job_id": ..., "job_parameters": {...}}'`.
- **Failure notifications.** `email_notifications.on_failure` and `on_duration_warning_threshold_exceeded` go to the
  bundle variable `banking_etl_alert_email` (placeholder default; set it per target or with
  `--var banking_etl_alert_email=...`). A health rule warns when a run exceeds 90 min. The job timeout is 6 h: the critical path of per-task timeouts is 4.5 h, plus headroom for retries.
  Skipped and canceled runs don't alert.
- **Concurrency and schedule.** `max_concurrent_runs: 1` with queueing, so overlapping triggers wait instead of
  racing on the same Delta tables. A daily 02:00 UTC schedule is declared but `PAUSED`; the legacy jobs had no
  scheduler, so turning it on is an operational decision.

## Bundle integration

The job file defines `resources.jobs.banking_etl_pipeline` plus the `banking_etl_alert_email` variable. The bundle
root (`databricks/databricks.yml`, owned by the scaffolding ticket) must include it, e.g.
`include: [resources/jobs/*.yml]`. Notebook paths are relative to the job file
(`../../notebooks/<task>.py`), so they resolve to `databricks/notebooks/<task>.py`. Those are the agreed paths
the other tickets deliver.

## Tests

`databricks/tests/orchestration/test_job_dag.py` parses the YAML (pure Python, no Spark) and asserts:

- the exact edge set and that the graph is acyclic;
- the topological order (bronze pair, then dim_branch, dim_account, dim_customer, fact_transaction, analytics,
  validation) and that the legacy dim-before-fact order holds;
- the two bronze tasks are parallel roots and `validation` is the single sink;
- notebook paths, serverless-only compute, job parameters, per-task timeouts and retries, and failure
  notifications.

```bash
cd databricks && pytest tests/orchestration
```

## Live DAG smoke run (placeholder notebooks)

The real task notebooks are delivered by parallel tickets. To check the DAG on the workspace before they land,
`databricks/tests/orchestration/live_submit.py` does three things:

1. Uploads `placeholder_notebook.py` once per task to `/Workspace/Shared/banking_etl_migration_v2/ticket_8/<task>`.
   The placeholder logs its task name and parameters, touches Spark, sleeps a few seconds, and exits with a JSON
   payload.
2. Builds a `runs/submit` payload **from the job YAML**: task keys, `depends_on`, timeouts, environments,
   notifications, and parameters as `base_parameters`. `max_retries` / `min_retry_interval_millis` /
   `retry_on_timeout` are left out because one-time runs don't support them.
3. Submits the one-time run and prints each task's start/end times.

```bash
python databricks/tests/orchestration/live_submit.py            # live
python databricks/tests/orchestration/live_submit.py --dry-run  # print payload only
```

Once the real notebooks exist, deploy and run the actual job with the bundle (`databricks bundle deploy -t <target>`,
then `databricks bundle run banking_etl_pipeline`). Do this from the integration branch, not from parallel
ticket sessions, because they share the bundle root.
