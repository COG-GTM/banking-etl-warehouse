# Cutover runbook: Talend + SQL Server DWH -> Databricks Delta gold

Moves every consumer of the legacy SQL Server `DWH` star schema (`DimBranch`, `DimAccount`,
`DimCustomer`, `FactTransaction`, `sp_DailyTransaction`, `sp_BalancePerCustomer`) onto the Unity
Catalog gold layer (`migration_demo.banking_mig_gold.*`), with a hard reconciliation gate and a
tested rollback. The steps below are automated end to end by
`databricks/src/banking_etl/validation/cutover.py` (`dry_run_cutover`) and the notebook
`databricks/notebooks/validation.py`; every step is logged to `banking_mig_ops.cutover_log` and every
reconciliation check / mismatching row to `banking_mig_ops.reconciliation_results`.

| # | Step | Owner | Automated by | Gate |
|---|------|-------|--------------|------|
| 0 | Pre-checks + legacy baseline | Data eng | `legacy_baseline.py`, `seed` | baseline manifest checksums match |
| 1 | Historical load into Delta | Data eng | pipelines (tickets 3-8) / `historical_load` | jobs green |
| 2 | Freeze legacy | Data eng + DBA | `freeze` (records high watermark) | Talend schedules paused, no open DWH transactions |
| 3 | Final incremental | Data eng | `final_incremental` | Delta watermark >= legacy watermark |
| 4 | Reconcile gate | Data eng | `reconcile` | **100% of checks PASS, 0 mismatch rows** |
| 5 | Switch consumers | Data eng + BI | `point_consumers(..., "gold")` | consumer smoke counts = legacy |
| 6 | Rollback (if needed) | Data eng | `point_consumers(..., "legacy")` | consumers back on legacy |
| 7 | Talend decommission | Platform | `TALEND_DECOMMISSION_CHECKLIST` | after rollback window |

## 0. Pre-checks and legacy baseline (T-1 week)

1. Confirm the gold pipelines have run green for at least 3 consecutive days in `banking_mig_gold`.
2. Refresh the legacy baseline from production (or the `sample.bak` restore in non-prod):

   ```bash
   docker run -d --name mssql2022 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="$MSSQL_SA_PASSWORD" \
     -p 1433:1433 -v "$PWD/data_sources:/data_sources:ro" mcr.microsoft.com/mssql/server:2022-latest
   cd databricks && PYTHONPATH=src python -m banking_etl.validation.legacy_baseline \
     --restore --password "$MSSQL_SA_PASSWORD"
   ```

   This restores `sample`, runs `sql_scripts/01_create_tables.sql` / `02_create_procedures.sql`,
   replays the four Talend jobs as INSERTs (tUnite sql -> excel -> csv, tUniqRow first wins,
   FK violations rejected because `DIE_ON_ERROR=false`) and exports snapshots, proc outputs, rejects
   and a `manifest.json` (row counts + SHA-256) to `databricks/fixtures/legacy_dwh/`.
3. Upload the snapshots to the reconciliation volume:

   ```bash
   databricks fs mkdirs dbfs:/Volumes/migration_demo/banking_mig_t10/fixtures/legacy_dwh
   databricks fs cp -r --overwrite databricks/fixtures/legacy_dwh \
     dbfs:/Volumes/migration_demo/banking_mig_t10/fixtures/legacy_dwh
   ```
4. Run the notebook in `mode=dry_run_cutover` (section "Dry run") and require a GREEN report.
5. Announce the freeze window to consumers of the DWH (BI dashboards, ad-hoc users of the procs).

## 1. Historical load

Run the bronze -> silver -> gold jobs for the full history. The dry run uses
`historical_cutoff = 2024-01-22 00:00:00`, so the last CSV batch is held back and arrives in step 3,
which proves the incremental path rather than only a full reload.

## 2. Freeze legacy (T0)

1. Pause the Talend schedules for `Load_DimBranch`, `Load_DimAccount`, `Load_DimCustomer`,
   `Load_FactTransaction` (run order is DimBranch -> DimAccount -> DimCustomer -> FactTransaction;
   let an in-flight run finish rather than killing it mid-fact-load).
2. Stop writes to the `sample` source DB tables and to the landing folders for
   `transaction_excel.xlsx` / `transaction_csv.csv` (or route new files to a quarantine folder).
3. Record the legacy high watermark (`freeze` logs `MAX(TransactionDate)`, `MAX(TransactionID)` and
   per-table row counts into the `freeze_legacy` step of `cutover_log`).
4. Take a `COPY_ONLY` backup of `DWH` - this is the rollback point.

## 3. Final incremental

1. Run the bronze ingestion once more for files/rows that arrived before the freeze.
2. Dimensions are refreshed SCD-1 (full overwrite); facts are `MERGE ... WHEN NOT MATCHED THEN INSERT`
   on `transaction_id` for rows newer than the Delta watermark, so the step is idempotent.
3. FK violations go to `fact_transaction_rejects` (mirrors Talend's reject flow).
4. Check: `final_incremental` details show `fact_rows_after` = legacy `FactTransaction` count.

## 4. Reconcile gate = 100% match

Run `banking_etl.validation.reconcile.reconcile` (the notebook does this). Per gold table it checks:

| check_type | What |
|------------|------|
| `schema` | every gold column present (type drift reported in details) |
| `row_count` | legacy vs Delta row counts |
| `pk_unique` | no duplicate / NULL PKs on both sides |
| `fk_orphans` | `fact_transaction.account_id -> dim_account`, `branch_id -> dim_branch`; `dim_account.customer_id -> dim_customer` must match legacy (not enforced there) |
| `column_checksum` | per column `SUM(crc32(canonical value))` + NULL counts |
| `row_checksum` | whole-row checksum |
| `aggregate` | `SUM(amount)` total / by `transaction_type` / by branch, count by type, net flow by account, min/max `transaction_date`, min/max `date_opened`, balances, ages, ... |
| `row_diff` | full outer join on PK; every missing row and every differing column is written as a `mismatch` record (`pk_value`, `column_name`, `legacy_value`, `target_value`) |
| `proc_parity` | `sp_DailyTransaction` and `sp_BalancePerCustomer` outputs for every parameter set in `specs.py` vs Delta replicas |
| `reject_parity` | Talend rejects == Delta rejects |

**The gate is binary: GREEN only if every check is PASS (`match_pct = 100.0`, 0 mismatch rows).**
There is no allow-list. Inspect failures with:

```sql
SELECT * FROM migration_demo.banking_mig_ops.reconciliation_results
WHERE run_id = '<run_id>' AND (status <> 'PASS' OR record_type = 'mismatch')
ORDER BY table_name, check_type, pk_value;
```

If RED: do **not** switch consumers; go to step 6, fix, re-run step 3 + 4.

## 5. Switch consumers

1. Repoint the consumer views (contract = gold snake_case columns) at the gold tables. In the dry run
   these are `consumer_<table>` views in the work schema; in production, repoint BI connections /
   views to `migration_demo.banking_mig_gold` and the proc consumers to the ticket-9 table functions.
2. Smoke test: consumer view counts == legacy counts (`switch_consumers` step details).
3. Grant read to consumers via account-level principals (`account users`), not workspace groups.
4. Keep the legacy `DWH` online read-only for the rollback window (default 2 weeks).

## 6. Rollback

Trigger: RED gate, failed smoke test, or a consumer-reported data defect within the rollback window.

1. Repoint consumer views / connections back to the legacy DWH (`point_consumers(..., "legacy")`).
2. Resume the Talend schedules (they are only paused, not removed, until step 7).
3. Re-open source writes; the Delta pipelines can keep running in shadow mode.
4. Log the incident and the failing `run_id` from `reconciliation_results`.

The dry run rehearses rollback on every GREEN run (`rollback_rehearsal`: switch to legacy, smoke,
switch back) and the test suite exercises the RED path (`test_red_gate_rolls_back_and_blocks_decommission`).

## 7. Talend decommission (after the rollback window)

Blocked automatically while the gate is RED. Checklist (`TALEND_DECOMMISSION_CHECKLIST`):

- [ ] Disable Talend schedules/triggers for the four `Load_*` jobs
- [ ] Revoke the Talend service account's write access on `DWH`
- [ ] Archive `talend_jobs/*.zip` and the `DWH_DB_Connection` / `Sample_DB_Connection` metadata
- [ ] Final `BACKUP DATABASE DWH`, retained per policy
- [ ] `ALTER DATABASE DWH SET READ_ONLY`
- [ ] Remove Talend JobServer agents

## Dry run

```bash
W=/Workspace/Shared/banking_etl_migration_v2/ticket_10
databricks workspace import-dir databricks/src $W/src --overwrite
databricks workspace import $W/notebooks/validation --file databricks/notebooks/validation.py \
  --language PYTHON --format SOURCE --overwrite
databricks jobs submit --json @submit.json   # serverless notebook task, base_parameters below
```

Notebook parameters: `catalog=migration_demo`, `schema_prefix=banking_mig_`, `work_suffix=t10`,
`ops_suffix=ops`, `fixtures_path=/Volumes/migration_demo/banking_mig_t10/fixtures/legacy_dwh`,
`src_path=$W/src`, `mode=dry_run_cutover`. The notebook fails the job if the gate is RED.
`mode=reconcile_only` re-runs only the gate against tables already in the work schema. For the real
cutover, point `_gold` in `cutover.py` at `banking_mig_gold` once the pipeline tickets land (the dry run
uses the ticket-scoped reference transform because those tickets run in parallel).

Locally: `cd databricks && pytest tests/validation` (local PySpark + delta-spark, JDK 17).
