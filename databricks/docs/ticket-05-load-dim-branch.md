# Ticket 5: `Load_DimBranch` -> silver/gold

Talend job: `talend_jobs/Load_DimBranch.zip` (`process/Load_DimBranch_0.1.item`).
Code: `src/banking_etl/dims/branch.py`; notebooks `notebooks/silver/silver_branch.py` and
`notebooks/gold/load_dim_branch.py`; bundle job `load_dim_branch` in `resources/ticket-05-load-dim-branch.yml`.

```
bronze.sqlserver_branch --(to_silver_branch, overwrite)--> silver.branch --(to_dim_branch + merge_scd1 on BranchID)--> gold.dim_branch
```

## Talend -> Databricks mapping

| Talend component | Setting | Databricks |
|---|---|---|
| `tDBInput_1` (tMSSqlInput) | `SELECT branch_id, branch_name, branch_location FROM dbo.branch`, `TRIM_ALL_COLUMN=false` | `bronze.sqlserver_branch` (ticket 2), then `silver.branch`, which keeps values as-is (no trim) |
| `tMap_1` -> `to_DimBranch` | `BranchID = row1.branch_id`, `BranchName = row1.branch_name`, `BranchLocation = row1.branch_location`; no Var, lookups, filters, trims or case changes | `to_dim_branch`: rename only (`TMAP_TO_DIM_BRANCH`) |
| `tMap_1` | `DIE_ON_ERROR=true` | n/a, because the expressions cannot fail |
| `tDBOutput_1` (tMSSqlOutput) | `TABLE=context.namaTabel` (`"DimBranch"`), `TABLE_ACTION=CREATE_IF_NOT_EXISTS` | `ensure_dim_branch`: runs ticket 4's `apply_star_schema` if `gold.dim_branch` is missing |
| `tDBOutput_1` | `DATA_ACTION=INSERT`, `DIE_ON_ERROR=false`, `IDENTITY_INSERT=false` | `merge_scd1(..., key_cols=["BranchID"], update_cols=["BranchName","BranchLocation"])` (README SCD-1 convention) |

### silver.branch
`branch_id INT, branch_name STRING, branch_location STRING, _ingested_at TIMESTAMP, _source STRING`.
This is a full rebuild from the current bronze snapshot (overwrite), mirroring bronze. Bronze audit
columns are kept for lineage.

### Load semantics vs Talend
- **Initial load**: Talend INSERTs 5 rows. The merge inserts 5 rows, and Delta generates `BranchKey`.
- **Rerun with unchanged source**: on SQL Server, Talend's INSERT hits `PRIMARY KEY` violations
  (reproduced: `Msg 2627 ... duplicate key value is (1)`), and with `DIE_ON_ERROR=false` the rows go to
  the reject flow, so the table is unchanged. The merge is a no-op (0 inserted / 0 updated), with the same end state.
- **Changed source attribute**: Talend would reject the row and keep the stale value in DWH. The port
  updates it in place (SCD-1, README convention) and keeps `BranchKey` stable. This is an intentional
  improvement over the legacy behavior.
- **Branch removed from source**: neither Talend nor the merge deletes it.
- **Duplicate / NULL `branch_id`**: impossible in the source (`branch_id` is the PK and `NOT NULL`).
  If it ever happens, `merge_scd1` raises instead of silently rejecting rows.

## Parity evidence
`fixtures/parity/dim_branch.csv` holds `DWH.dbo.DimBranch` exported from SQL Server 2022 after restoring
`data_sources/sample.bak`, running `sql_scripts/01_create_tables.sql` and running the Talend-equivalent
`INSERT INTO DWH.dbo.DimBranch SELECT dbo.branch.branch_id, dbo.branch.branch_name, dbo.branch.branch_location FROM dbo.branch`.
No values had leading or trailing whitespace (`DATALENGTH = LEN`), and none exceeded the DWH column
lengths (`VARCHAR(100)`/`VARCHAR(255)`, source `VARCHAR(50)`). `tests/dims/test_dim_branch.py::test_parity_with_talend_dwh_dimbranch`
asserts that `gold.dim_branch` (`BranchID, BranchName, BranchLocation`) equals that export exactly.
