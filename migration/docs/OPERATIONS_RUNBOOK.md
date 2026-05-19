# Operations Runbook — ADF DWH Pipeline

## Overview

This runbook covers day-to-day operations for the Azure Data Factory pipelines that load the banking Data Warehouse (DWH). These pipelines replace the legacy Talend Open Studio ETL jobs and SSIS packages.

## Pipeline Architecture

```
TR_Daily_DWH_Load (02:00 UTC)
    └── PL_Master_DWH_Load
        ├── PL_Load_DimBranch        (parallel)
        ├── PL_Load_DimAccount       (parallel)
        ├── PL_Load_DimCustomer      (parallel)
        ├── PL_Load_FactTransaction  (after DimBranch + DimAccount)
        └── PL_Execute_StoredProcedures (after all loads)
            ├── sp_DailyTransaction
            └── sp_BalancePerCustomer (conditional)

TR_Hourly_StoredProcedures (08:00-18:00 UTC)
    └── PL_Execute_StoredProcedures

TR_FileArrival_CSV (event-driven)
    └── PL_Load_FactTransaction
```

## Daily Operations

### Morning Checklist (by 09:00 UTC)

1. **Verify nightly load completed** — Check ADF Monitor for `PL_Master_DWH_Load` run status
2. **Review pipeline duration** — Compare against baseline (< 1 hour expected)
3. **Check alert inbox** — Review any pipeline failure or long-running alerts
4. **Spot-check data** — Run `03_sample_data_comparison.sql` against DWH

### Monitoring Dashboard

Access the ADF monitoring dashboard at:
```
Azure Portal → Data Factory → {factory-name} → Monitor → Pipeline runs
```

Key filters:
- Pipeline name: `PL_Master_DWH_Load`
- Time range: Last 24 hours
- Status: All

### Key Metrics

| Metric | Expected Range | Alert Threshold |
|--------|---------------|-----------------|
| PL_Master_DWH_Load duration | 20-45 minutes | > 120 minutes |
| DimAccount row count | ~100-500 rows | 0 rows |
| DimBranch row count | ~5-20 rows | 0 rows |
| DimCustomer row count | ~100-500 rows | 0 rows |
| FactTransaction row count | ~500-5000 rows | 0 rows |
| Integration Runtime nodes | >= 1 | < 1 |

## Troubleshooting Guide

### Pipeline Failure: PL_Load_DimAccount / PL_Load_DimBranch

**Symptoms:** Copy activity fails with connection or permission errors.

**Common causes:**
1. SQL Server connection timeout — Self-hosted IR may be down
2. Source table schema change — Column added/removed in Sample_DB
3. Target table locked — Another process holding a lock on DWH tables

**Resolution steps:**
1. Check Integration Runtime status in ADF Monitor → Manage → Integration Runtimes
2. Verify SQL Server connectivity: `Test-NetConnection {sql-server} -Port 1433`
3. Check for blocking sessions: `SELECT * FROM sys.dm_exec_requests WHERE blocking_session_id <> 0`
4. Re-run the failed pipeline manually from ADF Monitor

### Pipeline Failure: PL_Load_DimCustomer

**Symptoms:** Data Flow activity fails during transformation.

**Common causes:**
1. Source table schema mismatch — City or State table structure changed
2. Data type conversion error — Unexpected data in customer fields
3. Cluster startup failure — Spark cluster for data flow couldn't provision

**Resolution steps:**
1. Check data flow debug output in ADF Monitor → Activity runs → DF_Transform_DimCustomer
2. Verify source tables exist: `SELECT TOP 1 * FROM Sample_DB.dbo.Customer`, `.City`, `.State`
3. Check data flow cluster logs for memory or timeout issues
4. If cluster issue, retry — transient compute provisioning failures are common

### Pipeline Failure: PL_Load_FactTransaction

**Symptoms:** Union or deduplication step fails, or referential integrity check fires.

**Common causes:**
1. CSV file format change — Column order or delimiter changed
2. Excel file corruption — `.xlsx` file unreadable
3. Orphan records — Transaction references non-existent Account or Branch
4. Duplicate TransactionIDs across sources

**Resolution steps:**
1. Check CSV file in blob storage — Verify header row matches expected schema
2. For Excel issues, re-upload a clean copy to blob storage
3. For orphan records, ensure dimension pipelines ran successfully before fact load
4. Review deduplication logic in `DF_Transform_FactTransaction` aggregate step

### Pipeline Failure: PL_Execute_StoredProcedures

**Symptoms:** Stored procedure activity fails or returns unexpected results.

**Common causes:**
1. Stored procedure doesn't exist — Not deployed to DWH database
2. Parameter type mismatch — Date format not matching expected input
3. Timeout — Large dataset causing procedure to exceed timeout

**Resolution steps:**
1. Verify procedures exist: `SELECT name FROM DWH.sys.procedures`
2. Test manually: `EXEC sp_DailyTransaction @start_date='2024-01-18', @end_date='2024-01-20'`
3. For timeouts, increase the activity timeout in the pipeline definition

### Integration Runtime Offline

**Symptoms:** All pipelines fail, IR node count = 0 alert fires.

**Resolution steps:**
1. RDP into the IR host machine
2. Check Windows Services for `Microsoft Integration Runtime` service status
3. Restart the service: `Restart-Service -Name "DIAHostService"`
4. Verify in ADF portal: Manage → Integration Runtimes → Status = Running
5. If persistent, reinstall IR from ADF portal download link

## Manual Pipeline Execution

### Run Full DWH Load

```
ADF Portal → Author → Pipelines → Orchestration → PL_Master_DWH_Load → Add Trigger → Trigger Now
```

### Run Individual Dimension Load

```
ADF Portal → Author → Pipelines → DimensionLoads → PL_Load_{DimName} → Add Trigger → Trigger Now
```

### Run Stored Procedures with Parameters

```
ADF Portal → Author → Pipelines → Reporting → PL_Execute_StoredProcedures → Add Trigger → Trigger Now
Parameters:
  StartDate: "2024-01-18"
  EndDate: "2024-01-20"
  CustomerName: "John"
```

## Escalation Path

| Severity | Response Time | Escalation |
|----------|--------------|------------|
| Pipeline failure (single run) | 1 hour | Data Engineering Team |
| Pipeline failure (consecutive) | 30 minutes | Data Engineering Lead |
| IR offline | 15 minutes | Infrastructure On-Call |
| Data quality issue (validation failure) | 2 hours | Data Engineering + Business Analyst |

## Maintenance Windows

- **Self-hosted IR patching:** Sundays 06:00-08:00 UTC (pipelines will queue)
- **SQL Server maintenance:** First Saturday of month, 04:00-06:00 UTC
- **ADF service updates:** Managed by Microsoft, no downtime expected

## Contact Information

| Role | Contact | Responsibilities |
|------|---------|-----------------|
| Data Engineering Lead | {lead-email} | Pipeline design, troubleshooting escalation |
| Data Engineering Team | {team-dl} | Day-to-day monitoring, incident response |
| Infrastructure On-Call | {infra-oncall} | IR host, SQL Server, network issues |
| Business Analyst | {ba-email} | Data quality validation, report verification |
