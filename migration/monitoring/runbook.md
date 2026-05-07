# Operational Runbook — ADF ETL Pipelines

## Overview

This runbook provides operational procedures for the Azure Data Factory (ADF) pipelines that replaced the legacy Talend ETL jobs for the Banking Data Warehouse.

---

## Pipeline Inventory

| Pipeline                        | Type          | Trigger         | Avg Duration | Description                                    |
|---------------------------------|---------------|-----------------|--------------|------------------------------------------------|
| `pipeline_Master_ETL`           | Orchestration | Daily 02:00 UTC | ~15 min      | Master pipeline executing all ETL jobs in order |
| `pipeline_Load_DimBranch`       | Copy Activity | Via Master      | ~1 min       | Branch dimension full load                     |
| `pipeline_Load_DimAccount`      | Copy Activity | Via Master      | ~2 min       | Account dimension full load                    |
| `pipeline_Load_DimCustomer`     | Data Flow     | Via Master      | ~5 min       | Customer dimension transform + load            |
| `pipeline_Load_FactTransaction` | Data Flow     | Via Master      | ~7 min       | Transaction fact union + dedup + load          |

---

## Execution Order & Dependencies

```
pipeline_Master_ETL
├── 1. pipeline_Load_DimBranch       (no dependencies)
├── 2. pipeline_Load_DimAccount      (depends on: DimBranch success)
├── 3. pipeline_Load_DimCustomer     (depends on: DimAccount success)
└── 4. pipeline_Load_FactTransaction (depends on: DimCustomer success)
```

Dimensions must load before the fact table due to foreign key constraints in `FactTransaction`.

---

## Daily Operations

### Normal Execution (Automated)

1. The `trigger_Daily_ETL` fires at **02:00 UTC** daily.
2. It invokes `pipeline_Master_ETL` with `Environment=production`.
3. Each sub-pipeline executes sequentially.
4. On success, all dimension and fact tables are refreshed.

### Manual Execution (Ad-hoc)

1. Navigate to **Azure Data Factory Studio** > **Author** > **Pipelines** > `pipeline_Master_ETL`.
2. Click **Add Trigger** > **Trigger Now**.
3. Set parameters:
   - `Environment`: `production` (or `staging` for testing)
   - `AlertWebhookUrl`: Your team's alert endpoint
4. Click **OK** to start.

Alternatively, use Azure CLI:
```bash
az datafactory pipeline create-run \
  --factory-name <adf-name> \
  --resource-group <rg-name> \
  --name pipeline_Master_ETL \
  --parameters '{"Environment":"production","AlertWebhookUrl":"https://your-endpoint.com/alerts"}'
```

---

## Monitoring

### ADF Monitor

1. Open **Azure Data Factory Studio** > **Monitor** > **Pipeline runs**.
2. Filter by pipeline name or time range.
3. Click a run to view activity-level details, durations, and row counts.

### Azure Monitor Dashboard

Deploy `monitoring_dashboard.json` to view:
- Pipeline success/failure rates
- Activity run trends
- Trigger status
- Pipeline elapsed time
- Integration Runtime utilization

### Alert Rules

Deploy `alert_rules.json` for automated alerting:

| Alert                          | Severity | Condition                        | Action              |
|-------------------------------|----------|----------------------------------|---------------------|
| Pipeline Failure              | 1 (Crit) | Any pipeline run fails           | Notify Action Group |
| Activity Failure              | 2 (High) | Any activity run fails           | Notify Action Group |
| Long Running Pipeline         | 3 (Warn) | Duration > 1 hour                | Notify Action Group |
| Trigger Failure               | 2 (High) | Any trigger fails to fire        | Notify Action Group |

---

## Incident Response

### Pipeline Failure

1. **Identify**: Check ADF Monitor for the failed run. Note the pipeline name, activity name, and error message.
2. **Diagnose**:
   - Copy Activity failure: Check source/sink connectivity, linked service credentials, SQL query syntax.
   - Data Flow failure: Check Data Flow debug output, source schema changes, transformation errors.
   - Timeout: Check source database performance, network latency, Integration Runtime sizing.
3. **Resolve**:
   - Fix the root cause (credentials, query, schema).
   - Re-run the failed pipeline manually.
4. **Verify**: Run validation scripts (`validate_row_counts.sql`, `validate_checksums.sql`).
5. **Document**: Log the incident and resolution.

### Common Failure Scenarios

#### Source Database Connection Failure
- **Symptom**: Copy Activity fails with "Cannot connect to SQL Server"
- **Check**: Self-hosted Integration Runtime status, VPN/firewall rules, SQL Server availability
- **Fix**: Restart IR, verify network connectivity, check SQL Server service status

#### Data Flow Out of Memory
- **Symptom**: Data Flow activity fails with memory-related error
- **Check**: Data volume, compute size in Data Flow settings
- **Fix**: Increase `coreCount` in the pipeline's Data Flow compute settings (8 -> 16 -> 32)

#### Schema Drift
- **Symptom**: Copy/Data Flow fails with column mapping errors
- **Check**: Source table schema for changes (added/removed/renamed columns)
- **Fix**: Update column mappings in the pipeline/data flow definition, redeploy

#### Duplicate Key Violation
- **Symptom**: Sink insert fails with primary key constraint violation
- **Check**: Pre-copy TRUNCATE script, deduplication in Data Flow
- **Fix**: Ensure pre-copy script runs (DimBranch/DimAccount) or Aggregate dedup works (FactTransaction)

#### Trigger Not Firing
- **Symptom**: Daily ETL does not run at scheduled time
- **Check**: Trigger status (Started vs Stopped), trigger definition time zone
- **Fix**: Start the trigger in ADF Studio: **Manage** > **Triggers** > `trigger_Daily_ETL` > **Start**

---

## Maintenance Procedures

### Weekly
- Review ADF Monitor for any warnings or performance degradation
- Check Integration Runtime health and version
- Verify alert rules are active

### Monthly
- Run full validation suite (row counts + checksums + business rules)
- Review pipeline performance trends; adjust compute sizing if needed
- Rotate linked service credentials if required by security policy
- Review and archive old pipeline run logs (> 45 days)

### Quarterly
- Test disaster recovery: restore from backup and re-run full ETL
- Review and update alert thresholds based on actual pipeline durations
- Validate stored procedure outputs against business expectations
- Review Azure cost analysis for Data Factory consumption

---

## Contacts

| Role                  | Name          | Email                        | Escalation |
|-----------------------|---------------|------------------------------|------------|
| Data Engineer (Primary)| TBD          | TBD                          | L1         |
| Data Engineer (Backup) | TBD          | TBD                          | L1         |
| DBA                   | TBD           | TBD                          | L2         |
| Azure Platform Admin  | TBD           | TBD                          | L2         |
| Business Owner        | TBD           | TBD                          | L3         |

---

## Change Log

| Date       | Author | Change Description                                |
|------------|--------|---------------------------------------------------|
| 2024-XX-XX | TBD    | Initial creation — migrated from Talend ETL jobs  |
