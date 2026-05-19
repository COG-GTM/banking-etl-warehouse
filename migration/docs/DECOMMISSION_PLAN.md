# Decommission Plan — Legacy SSIS/Talend ETL Jobs

## Overview

This document outlines the phased decommission plan for legacy Talend Open Studio ETL jobs and any SSIS packages, following successful migration to Azure Data Factory. The plan ensures zero data loss and provides rollback procedures at each phase.

## Decommission Timeline

```
Phase 1: Parallel Run (Weeks 1-4)
    │   Both legacy and ADF pipelines run simultaneously
    │
Phase 2: ADF Primary (Weeks 5-6)
    │   ADF is primary; legacy runs as backup (read-only)
    │
Phase 3: Legacy Standby (Weeks 7-8)
    │   Legacy disabled but preserved; ADF is sole source
    │
Phase 4: Full Decommission (Week 9+)
        Legacy artifacts archived and infrastructure retired
```

## Phase 1: Parallel Run (Weeks 1-4)

### Objective
Run both legacy Talend/SSIS and new ADF pipelines side-by-side to validate output parity.

### Setup
1. Create a parallel target database: `DWH_ADF` (separate from legacy `DWH`)
2. Configure ADF pipelines to target `DWH_ADF`
3. Keep legacy Talend jobs targeting original `DWH`

### Daily Validation
Execute daily cross-database comparisons:

```sql
-- Run after both legacy and ADF pipelines complete
SELECT
    'DimAccount' AS TableName,
    (SELECT COUNT(*) FROM DWH.dbo.DimAccount) AS Legacy_Count,
    (SELECT COUNT(*) FROM DWH_ADF.dbo.DimAccount) AS ADF_Count,
    CASE WHEN (SELECT COUNT(*) FROM DWH.dbo.DimAccount) =
              (SELECT COUNT(*) FROM DWH_ADF.dbo.DimAccount)
         THEN 'MATCH' ELSE 'MISMATCH' END AS Status
UNION ALL
SELECT 'DimBranch',
    (SELECT COUNT(*) FROM DWH.dbo.DimBranch),
    (SELECT COUNT(*) FROM DWH_ADF.dbo.DimBranch),
    CASE WHEN (SELECT COUNT(*) FROM DWH.dbo.DimBranch) =
              (SELECT COUNT(*) FROM DWH_ADF.dbo.DimBranch)
         THEN 'MATCH' ELSE 'MISMATCH' END
UNION ALL
SELECT 'DimCustomer',
    (SELECT COUNT(*) FROM DWH.dbo.DimCustomer),
    (SELECT COUNT(*) FROM DWH_ADF.dbo.DimCustomer),
    CASE WHEN (SELECT COUNT(*) FROM DWH.dbo.DimCustomer) =
              (SELECT COUNT(*) FROM DWH_ADF.dbo.DimCustomer)
         THEN 'MATCH' ELSE 'MISMATCH' END
UNION ALL
SELECT 'FactTransaction',
    (SELECT COUNT(*) FROM DWH.dbo.FactTransaction),
    (SELECT COUNT(*) FROM DWH_ADF.dbo.FactTransaction),
    CASE WHEN (SELECT COUNT(*) FROM DWH.dbo.FactTransaction) =
              (SELECT COUNT(*) FROM DWH_ADF.dbo.FactTransaction)
         THEN 'MATCH' ELSE 'MISMATCH' END;
```

### Exit Criteria
- [ ] 5 consecutive days of matching row counts across all tables
- [ ] Checksum validation passes for all tables
- [ ] Stored procedure outputs match between `DWH` and `DWH_ADF`
- [ ] No ADF pipeline failures requiring manual intervention
- [ ] Business stakeholders sign off on report quality

## Phase 2: ADF Primary (Weeks 5-6)

### Objective
Switch ADF to primary production pipeline. Legacy runs continue as read-only backup.

### Cutover Steps
1. Stop legacy Talend scheduled jobs
2. Reconfigure ADF pipelines to target production `DWH` database
3. Enable the `TR_Daily_DWH_Load` trigger
4. Enable the `TR_Hourly_StoredProcedures` trigger
5. Update downstream consumers (reports, dashboards) to confirm no changes needed

### Legacy Backup
- Keep Talend jobs configured but **disabled** (not scheduled)
- Maintain Talend Studio installation on the ETL server
- Keep `Sample_DB` source connections active

### Monitoring
- Increased monitoring frequency during cutover week
- Daily validation script execution
- Business team confirms report accuracy each morning

### Rollback Trigger
If any of the following occur, revert to legacy:
- ADF pipeline fails 3+ consecutive runs
- Data quality validation shows > 1% row count discrepancy
- Business reports produce incorrect results
- Integration Runtime unavailable for > 2 hours during business hours

### Rollback Procedure
1. Disable ADF triggers (`TR_Daily_DWH_Load`, `TR_Hourly_StoredProcedures`)
2. Re-enable legacy Talend scheduled jobs
3. Run legacy jobs manually to reload DWH with fresh data
4. Verify DWH data via `01_row_count_validation.sql`
5. Notify stakeholders of rollback
6. Investigate ADF issues and re-plan cutover

## Phase 3: Legacy Standby (Weeks 7-8)

### Objective
Confirm ADF stability with legacy in cold standby.

### Actions
1. **Talend Studio:** Uninstall from ETL server (archive project files first)
2. **SSIS packages:** Disable all related SQL Agent jobs
3. **Source connections:** Maintain `Sample_DB` connections (still needed by ADF)
4. **Documentation:** Update all runbooks to reference ADF exclusively

### Archive Checklist
- [ ] Export Talend job definitions (ZIP from `talend_jobs/` already in repo)
- [ ] Screenshot all Talend job configurations and tMap mappings
- [ ] Export SSIS package definitions (if any .dtsx files exist)
- [ ] Document all Talend connection strings (redact credentials)
- [ ] Save Talend execution logs from the last 30 days

## Phase 4: Full Decommission (Week 9+)

### Objective
Permanently retire legacy ETL infrastructure.

### Decommission Checklist

#### Software
- [ ] Uninstall Talend Open Studio from ETL server
- [ ] Remove SSIS packages from SQL Server Integration Services catalog
- [ ] Delete legacy SQL Agent jobs related to ETL scheduling
- [ ] Remove Talend from any software inventory/license management

#### Infrastructure
- [ ] Decommission dedicated ETL server (if no longer needed)
- [ ] Remove firewall rules specific to legacy ETL connectivity
- [ ] Revoke service accounts used by Talend/SSIS
- [ ] Clean up any staging tables created by legacy ETL in `Sample_DB`

#### Documentation
- [ ] Update architecture diagrams to show ADF-only flow
- [ ] Archive this decommission plan with completion dates
- [ ] Update onboarding documentation for new team members
- [ ] Close any legacy ETL-related tickets/issues

### Post-Decommission Validation
After decommission, monitor for 30 days:
- ADF pipeline success rate should remain > 99%
- No downstream reports should reference legacy DWH tables
- No scheduled jobs should fail due to missing legacy components

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| ADF pipeline instability during parallel run | Medium | Low | Legacy continues as primary until validated |
| Data quality regression after cutover | Low | High | Daily validation scripts + business sign-off |
| IR host failure during cutover | Low | High | HA IR setup with 2+ nodes |
| Source schema changes during migration | Medium | Medium | Schema drift detection in data flows |
| Team unfamiliar with ADF troubleshooting | High | Medium | Ops runbook + training sessions |

## Approvals

| Phase | Approver | Date | Signature |
|-------|----------|------|-----------|
| Phase 1 → Phase 2 | Data Engineering Lead | ________ | ________ |
| Phase 2 → Phase 3 | Data Engineering Lead + Business Owner | ________ | ________ |
| Phase 3 → Phase 4 | IT Director + Business Owner | ________ | ________ |
