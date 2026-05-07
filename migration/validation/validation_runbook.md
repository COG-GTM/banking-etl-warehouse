# Validation Runbook — ADF Pipeline Migration

## Overview

This runbook provides step-by-step procedures to validate the migrated ADF pipelines against the legacy Talend ETL jobs. Execute these validations after each ADF pipeline run to ensure data completeness, integrity, and correctness.

---

## Prerequisites

- Access to both `sample` (source) and `DWH` (target) databases in SQL Server Management Studio (SSMS) or Azure Data Studio
- ADF pipelines have completed at least one successful run
- Stored procedures (`sp_DailyTransaction`, `sp_BalancePerCustomer`) are deployed to the DWH database

---

## Validation Steps

### Step 1: Row Count Validation

**Script:** `validate_row_counts.sql`

1. Open SSMS and connect to the DWH database server.
2. Open and execute `validate_row_counts.sql`.
3. Review the output:
   - All tables should show `PASS` status.
   - For `FactTransaction`, the deduplicated source count should match the target count (mirrors `tUniqRow` behavior from the Talend job).

**Expected Results:**

| Table           | Validation                                 |
|-----------------|--------------------------------------------|
| DimBranch       | Source `branch` count = Target count       |
| DimAccount      | Source `account` count = Target count      |
| DimCustomer     | Source `customer` count = Target count     |
| FactTransaction | Deduplicated union count = Target count    |

**If FAIL:** Investigate ADF pipeline run logs for errors. Check if source data changed between validation runs.

---

### Step 2: Checksum Validation

**Script:** `validate_checksums.sql`

1. Execute `validate_checksums.sql` against the DWH database.
2. Compare source and target checksums for each table.
3. For `DimCustomer`, checksums are computed on the **transformed** values (UPPER on text fields) to match the ADF Data Flow output.

**Expected Results:** All checksums should match (status = `PASS`).

**If FAIL:**
- DimBranch/DimAccount: Check for data type conversion issues in the Copy Activity column mapping.
- DimCustomer: Verify the UPPER() / LOWER() transformations in the Data Flow are applied consistently.
- FactTransaction: Check for deduplication differences between the ADF Aggregate transformation and the original Talend `tUniqRow`.

---

### Step 3: Business Rules Validation

**Script:** `validate_business_rules.sql`

1. Execute `validate_business_rules.sql` against the DWH database.
2. Validate the following:

| Check                            | Description                                                        |
|----------------------------------|--------------------------------------------------------------------|
| FK Integrity (Account)           | No orphan `AccountID` in FactTransaction                           |
| FK Integrity (Branch)            | No orphan `BranchID` in FactTransaction                            |
| PK Uniqueness (Customer)         | No duplicate `CustomerID` in DimCustomer                           |
| PK Uniqueness (Transaction)      | No duplicate `TransactionID` in FactTransaction                    |
| UPPER() Compliance               | All text fields in DimCustomer are uppercase                       |
| sp_DailyTransaction              | SP output row count matches manual aggregation                     |
| sp_BalancePerCustomer            | Calculated balance matches SP output                               |
| NULL Checks                      | No NULL values in required/PK fields                               |
| Transaction Amounts              | No negative amounts (warning only)                                 |

---

### Step 4: Parallel Run Comparison (Legacy vs. ADF)

During the transition period, run both the legacy Talend jobs and the new ADF pipelines against the same source data snapshot:

1. **Take a source snapshot:** Back up the `sample` database.
2. **Run legacy Talend jobs** against the snapshot and record DWH state.
3. **Truncate DWH tables** and run ADF pipelines against the same snapshot.
4. **Compare results** using the row count and checksum scripts above.
5. **Document differences** in the migration validation report.

---

### Step 5: Performance Comparison

| Metric                    | Talend (Legacy)   | ADF (New)         | Notes                        |
|---------------------------|-------------------|-------------------|------------------------------|
| DimBranch load time       | _____ sec         | _____ sec         |                              |
| DimAccount load time      | _____ sec         | _____ sec         |                              |
| DimCustomer load time     | _____ sec         | _____ sec         | Includes Data Flow execution |
| FactTransaction load time | _____ sec         | _____ sec         | Includes Data Flow execution |
| Total ETL duration        | _____ min         | _____ min         |                              |

Record metrics from ADF Monitor and Talend execution logs.

---

## Validation Sign-Off

| Validation Step          | Date       | Tester     | Result   | Notes |
|--------------------------|------------|------------|----------|-------|
| Row Count                |            |            |          |       |
| Checksum                 |            |            |          |       |
| Business Rules           |            |            |          |       |
| Parallel Run             |            |            |          |       |
| Performance Comparison   |            |            |          |       |

**Approval:**
- Data Engineer: _________________ Date: _________
- QA Lead: _________________ Date: _________
- Business Owner: _________________ Date: _________

---

## Troubleshooting

### Common Issues

| Issue                           | Likely Cause                                     | Resolution                                                     |
|---------------------------------|--------------------------------------------------|----------------------------------------------------------------|
| Row count mismatch (DimBranch)  | Source data changed between runs                 | Re-run with consistent source snapshot                         |
| Checksum mismatch (DimCustomer) | UPPER/LOWER transformation inconsistency         | Review Data Flow derived column expressions                    |
| Duplicate TransactionIDs        | Aggregate dedup not matching tUniqRow behavior   | Review Aggregate groupBy vs. ROW_NUMBER approach               |
| FK orphan records               | Dimension load order incorrect                   | Verify Master ETL pipeline dependency chain                    |
| SP output mismatch              | Data type precision differences (MONEY vs DECIMAL)| Align data types in ADF column mapping                        |
| NULL violations                 | Source data has NULLs not handled by transform   | Add ISNULL/COALESCE in Data Flow derived columns              |
