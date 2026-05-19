# Validation Runbook — SSIS/Talend to ADF Migration

## Overview

This runbook documents the process for verifying output parity between the legacy Talend/SSIS ETL pipelines and the new Azure Data Factory (ADF) implementation.

## Prerequisites

- Access to SQL Server hosting both `Sample_DB` (source) and `DWH` (target)
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Completed execution of the ADF `PL_Master_DWH_Load` pipeline

## Validation Sequence

Run the validation scripts in order after each ADF pipeline execution:

| Step | Script | Purpose | Pass Criteria |
|------|--------|---------|---------------|
| 1 | `01_row_count_validation.sql` | Row count parity | All Delta = 0, Status = PASS |
| 2 | `02_checksum_validation.sql` | Data integrity hashes | All Status = MATCH/PASS |
| 3 | `03_sample_data_comparison.sql` | Field-level spot checks | All Status = MATCH |
| 4 | `04_referential_integrity.sql` | FK and NULL checks | All OrphanCount = 0, Status = PASS |

## Detailed Validation Steps

### Step 1: Row Count Validation

**What it checks:** Total row counts in each DWH table match their source counterparts.

**Tables validated:**
- `DimAccount` vs `Sample_DB.dbo.Account`
- `DimBranch` vs `Sample_DB.dbo.Branch`
- `DimCustomer` vs `Sample_DB.dbo.Customer`
- `FactTransaction` — deduplication check (no duplicate TransactionIDs)

**Troubleshooting failures:**
- Non-zero Delta on dimensions → Check ADF Copy activity logs for errors
- Duplicates in FactTransaction → Review `DF_Transform_FactTransaction` aggregate step
- Zero rows → Verify source connectivity and linked service credentials

### Step 2: Checksum Validation

**What it checks:** Aggregate checksums and row-level SHA-256 hashes to detect data corruption.

**Key considerations:**
- DimCustomer uses `UPPER()` transformation — the script applies `UPPER()` to source data for fair comparison
- CHECKSUM_AGG is order-independent, making it suitable for comparing unordered datasets
- HASHBYTES provides stronger collision resistance than CHECKSUM for row-level comparison

**Troubleshooting failures:**
- DimCustomer MISMATCH → Verify the `CleanseAndTransform` step in `DF_Transform_DimCustomer` applies UPPER() correctly
- FactTransaction MISMATCH → Check data type conversions in `NormalizeCSVSchema` and `NormalizeExcelSchema` steps

### Step 3: Sample Data Comparison

**What it checks:** Individual field values for a representative sample of records.

**Stored procedure verification:**
- `sp_DailyTransaction` — Confirms daily aggregation logic produces correct totals
- `sp_BalancePerCustomer` — Validates CASE WHEN logic for Deposit/Withdrawal balance calculation

### Step 4: Referential Integrity

**What it checks:**
- Foreign key relationships (FactTransaction → DimAccount, FactTransaction → DimBranch)
- Logical relationships (DimAccount → DimCustomer)
- NULL values in required columns
- Duplicate primary keys

**Troubleshooting failures:**
- Orphan records → Dimension tables must be loaded before FactTransaction (enforced by PL_Master_DWH_Load dependency chain)
- NULL values → Check source data quality and ADF data flow transformations

## Parallel Run Validation

During the parallel-run phase (legacy and ADF running side-by-side):

1. Run the legacy Talend jobs against `DWH_Legacy` database
2. Run the ADF pipeline against `DWH` database
3. Execute cross-database comparison:

```sql
-- Cross-database row count comparison
SELECT
    'DimAccount' AS TableName,
    (SELECT COUNT(*) FROM DWH.dbo.DimAccount) AS ADF_Count,
    (SELECT COUNT(*) FROM DWH_Legacy.dbo.DimAccount) AS Legacy_Count
UNION ALL
SELECT 'DimBranch',
    (SELECT COUNT(*) FROM DWH.dbo.DimBranch),
    (SELECT COUNT(*) FROM DWH_Legacy.dbo.DimBranch)
UNION ALL
SELECT 'DimCustomer',
    (SELECT COUNT(*) FROM DWH.dbo.DimCustomer),
    (SELECT COUNT(*) FROM DWH_Legacy.dbo.DimCustomer)
UNION ALL
SELECT 'FactTransaction',
    (SELECT COUNT(*) FROM DWH.dbo.FactTransaction),
    (SELECT COUNT(*) FROM DWH_Legacy.dbo.FactTransaction);
```

## Sign-Off Checklist

- [ ] All four validation scripts pass with no failures
- [ ] Parallel run comparison shows matching counts for 5 consecutive days
- [ ] Stored procedure outputs match between legacy and ADF-loaded databases
- [ ] Data engineering team has reviewed and approved results
- [ ] Business stakeholders have verified report outputs
