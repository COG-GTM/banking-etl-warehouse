# Recommended Migration Sequence to Azure Data Factory

## Overview

This document provides the recommended migration order for moving all ETL components from the current Talend-based architecture to **Azure Data Factory (ADF)** and **Azure SQL Database**. The sequence is prioritized based on:

1. **Dependency order** — upstream components must migrate before downstream consumers
2. **Business criticality** — foundational data infrastructure moves first
3. **Risk minimization** — simpler components migrate first to build team confidence
4. **Parallel opportunities** — independent components that can migrate simultaneously

---

## Migration Phases

### Phase 0: Azure Foundation (Prerequisites)

**Estimated Effort:** M | **Duration:** 1–2 days

Before migrating any ETL components, the Azure target environment must be provisioned.

| Step | Task | Details |
|---|---|---|
| 0.1 | Provision Azure SQL Database | Create Azure SQL DB instance to host the `DWH` database. Choose appropriate service tier (General Purpose recommended for initial migration). |
| 0.2 | Deploy Star Schema DDL | Execute `01_create_tables.sql` against Azure SQL DB. Minor modifications needed: remove `USE`/`GO` statements if deploying via dacpac, or keep if using SSMS against Azure SQL. |
| 0.3 | Deploy Stored Procedures | Execute `02_create_procedures.sql` against Azure SQL DB. Both procedures (`sp_DailyTransaction`, `sp_BalancePerCustomer`) are fully Azure SQL-compatible. |
| 0.4 | Provision Azure Storage | Create Azure Blob Storage container or ADLS Gen2 filesystem for file-based sources (CSV, Excel). |
| 0.5 | Upload File Sources | Upload `transaction_csv.csv` and `transaction_excel.xlsx` to Azure Storage. |
| 0.6 | Create ADF Instance | Provision Azure Data Factory. Configure Managed Identity for Azure SQL and Storage access. |
| 0.7 | Create ADF Linked Services | Replace Talend DB connections with ADF Linked Services: (a) Azure SQL Database for DWH, (b) Azure Blob/ADLS for file sources, (c) Source SQL Server connection (if source DB is also migrated, or use Self-Hosted IR for on-prem). |
| 0.8 | Set Up Key Vault | Store connection strings and credentials in Azure Key Vault. Configure ADF to retrieve secrets via Managed Identity. |

**Rationale:** All downstream pipeline work depends on having the Azure infrastructure in place. Authentication must shift from Windows Integrated Security to Azure AD or SQL Authentication.

---

### Phase 1: Simple Dimension Loads (Low Risk)

**Estimated Effort:** S | **Duration:** 0.5–1 day | **Can run in parallel**

Migrate the two simplest Talend jobs first to validate the end-to-end ADF pipeline pattern.

| Priority | Component | ADF Implementation | Rationale |
|---|---|---|---|
| **1.1** | `Load_DimBranch` | **ADF Copy Activity** — single SQL-to-SQL copy with no transformation. Source dataset: SQL query on `dbo.branch`. Sink dataset: Azure SQL `DimBranch`. | Simplest job (3 columns, no transforms). Validates the basic Copy Activity pattern. |
| **1.2** | `Load_DimAccount` | **ADF Copy Activity** — single SQL-to-SQL copy with no transformation. Source dataset: SQL query on `dbo.account`. Sink dataset: Azure SQL `DimAccount`. | Simple job (6 columns, no transforms). Can be built in parallel with DimBranch. |

**ADF Pipeline Design:**
```
Pipeline: PL_Load_Dimensions_Simple
├── Copy Activity: Load_DimBranch (source: SQL → sink: Azure SQL)
└── Copy Activity: Load_DimAccount (source: SQL → sink: Azure SQL)
```

**Validation Criteria:**
- Row counts match between source and target
- All column values transfer correctly (spot-check 10% sample)
- Pipeline completes without errors

---

### Phase 2: Complex Dimension Load (Medium Risk)

**Estimated Effort:** M | **Duration:** 1–2 days

| Priority | Component | ADF Implementation | Rationale |
|---|---|---|---|
| **2.1** | `Load_DimCustomer` | **ADF Mapping Data Flow** — three source transformations reading `customer`, `city`, and `state` tables. Join transformation (customer → city on city_id, city → state on state_id). Derived column transformation for `UPPER()` on text fields. Sink to Azure SQL `DimCustomer`. | Requires multi-table JOIN and data transformation. More complex than Phase 1 but still well within ADF Data Flow capabilities. |

**Alternative Approach:** Create a SQL View in the source database that performs the JOIN and UPPER() logic, then use a simple Copy Activity to load from the view. This avoids Data Flow costs and is simpler to maintain.

**Recommended SQL View (if using alternative approach):**
```sql
CREATE VIEW vw_DimCustomer AS
SELECT
    c.customer_id AS CustomerID,
    UPPER(c.customer_name) AS CustomerName,
    UPPER(c.address) AS Address,
    UPPER(ci.city_name) AS CityName,
    UPPER(s.state_name) AS StateName,
    c.age AS Age,
    UPPER(c.gender) AS Gender,
    c.email AS Email
FROM dbo.customer c
JOIN dbo.city ci ON c.city_id = ci.city_id
JOIN dbo.state s ON ci.state_id = s.state_id;
```

**ADF Pipeline Design:**
```
Pipeline: PL_Load_DimCustomer
└── Data Flow: DF_Load_DimCustomer
    ├── Source: customer (SQL)
    ├── Source: city (SQL)
    ├── Source: state (SQL)
    ├── Join: customer ↔ city (city_id)
    ├── Join: result ↔ state (state_id)
    ├── Derived Column: UPPER() on text fields
    └── Sink: DimCustomer (Azure SQL)
```

**Validation Criteria:**
- Row counts match
- JOIN integrity: no orphaned customers (all city_id/state_id resolve)
- UPPER() transformation applied correctly on all text fields
- Compare output row-by-row with Talend job output

---

### Phase 3: Fact Table Load (Highest Complexity)

**Estimated Effort:** M–L | **Duration:** 2–3 days

| Priority | Component | ADF Implementation | Rationale |
|---|---|---|---|
| **3.1** | `Load_FactTransaction` | **ADF Mapping Data Flow** — three source transformations (SQL, Excel, CSV from Azure Storage). Union transformation to merge all streams. Aggregate/Distinct transformation for deduplication on `transaction_id`. Sink to Azure SQL `FactTransaction` with pre-copy truncation. | Most complex job: multi-source, multi-format, union, dedup. Core business data pipeline. Must run after all dimension loads due to FK constraints. |

**ADF Pipeline Design:**
```
Pipeline: PL_Load_FactTransaction
├── Validation: Check file existence in Azure Storage (CSV, Excel)
└── Data Flow: DF_Load_FactTransaction
    ├── Source: transaction_db (SQL) — DelimitedText dataset
    ├── Source: transaction_excel (Azure Blob) — Excel dataset
    ├── Source: transaction_csv (Azure Blob) — CSV dataset
    │   └── Date parsing: dd-MM-yyyy HH:mm:ss → datetime
    ├── Union: Merge all three streams
    ├── Aggregate: Deduplicate on transaction_id (keep first)
    └── Sink: FactTransaction (Azure SQL, pre-copy: TRUNCATE)
```

**Key Implementation Notes:**
1. **Date Format:** CSV uses `dd-MM-yyyy HH:mm:ss` — configure explicit date parsing in the CSV source transformation.
2. **Deduplication:** The current Talend job uses `tUniqRow` on `transaction_id`. In ADF Data Flow, use an Aggregate transformation grouped by `transaction_id` with `first()` on all other columns.
3. **Truncate-and-Reload:** The current pattern truncates the target before loading. Implement this via the Sink pre-copy script: `TRUNCATE TABLE FactTransaction`.
4. **Excel Connector:** ADF's Excel connector requires specifying the sheet name. Test that header detection works correctly.

**Validation Criteria:**
- Total row count = unique `transaction_id` count across all three sources
- No FK violations (all `AccountID` and `BranchID` exist in dimension tables)
- Amount totals match between source and target
- Date values parsed correctly (verify timezone handling)

---

### Phase 4: Stored Procedures & Reporting (Low Risk)

**Estimated Effort:** S | **Duration:** 0.5 day

| Priority | Component | ADF Implementation | Rationale |
|---|---|---|---|
| **4.1** | `sp_DailyTransaction` | Already deployed in Phase 0. Create an **ADF Pipeline** with a Stored Procedure Activity that calls `sp_DailyTransaction` with parameterized date range. | Enables automated reporting. Low risk — procedure is simple aggregation. |
| **4.2** | `sp_BalancePerCustomer` | Already deployed in Phase 0. Create an **ADF Pipeline** with a Stored Procedure Activity that calls `sp_BalancePerCustomer` with parameterized customer name. | Enables on-demand balance inquiries via ADF. |

**ADF Pipeline Design:**
```
Pipeline: PL_Reports
├── Stored Procedure Activity: Exec_DailyTransaction (@start_date, @end_date)
└── Stored Procedure Activity: Exec_BalancePerCustomer (@customer_name)
```

---

### Phase 5: Orchestration & Scheduling

**Estimated Effort:** M | **Duration:** 1–2 days

| Priority | Component | ADF Implementation | Rationale |
|---|---|---|---|
| **5.1** | Master Pipeline | Create **ADF Master Pipeline** (`PL_Master_DWH_Load`) that orchestrates all loads in dependency order using Execute Pipeline activities with dependency chaining. | No orchestration exists today. This adds proper dependency management. |
| **5.2** | Schedule Trigger | Create **ADF Schedule Trigger** (daily or as determined by business requirements). | No scheduling exists in the current repo. Define based on business SLAs. |
| **5.3** | Error Handling | Add **failure paths**, **retry policies** (3 retries, 10-minute intervals), and **Azure Monitor alerts** for pipeline failures. | No error handling exists in current Talend jobs. Critical for production readiness. |
| **5.4** | Monitoring | Configure **ADF Monitor**, **Log Analytics workspace**, and **Azure Monitor alerts** for pipeline health dashboards. | Provides operational visibility absent in current setup. |

**Master Pipeline Design:**
```
Pipeline: PL_Master_DWH_Load
├── Execute Pipeline: PL_Load_Dimensions_Simple
│   ├── Load_DimBranch (parallel)
│   └── Load_DimAccount (parallel)
├── Execute Pipeline: PL_Load_DimCustomer
│   └── (depends on: PL_Load_Dimensions_Simple ✅)
├── Execute Pipeline: PL_Load_FactTransaction
│   └── (depends on: PL_Load_DimCustomer ✅, PL_Load_Dimensions_Simple ✅)
└── Execute Pipeline: PL_Reports (optional, on success)
    └── (depends on: PL_Load_FactTransaction ✅)
```

---

## Migration Timeline Summary

```
Week 1                          Week 2                          Week 3
├── Phase 0: Azure Foundation   ├── Phase 2: DimCustomer        ├── Phase 4: Stored Procedures
├── Phase 1: DimBranch/Account  ├── Phase 3: FactTransaction    ├── Phase 5: Orchestration
│                               │                               ├── UAT & Validation
│                               │                               └── Cutover
```

| Phase | Duration | Effort | Risk | Parallel? |
|---|---|---|---|---|
| Phase 0: Azure Foundation | 1–2 days | M | Low | — |
| Phase 1: Simple Dimensions | 0.5–1 day | S | Low | ✅ DimBranch + DimAccount |
| Phase 2: DimCustomer | 1–2 days | M | Low | — |
| Phase 3: FactTransaction | 2–3 days | M–L | Medium | — |
| Phase 4: Stored Procedures | 0.5 day | S | Low | ✅ Both SPs |
| Phase 5: Orchestration | 1–2 days | M | Low | — |
| **Total** | **~6–10 business days** | | | |

---

## Post-Migration Recommendations

### Immediate (During Migration)
1. **Implement Incremental Loading:** Replace the TRUNCATE-and-reload pattern in `Load_FactTransaction` with a watermark-based incremental approach using ADF's built-in Change Data Capture or a high-watermark column (e.g., `TransactionDate`).
2. **Add Data Quality Checks:** Implement ADF Data Flow assertions or post-load SQL validation queries to verify row counts, null checks, and referential integrity.
3. **Parameterize File Paths:** Use ADF parameters and variables for file source paths instead of hardcoding, enabling environment-specific configurations (dev/staging/prod).

### Short-Term (Post-Migration)
4. **Source Data Consolidation:** Consider migrating the source `sample` database to Azure SQL as well, eliminating the need for a Self-Hosted Integration Runtime.
5. **Replace File Sources:** Evaluate whether the CSV and Excel transaction sources can be replaced by direct database feeds or API integrations, eliminating file-based ingestion fragility.
6. **Implement CI/CD:** Use Azure DevOps or GitHub Actions with ADF's native ARM template export to implement version-controlled pipeline deployments.
7. **Add Lineage Tracking:** Leverage Azure Purview for end-to-end data lineage across the ADF pipelines.

### Long-Term (Optimization)
8. **Consider Azure Synapse:** If data volumes grow significantly, evaluate migrating from Azure SQL Database to Azure Synapse Analytics (dedicated SQL pool) for better analytical query performance.
9. **Implement Data Lakehouse:** Consider adding a Delta Lake / Lakehouse layer in Azure for raw/curated data storage before DWH loading.
10. **Real-Time Streaming:** If business requires near-real-time transaction visibility, consider supplementing batch ETL with Azure Event Hubs + Stream Analytics for live transaction feeds.

---

## Appendix: Component Dependency Graph

```
                    ┌─────────────────┐
                    │  Source Systems  │
                    │  (SQL, CSV, XLS) │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
    ┌─────────▼──────┐ ┌────▼─────┐ ┌──────▼────────┐
    │ Load_DimBranch │ │Load_Dim  │ │ Load_Dim      │
    │ (Phase 1)      │ │Account   │ │ Customer      │
    │                │ │(Phase 1) │ │ (Phase 2)     │
    └─────────┬──────┘ └────┬─────┘ └──────┬────────┘
              │              │              │
              └──────────────┼──────────────┘
                             │
                   ┌─────────▼──────────┐
                   │Load_FactTransaction│
                   │    (Phase 3)       │
                   └─────────┬──────────┘
                             │
                   ┌─────────▼──────────┐
                   │ Stored Procedures  │
                   │    (Phase 4)       │
                   └─────────┬──────────┘
                             │
                   ┌─────────▼──────────┐
                   │  Orchestration &   │
                   │  Scheduling        │
                   │    (Phase 5)       │
                   └────────────────────┘
```
