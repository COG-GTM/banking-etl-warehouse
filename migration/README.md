# Banking ETL/Data Warehouse — Azure Migration Project

## Overview

This directory contains all documentation and artifacts related to the migration of the Banking ETL/Data Warehouse solution from **Talend Open Studio** (on-premises SQL Server) to **Azure Data Factory** (Azure SQL Database).

### Current Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│    Data Sources      │     │   Talend Open Studio │     │   SQL Server DWH    │
│                      │     │                      │     │                      │
│  ● SQL Server (.bak) │────▶│  ● Load_DimBranch    │────▶│  ● DimBranch        │
│  ● Excel (.xlsx)     │────▶│  ● Load_DimAccount   │────▶│  ● DimAccount       │
│  ● CSV (.csv)        │────▶│  ● Load_DimCustomer  │────▶│  ● DimCustomer      │
│                      │────▶│  ● Load_FactTxn      │────▶│  ● FactTransaction  │
└─────────────────────┘     └─────────────────────┘     │                      │
                                                         │  Stored Procedures:  │
                                                         │  ● sp_DailyTxn      │
                                                         │  ● sp_BalancePerCust │
                                                         └─────────────────────┘
```

### Target Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│    Azure Storage     │     │  Azure Data Factory  │     │  Azure SQL Database │
│                      │     │                      │     │                      │
│  ● Azure SQL (src)   │────▶│  ● PL_DimBranch      │────▶│  ● DimBranch        │
│  ● Blob (Excel)      │────▶│  ● PL_DimAccount     │────▶│  ● DimAccount       │
│  ● Blob (CSV)        │────▶│  ● PL_DimCustomer    │────▶│  ● DimCustomer      │
│                      │────▶│  ● PL_FactTxn        │────▶│  ● FactTransaction  │
└─────────────────────┘     │                      │     │                      │
                             │  Master Pipeline     │     │  Stored Procedures:  │
                             │  (orchestration)     │     │  ● sp_DailyTxn      │
                             └─────────────────────┘     │  ● sp_BalancePerCust │
                                                         └─────────────────────┘
```

## Migration Documents

| # | Document | Description | Status |
|---|---|---|---|
| 01 | [Inventory & Assessment](01_inventory_and_assessment.md) | Complete catalog of all ETL jobs, stored procedures, and data sources with migration classification, risk matrix, and recommended sequence | Complete |
| 02 | ADF Pipeline Specifications | Detailed technical design for each ADF pipeline | Planned |
| 03 | Data Migration Runbook | Step-by-step guide for migrating data to Azure | Planned |
| 04 | Test Plan & Validation | Test cases and validation criteria for migrated workloads | Planned |
| 05 | Cutover Plan | Production cutover checklist and rollback procedures | Planned |

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Migration approach | **Native ADF Rewrite** | No existing SSIS packages; Talend components map cleanly to ADF Data Flows |
| Azure SQL target | **Azure SQL Database** | All T-SQL is compatible; lower cost than Managed Instance; no need for `USE` or cross-DB queries |
| Azure-SSIS IR | **Not required** | No SSIS packages exist; avoids dedicated VM cluster cost |
| File storage | **Azure Blob Storage** | Excel and CSV files uploaded to blob; ADF has native connectors |
| Stored procedures | **Deploy as-is** | Both procedures use standard T-SQL fully compatible with Azure SQL DB |

## Workload Summary

| Workload | Type | Complexity | Migration Strategy | Risk |
|---|---|---|---|---|
| `Load_DimBranch` | Talend Job | Simple | ADF Copy Activity | Low |
| `Load_DimAccount` | Talend Job | Simple | ADF Copy Activity | Low |
| `Load_DimCustomer` | Talend Job | Complex | ADF Mapping Data Flow | Medium |
| `Load_FactTransaction` | Talend Job | Complex | ADF Mapping Data Flow | Medium-High |
| `sp_DailyTransaction` | Stored Procedure | Simple | Deploy to Azure SQL DB | Low |
| `sp_BalancePerCustomer` | Stored Procedure | Medium | Deploy to Azure SQL DB | Low |
| Star Schema DDL | SQL Script | Simple | Minor edits + deploy | Low |

## Prerequisites

Before starting the migration, ensure the following Azure resources are provisioned:

1. **Azure SQL Database** (General Purpose, 2-4 vCores)
2. **Azure Data Factory** instance
3. **Azure Blob Storage** account (or ADLS Gen2)
4. **Azure Key Vault** (for connection strings and credentials)
5. **Resource Group** for all migration resources

## Getting Started

1. Review the [Inventory & Assessment](01_inventory_and_assessment.md) document
2. Validate the migration classification and risk ratings with stakeholders
3. Provision Azure infrastructure per the prerequisites above
4. Follow the recommended migration sequence (Phases 1-6) outlined in the assessment

## Repository Structure

```
migration/
├── README.md                          # This file
├── 01_inventory_and_assessment.md     # Complete inventory and migration assessment
├── (planned) 02_adf_pipeline_specs.md
├── (planned) 03_data_migration_runbook.md
├── (planned) 04_test_plan.md
└── (planned) 05_cutover_plan.md
```
