# Pipeline Mapping Reference — Legacy to ADF

## Overview

This document maps every legacy Talend job and SSIS component to its Azure Data Factory equivalent, providing a complete traceability matrix for the migration.

## Pipeline Inventory

### Dimension Load Pipelines

| Legacy Component | Legacy Type | ADF Pipeline | ADF Type | Notes |
|-----------------|-------------|--------------|----------|-------|
| `Load_DimBranch` | Talend Job | `PL_Load_DimBranch` | Copy Activity | Direct table copy from `Sample_DB.dbo.Branch` |
| `Load_DimAccount` | Talend Job | `PL_Load_DimAccount` | Copy Activity | Direct table copy from `Sample_DB.dbo.Account` |
| `Load_DimCustomer` | Talend Job | `PL_Load_DimCustomer` | Data Flow Activity | Uses `DF_Transform_DimCustomer` for multi-table JOIN + UPPER() |

### Fact Load Pipelines

| Legacy Component | Legacy Type | ADF Pipeline | ADF Type | Notes |
|-----------------|-------------|--------------|----------|-------|
| `Load_FactTransaction` | Talend Job | `PL_Load_FactTransaction` | Data Flow Activity | Uses `DF_Transform_FactTransaction` for union + dedup |

### Orchestration

| Legacy Component | Legacy Type | ADF Pipeline | ADF Type | Notes |
|-----------------|-------------|--------------|----------|-------|
| Manual execution order | SSMS / Talend Studio | `PL_Master_DWH_Load` | Execute Pipeline | Enforces dependency chain + notifications |

### Stored Procedures

| Legacy Component | Legacy Type | ADF Pipeline | ADF Type | Notes |
|-----------------|-------------|--------------|----------|-------|
| `sp_DailyTransaction` | Manual SSMS exec | `PL_Execute_StoredProcedures` | SP Activity | Parameterized with date range |
| `sp_BalancePerCustomer` | Manual SSMS exec | `PL_Execute_StoredProcedures` | SP Activity (conditional) | Runs only when CustomerName provided |

## Talend Component to ADF Mapping

### Data Integration Components

| Talend Component | Purpose in Legacy | ADF Equivalent | Implementation |
|-----------------|-------------------|----------------|----------------|
| `tMap` | Multi-table JOIN, field mapping | Mapping Data Flow — `Join` + `DerivedColumn` | `DF_Transform_DimCustomer`: joins customer, city, state tables |
| `tUnite` | Merge multiple data streams | Mapping Data Flow — `Union` | `DF_Transform_FactTransaction`: merges SQL, CSV, Excel streams |
| `tUniqRow` | Deduplicate by key column | Mapping Data Flow — `Aggregate` (GROUP BY) | `DF_Transform_FactTransaction`: dedup on TransactionID |
| `tLogRow` | Data cleansing / transformation | Mapping Data Flow — `DerivedColumn` | `DF_Transform_DimCustomer`: UPPER() transformation |
| `tDBInput` | SQL Server source extraction | Copy Activity / Data Flow Source | SQL Server linked service + datasets |
| `tDBOutput` | SQL Server target loading | Copy Activity Sink / Data Flow Sink | DWH linked service + datasets |
| `tFileInputDelimited` | CSV file reading | Data Flow Source (DelimitedText) | `DS_CSV_Transaction` dataset |
| `tFileInputExcel` | Excel file reading | Data Flow Source (Excel) | `DS_Excel_Transaction` dataset |

### Execution & Scheduling

| Legacy Mechanism | ADF Equivalent | Configuration |
|-----------------|----------------|---------------|
| Manual Talend Studio run | Schedule Trigger | `TR_Daily_DWH_Load` — daily at 02:00 UTC |
| Manual SSMS execution | Schedule Trigger | `TR_Hourly_StoredProcedures` — hourly 08:00-18:00 UTC |
| N/A (new capability) | Event Trigger | `TR_FileArrival_CSV` — fires on CSV blob upload |

## Data Flow Mapping Detail

### DF_Transform_DimCustomer

```
Source Tables           Talend tMap                    ADF Data Flow
─────────────          ──────────                     ─────────────
Customer ──┐                                          srcCustomer ──┐
           ├── tMap (JOIN on CityID) ──►                            ├── JoinCustomerCity ──┐
City ──────┘    tMap (JOIN on StateID)                srcCity ───────┘                     │
                    │                                                                      ├── JoinWithState
State ──────────────┘                                 srcState ────────────────────────────┘
                    │                                                     │
                tMap (UPPER())                                  CleanseAndTransform (UPPER())
                    │                                                     │
                tDBOutput                                        SelectFinalColumns
                    │                                                     │
              DWH.DimCustomer                                    sinkDimCustomer
```

### DF_Transform_FactTransaction

```
Source Streams          Talend Components              ADF Data Flow
──────────────          ─────────────────              ─────────────
SQL Transaction ──┐                                   srcTransactionSQL ──────┐
                  │                                                           │
CSV Transaction ──┼── tUnite (merge) ──►              srcTransactionCSV ──┐   │
                  │                                   NormalizeCSVSchema ──┼───┼── UnionAllSources
Excel Transaction─┘                                   srcTransactionExcel─┘   │         │
                       │                              NormalizeExcelSchema────┘   DeduplicateTransactions
                  tUniqRow (dedup on                                                     │
                   transaction_id)                                              FormatTransactionDate
                       │                                                                 │
                  tDBOutput                                                     SelectFinalColumns
                       │                                                                 │
                DWH.FactTransaction                                             sinkFactTransaction
```

## Linked Services Required

| Name | Type | Target | Purpose |
|------|------|--------|---------|
| `LS_SqlServer_SampleDB` | SqlServer | `Sample_DB` database | Source system connectivity |
| `LS_SqlServer_DWH` | SqlServer | `DWH` database | Target warehouse connectivity |
| `LS_AzureBlobStorage_Staging` | AzureBlobStorage | Staging container | Data flow staging area |
| `LS_AzureBlobStorage_DataSources` | AzureBlobStorage | Data sources container | CSV/Excel file storage |

## Datasets Required

| Name | Linked Service | Table/File | Used By |
|------|---------------|------------|---------|
| `DS_SqlServer_SampleDB_Account` | `LS_SqlServer_SampleDB` | `dbo.Account` | `PL_Load_DimAccount` |
| `DS_SqlServer_SampleDB_Branch` | `LS_SqlServer_SampleDB` | `dbo.Branch` | `PL_Load_DimBranch` |
| `DS_SqlServer_SampleDB_Customer` | `LS_SqlServer_SampleDB` | `dbo.Customer` | `DF_Transform_DimCustomer` |
| `DS_SqlServer_SampleDB_City` | `LS_SqlServer_SampleDB` | `dbo.City` | `DF_Transform_DimCustomer` |
| `DS_SqlServer_SampleDB_State` | `LS_SqlServer_SampleDB` | `dbo.State` | `DF_Transform_DimCustomer` |
| `DS_SqlServer_SampleDB_Transaction` | `LS_SqlServer_SampleDB` | `dbo.Transaction` | `DF_Transform_FactTransaction` |
| `DS_CSV_Transaction` | `LS_AzureBlobStorage_DataSources` | `transaction_csv.csv` | `DF_Transform_FactTransaction` |
| `DS_Excel_Transaction` | `LS_AzureBlobStorage_DataSources` | `transaction_excel.xlsx` | `DF_Transform_FactTransaction` |
| `DS_SqlServer_DWH_DimAccount` | `LS_SqlServer_DWH` | `dbo.DimAccount` | `PL_Load_DimAccount` |
| `DS_SqlServer_DWH_DimBranch` | `LS_SqlServer_DWH` | `dbo.DimBranch` | `PL_Load_DimBranch` |
| `DS_SqlServer_DWH_DimCustomer` | `LS_SqlServer_DWH` | `dbo.DimCustomer` | `DF_Transform_DimCustomer` |
| `DS_SqlServer_DWH_FactTransaction` | `LS_SqlServer_DWH` | `dbo.FactTransaction` | `DF_Transform_FactTransaction` |
