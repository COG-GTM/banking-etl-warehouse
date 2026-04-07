# Databricks Pipeline: Banking ETL Data Warehouse

This directory contains the Databricks-native implementation of the legacy banking
ETL warehouse system, rebuilt using the **Bronze/Silver/Gold medallion architecture**
on Delta Lake.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Source Data                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────┐ │
│  │ SQL Server   │  │ CSV File     │  │ Excel File                │ │
│  │ (6 tables)   │  │ (txn data)   │  │ (txn data)                │ │
│  └──────┬───────┘  └──────┬───────┘  └─────────────┬─────────────┘ │
└─────────┼──────────────────┼────────────────────────┼───────────────┘
          │                  │                        │
          ▼                  ▼                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Raw Ingestion)                                      │
│  ┌────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │ src_customer   │  │ transactions    │  │ transactions        │  │
│  │ src_city       │  │ _csv            │  │ _excel              │  │
│  │ src_state      │  │                 │  │                     │  │
│  │ src_account    │  │                 │  │                     │  │
│  │ src_branch     │  │                 │  │                     │  │
│  │ src_transaction│  │                 │  │                     │  │
│  └────────┬───────┘  └────────┬────────┘  └──────────┬──────────┘  │
└───────────┼───────────────────┼───────────────────────┼─────────────┘
            │                   │                       │
            ▼                   ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Transformation)                                     │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────┐│
│  │ dim_customer │ │ dim_account  │ │ dim_branch   │ │ fact_      ││
│  │ (join+upper) │ │ (passthru)   │ │ (passthru)   │ │ transaction││
│  │              │ │              │ │              │ │ (union+    ││
│  │              │ │              │ │              │ │  dedup)    ││
│  └──────┬───────┘ └──────┬───────┘ └──────────────┘ └─────┬──────┘│
└─────────┼────────────────┼─────────────────────────────────┼───────┘
          │                │                                 │
          ▼                ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Analytics)                                            │
│  ┌─────────────────────────────┐  ┌──────────────────────────────┐ │
│  │ daily_transaction_summary   │  │ balance_per_customer         │ │
│  │ (replaces sp_DailyTxn)     │  │ (replaces sp_BalancePerCust) │ │
│  └─────────────────────────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
databricks/
├── README.md                          # This file
├── MIGRATION_MAPPING.md               # Detailed legacy → Databricks mapping
├── workflow.json                      # Databricks workflow/job definition (DAG)
├── setup/
│   └── upload_source_data.py          # Environment setup & data upload instructions
├── bronze/
│   ├── ingest_sql_server_tables.py    # Ingest 6 SQL Server source tables
│   ├── ingest_transactions_csv.py     # Ingest CSV transaction file
│   └── ingest_transactions_excel.py   # Ingest Excel transaction file
├── silver/
│   ├── build_dim_customer.py          # DimCustomer (join + uppercase)
│   ├── build_dim_account.py           # DimAccount (schema alignment)
│   ├── build_dim_branch.py            # DimBranch (schema alignment)
│   └── build_fact_transaction.py      # FactTransaction (union + dedup)
└── gold/
    ├── daily_transaction_summary.py   # Daily aggregates (replaces sp_DailyTransaction)
    └── balance_per_customer.py        # Balance reconciliation (replaces sp_BalancePerCustomer)
```

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Cluster** with Databricks Runtime 14.3+ (Photon recommended)
3. **spark-excel** library installed on cluster (optional; CSV fallback available):
   - Maven coordinates: `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`
4. **Source data files** uploaded to Unity Catalog Volume
   (see `setup/upload_source_data.py` for details)

## Quick Start

### 1. Prepare Source Data

Export the SQL Server source tables from `sample.bak` as CSV files:
- `src_customer.csv`, `src_city.csv`, `src_state.csv`
- `src_account.csv`, `src_branch.csv`, `src_transaction.csv`

Place these along with the existing `transaction_csv.csv` and
`transaction_excel.xlsx` in a local directory.

### 2. Run Setup Notebook

Open and run `setup/upload_source_data.py` in Databricks. This creates:
- Unity Catalog schemas: `bronze`, `silver`, `gold`
- A Volume for source files at `/Volumes/banking_dwh/raw_data/source_files/`

Then upload all source files to the Volume.

### 3. Run the Pipeline

**Option A: Databricks Workflow (recommended)**

Import `workflow.json` as a Databricks Job:
```bash
databricks jobs create --json @databricks/workflow.json
```

Then trigger it:
```bash
databricks jobs run-now --job-id <JOB_ID>
```

**Option B: Run notebooks manually**

Execute notebooks in this order:
1. `setup/upload_source_data.py`
2. `bronze/ingest_sql_server_tables.py` (parallel with next two)
3. `bronze/ingest_transactions_csv.py`
4. `bronze/ingest_transactions_excel.py`
5. `silver/build_dim_branch.py` (parallel with next two)
6. `silver/build_dim_account.py`
7. `silver/build_dim_customer.py`
8. `silver/build_fact_transaction.py` (after ALL bronze notebooks)
9. `gold/daily_transaction_summary.py`
10. `gold/balance_per_customer.py`

### 4. Query Gold Tables

```sql
-- Equivalent to: EXEC sp_DailyTransaction @start_date='2024-01-18', @end_date='2024-01-20'
SELECT * FROM banking_dwh.gold.daily_transaction_summary
WHERE Date BETWEEN '2024-01-18' AND '2024-01-20'
ORDER BY Date;

-- Equivalent to: EXEC sp_BalancePerCustomer @customer_name='John'
SELECT * FROM banking_dwh.gold.balance_per_customer
WHERE CustomerName LIKE '%JOHN%'
ORDER BY CustomerName, AccountType;
```

## Configuration

All notebooks use these configurable variables at the top:

| Variable | Default | Description |
|----------|---------|-------------|
| `VOLUME_PATH` | `/Volumes/banking_dwh/raw_data/source_files` | Location of source data files |
| `BRONZE_CATALOG` | `banking_dwh` | Unity Catalog name for Bronze tables |
| `BRONZE_SCHEMA` | `bronze` | Schema for raw ingested data |
| `SILVER_CATALOG` | `banking_dwh` | Unity Catalog name for Silver tables |
| `SILVER_SCHEMA` | `silver` | Schema for transformed dimensions/facts |
| `GOLD_CATALOG` | `banking_dwh` | Unity Catalog name for Gold tables |
| `GOLD_SCHEMA` | `gold` | Schema for analytical aggregates |

To point at a different catalog or schema, update these variables in each notebook.

## Migration Documentation

See [`MIGRATION_MAPPING.md`](MIGRATION_MAPPING.md) for a detailed mapping of:
- Legacy SQL Server tables → Delta tables
- Talend ETL jobs → Databricks notebooks
- Talend components (tMap, tUnite, tUniqRow) → PySpark equivalents
- T-SQL stored procedures → Gold layer queries
- SQL Server column types → Delta/Spark types

## Design Decisions

1. **Full Overwrite**: All notebooks use `mode("overwrite")` for simplicity.
   For production, consider switching to `MERGE INTO` for incremental processing.

2. **Metadata Columns**: Every Bronze/Silver table includes `_ingestion_timestamp`
   or `_transform_timestamp` for lineage tracking — not present in the legacy system.

3. **Source System Tracking**: `fact_transaction` includes a `_source_system` column
   to track which source each record came from (sql_server, csv_file, excel_file).

4. **Gold Tables vs Stored Procedures**: The legacy stored procedures ran on-demand
   with parameters. The Gold tables pre-materialize the full result set; consumers
   apply filters at query time. This trades storage for query performance.

5. **Deduplication Strategy**: Uses `Window + row_number()` instead of `dropDuplicates()`
   for deterministic ordering when resolving duplicates across sources.
