# Package Migration & Validation — Talend to ADF Native Pipelines

## 1. Executive Summary

This document details the migration of four Talend Open Studio ETL jobs to native Azure Data Factory (ADF) pipelines for the Banking Data Warehouse. The migration eliminates dependency on Talend desktop tooling and moves all ETL orchestration to Azure-native, cloud-managed infrastructure.

### Migration Scope

| Talend Job               | ADF Pipeline                           | Type          | Complexity |
|--------------------------|----------------------------------------|---------------|------------|
| `Load_DimBranch`         | `pipeline_Load_DimBranch`              | Copy Activity | Simple     |
| `Load_DimAccount`        | `pipeline_Load_DimAccount`             | Copy Activity | Simple     |
| `Load_DimCustomer`       | `pipeline_Load_DimCustomer`            | Data Flow     | Complex    |
| `Load_FactTransaction`   | `pipeline_Load_FactTransaction`        | Data Flow     | Complex    |
| *(New)*                  | `pipeline_Master_ETL`                  | Orchestration | —          |

---

## 2. Talend Component to ADF Mapping

### Component Equivalence Table

| Talend Component    | ADF Equivalent                  | Used In                  | Notes                                                       |
|---------------------|---------------------------------|--------------------------|-------------------------------------------------------------|
| `tDBInput`          | Copy Activity (SQL Source)      | All pipelines            | SQL query defined in `sqlReaderQuery`                        |
| `tDBOutput`         | Copy Activity (SQL Sink)        | DimBranch, DimAccount    | Pre-copy TRUNCATE + insert                                  |
| `tMap` (Join)       | Data Flow — Join transformation | DimCustomer              | LEFT JOIN on city_id and state_id                           |
| `tMap` (Expression) | Data Flow — Derived Column      | DimCustomer              | UPPER() on text fields, LOWER() on email                   |
| `tUnite`            | Data Flow — Union transformation| FactTransaction          | Union by name across 3 sources                              |
| `tUniqRow`          | Data Flow — Aggregate           | FactTransaction          | GROUP BY transaction_id with first() aggregation            |
| `tFileInputExcel`   | Data Flow — Excel Source         | FactTransaction          | Reads from Azure Blob Storage                               |
| `tFileInputDelimited`| Data Flow — CSV Source          | FactTransaction          | Reads from Azure Blob Storage                               |
| `tLogRow`           | ADF Monitor / Data Flow Debug   | All                      | Built-in monitoring replaces Talend console logging         |
| *(N/A — manual)*    | Execute Pipeline Activity       | Master ETL               | New orchestration layer with dependency chaining            |
| *(N/A — manual)*    | WebActivity (failure path)      | Master ETL               | Webhook-based alerting on pipeline failures                 |

### Data Type Mapping

| Source (SQL Server)   | Talend Type      | ADF Type      | DWH Column            |
|-----------------------|------------------|---------------|-----------------------|
| `INT`                 | `int`            | `Int32`       | All ID fields         |
| `VARCHAR(n)`          | `String`         | `String`      | All text fields       |
| `MONEY`               | `BigDecimal`     | `Decimal`     | Balance, Amount       |
| `DATE`                | `Date`           | `DateTime`    | DateOpened            |
| `DATETIME`            | `Date`           | `DateTime`    | TransactionDate       |

---

## 3. Detailed Pipeline Descriptions

### 3.1 pipeline_Load_DimBranch

**Talend Equivalent:** `Load_DimBranch.zip`

| Aspect          | Talend Implementation          | ADF Implementation                           |
|-----------------|-------------------------------|----------------------------------------------|
| Source          | tDBInput from SQL Server       | Copy Activity — SqlServerSource              |
| Transformation  | None (direct pass-through)     | TabularTranslator column mapping             |
| Target          | tDBOutput to DWH.DimBranch     | AzureSqlSink with TRUNCATE pre-copy          |
| Error Handling  | tLogRow / manual check         | ADF retry policy (2 retries, 30s interval)   |

**Column Mapping:**
- `BranchID` (INT) → `BranchID` (INT)
- `BranchName` (VARCHAR) → `BranchName` (VARCHAR)
- `BranchLocation` (VARCHAR) → `BranchLocation` (VARCHAR)

### 3.2 pipeline_Load_DimAccount

**Talend Equivalent:** `Load_DimAccount.zip`

| Aspect          | Talend Implementation          | ADF Implementation                           |
|-----------------|-------------------------------|----------------------------------------------|
| Source          | tDBInput from SQL Server       | Copy Activity — SqlServerSource              |
| Transformation  | None (direct pass-through)     | TabularTranslator column mapping             |
| Target          | tDBOutput to DWH.DimAccount    | AzureSqlSink with TRUNCATE pre-copy          |
| Error Handling  | tLogRow / manual check         | ADF retry policy (2 retries, 30s interval)   |

**Column Mapping:**
- `AccountID` (INT) → `AccountID` (INT)
- `CustomerID` (INT) → `CustomerID` (INT)
- `AccountType` (VARCHAR) → `AccountType` (VARCHAR)
- `Balance` (MONEY) → `Balance` (DECIMAL)
- `DateOpened` (DATE) → `DateOpened` (DATETIME)
- `Status` (VARCHAR) → `Status` (VARCHAR)

### 3.3 pipeline_Load_DimCustomer

**Talend Equivalent:** `Load_DimCustomer.zip`

| Aspect          | Talend Implementation                | ADF Implementation                            |
|-----------------|--------------------------------------|-----------------------------------------------|
| Source          | tDBInput (customer table)            | Data Flow — SourceCustomer                    |
| Lookup 1        | tMap JOIN on city_id                 | Data Flow — JoinCustomerCity (LEFT JOIN)      |
| Lookup 2        | tMap JOIN on state_id                | Data Flow — JoinWithState (LEFT JOIN)         |
| Transform       | tMap UPPER() expression              | Data Flow — CleanseUpperCase (Derived Column) |
| Column Select   | tMap output mapping                  | Data Flow — SelectDimCustomerColumns          |
| Target          | tDBOutput to DWH.DimCustomer         | Data Flow — SinkDimCustomer (TRUNCATE + insert)|

**Transformation Logic:**
```
CustomerName = UPPER(customer_name)
Address      = UPPER(address)
CityName     = UPPER(city_name)    -- from city table via JOIN on city_id
StateName    = UPPER(state_name)   -- from state table via JOIN on state_id
Email        = LOWER(email)        -- email normalized to lowercase
```

### 3.4 pipeline_Load_FactTransaction

**Talend Equivalent:** `Load_FactTransaction.zip`

| Aspect          | Talend Implementation                | ADF Implementation                            |
|-----------------|--------------------------------------|-----------------------------------------------|
| Source 1        | tDBInput (SQL transactions)          | Data Flow — SourceSqlTransaction              |
| Source 2        | tFileInputExcel (Excel)              | Data Flow — SourceExcelTransaction (Blob)     |
| Source 3        | tFileInputDelimited (CSV)            | Data Flow — SourceCsvTransaction (Blob)       |
| Union           | tUnite (merge 3 streams)             | Data Flow — UnionAllSources (Union by name)   |
| Deduplication   | tUniqRow on transaction_id           | Data Flow — DeduplicateTransactions (Aggregate)|
| Column Select   | tMap output mapping                  | Data Flow — SelectFactColumns                 |
| Target          | tDBOutput to DWH.FactTransaction     | Data Flow — SinkFactTransaction (TRUNCATE)    |

**Deduplication Strategy:**
- Talend `tUniqRow`: Keeps first occurrence per `transaction_id`
- ADF Aggregate: `GROUP BY transaction_id`, `first()` on all other columns

### 3.5 pipeline_Master_ETL (New)

This is a **new** orchestration pipeline with no Talend equivalent (Talend jobs were run manually in sequence).

**Execution Order:**
1. `pipeline_Load_DimBranch` — no dependencies
2. `pipeline_Load_DimAccount` — depends on DimBranch success
3. `pipeline_Load_DimCustomer` — depends on DimAccount success
4. `pipeline_Load_FactTransaction` — depends on DimCustomer success

**Error Handling:** Each pipeline step has a failure path that sends a webhook alert with pipeline name, activity name, run ID, and timestamp.

---

## 4. Data Validation Results Template

Complete this table after each parallel run:

### Row Count Validation

| Table           | Source Count | Target Count | Match | Status |
|-----------------|-------------|--------------|-------|--------|
| DimBranch       |             |              | Y/N   |        |
| DimAccount      |             |              | Y/N   |        |
| DimCustomer     |             |              | Y/N   |        |
| FactTransaction |             |              | Y/N   |        |

### Checksum Validation

| Table           | Source Checksum | Target Checksum | Match | Status |
|-----------------|----------------|-----------------|-------|--------|
| DimBranch       |                |                 | Y/N   |        |
| DimAccount      |                |                 | Y/N   |        |
| DimCustomer     |                |                 | Y/N   |        |
| FactTransaction |                |                 | Y/N   |        |

### Business Rules Validation

| Rule                            | Expected | Actual | Status |
|---------------------------------|----------|--------|--------|
| FK Integrity (Account)          | 0 orphans|        |        |
| FK Integrity (Branch)           | 0 orphans|        |        |
| PK Uniqueness (Customer)        | 0 dupes  |        |        |
| PK Uniqueness (Transaction)     | 0 dupes  |        |        |
| UPPER() Compliance              | 0 errors |        |        |
| sp_DailyTransaction             | Match    |        |        |
| sp_BalancePerCustomer           | Match    |        |        |

---

## 5. Parallel-Run Procedure (Legacy vs. New)

### Phase 1: Preparation
1. Create a backup of the `sample` source database.
2. Create two DWH target databases: `DWH_TALEND` and `DWH_ADF`.
3. Ensure both environments point to the same source snapshot.

### Phase 2: Legacy Run (Talend)
1. Open Talend Open Studio and configure connections to `sample` and `DWH_TALEND`.
2. Run jobs in order: `Load_DimBranch` → `Load_DimAccount` → `Load_DimCustomer` → `Load_FactTransaction`.
3. Record execution times and row counts.

### Phase 3: ADF Run
1. Trigger `pipeline_Master_ETL` in ADF Studio (set sink to `DWH_ADF`).
2. Monitor execution in ADF Monitor.
3. Record execution times and row counts.

### Phase 4: Comparison
1. Run `validate_row_counts.sql` against both `DWH_TALEND` and `DWH_ADF`.
2. Run `validate_checksums.sql` against both databases.
3. Run `validate_business_rules.sql` against both databases.
4. Document any differences.

### Phase 5: Resolution
1. Investigate and resolve any discrepancies.
2. Re-run affected pipelines after fixes.
3. Obtain sign-off from data engineer and business owner.

### Phase 6: Cutover
1. Switch `trigger_Daily_ETL` to active.
2. Disable/unschedule legacy Talend jobs.
3. Monitor first 5 production runs.
4. Confirm stable operation before proceeding to decommission.

---

## 6. Decommission Checklist for Legacy Talend Jobs

### Pre-Decommission (Complete All Before Proceeding)

- [ ] ADF pipelines have run successfully for at least 5 consecutive days
- [ ] All validation scripts pass (row counts, checksums, business rules)
- [ ] Parallel run comparison completed and signed off
- [ ] Stored procedures validated against ADF-loaded data
- [ ] Monitoring alerts are active and tested
- [ ] Operational runbook reviewed and approved by operations team
- [ ] Rollback plan documented and tested

### Decommission Steps

- [ ] Stop `trigger_Daily_ETL` temporarily (safety measure)
- [ ] Archive Talend job ZIP files to long-term storage (Azure Blob / S3)
- [ ] Remove Talend connection credentials from credential store
- [ ] Uninstall Talend Open Studio from ETL server(s)
- [ ] Decommission ETL server(s) if no other workloads remain
- [ ] Remove Talend-related firewall rules / network ACLs
- [ ] Update documentation: remove Talend references from README, wikis
- [ ] Re-enable `trigger_Daily_ETL`
- [ ] Send decommission notification to stakeholders

### Post-Decommission Verification

- [ ] Confirm ADF pipelines continue to run on schedule
- [ ] Verify no processes are referencing old Talend endpoints
- [ ] Monitor for 30 days post-decommission
- [ ] Close migration project ticket

---

## 7. Updated Pipeline Inventory

### Active ADF Pipelines

| Pipeline                        | Type          | Schedule        | Source               | Target          |
|---------------------------------|---------------|-----------------|----------------------|-----------------|
| `pipeline_Master_ETL`           | Orchestration | Daily 02:00 UTC | —                    | —               |
| `pipeline_Load_DimBranch`       | Copy Activity | Via Master      | SQL Server (branch)  | DWH.DimBranch   |
| `pipeline_Load_DimAccount`      | Copy Activity | Via Master      | SQL Server (account) | DWH.DimAccount  |
| `pipeline_Load_DimCustomer`     | Data Flow     | Via Master      | SQL Server (customer, city, state) | DWH.DimCustomer |
| `pipeline_Load_FactTransaction` | Data Flow     | Via Master      | SQL Server + Excel + CSV | DWH.FactTransaction |

### Data Flow Definitions

| Data Flow                            | Pipeline                        | Transformations                          |
|--------------------------------------|---------------------------------|------------------------------------------|
| `dataflow_Transform_DimCustomer`     | `pipeline_Load_DimCustomer`     | 3 Sources → 2 Joins → Derived Column → Select → Sink |
| `dataflow_Transform_FactTransaction` | `pipeline_Load_FactTransaction` | 3 Sources → Union → Aggregate (dedup) → Select → Sink |

### Trigger Definitions

| Trigger              | Type            | Schedule        | Pipeline              | Status  |
|----------------------|-----------------|-----------------|-----------------------|---------|
| `trigger_Daily_ETL`  | ScheduleTrigger | Daily 02:00 UTC | `pipeline_Master_ETL` | Stopped |
| `trigger_Manual_ETL` | BlobEvents      | On-demand       | `pipeline_Master_ETL` | Stopped |

### Decommissioned Talend Jobs

| Talend Job             | Status         | Archive Location                        | Decommission Date |
|------------------------|----------------|-----------------------------------------|-------------------|
| `Load_DimBranch.zip`   | Pending        | `talend_jobs/Load_DimBranch.zip`        | TBD               |
| `Load_DimAccount.zip`  | Pending        | `talend_jobs/Load_DimAccount.zip`       | TBD               |
| `Load_DimCustomer.zip` | Pending        | `talend_jobs/Load_DimCustomer.zip`      | TBD               |
| `Load_FactTransaction.zip` | Pending    | `talend_jobs/Load_FactTransaction.zip`  | TBD               |

---

## 8. Linked Services Required

Before deploying the ADF pipelines, create these linked services in the Data Factory:

| Linked Service Name        | Type                     | Description                                        |
|----------------------------|--------------------------|----------------------------------------------------|
| `ls_SqlServer_Source`      | SqlServer                | On-premises SQL Server hosting `sample` database   |
| `ls_AzureSqlDatabase_DWH`  | AzureSqlDatabase        | Azure SQL Database hosting `DWH` schema            |
| `ls_AzureBlobStorage`     | AzureBlobStorage         | Blob storage for Excel/CSV files and staging       |

### Dataset References

| Dataset Name                | Linked Service             | Table/File                          |
|-----------------------------|----------------------------|-------------------------------------|
| `ds_SqlServer_Branch`       | `ls_SqlServer_Source`      | `[sample].dbo.branch`               |
| `ds_SqlServer_Account`      | `ls_SqlServer_Source`      | `[sample].dbo.account`              |
| `ds_SqlServer_Customer`     | `ls_SqlServer_Source`      | `[sample].dbo.customer`             |
| `ds_SqlServer_City`         | `ls_SqlServer_Source`      | `[sample].dbo.city`                 |
| `ds_SqlServer_State`        | `ls_SqlServer_Source`      | `[sample].dbo.state`                |
| `ds_SqlServer_Transaction`  | `ls_SqlServer_Source`      | `[sample].dbo.transaction_sql`      |
| `ds_Blob_TransactionExcel`  | `ls_AzureBlobStorage`      | `data/transaction_excel.xlsx`       |
| `ds_Blob_TransactionCsv`    | `ls_AzureBlobStorage`      | `data/transaction_csv.csv`          |
| `ds_AzureSql_DimBranch`     | `ls_AzureSqlDatabase_DWH` | `[DWH].dbo.DimBranch`              |
| `ds_AzureSql_DimAccount`    | `ls_AzureSqlDatabase_DWH` | `[DWH].dbo.DimAccount`             |
| `ds_AzureSql_DimCustomer`   | `ls_AzureSqlDatabase_DWH` | `[DWH].dbo.DimCustomer`            |
| `ds_AzureSql_FactTransaction`| `ls_AzureSqlDatabase_DWH`| `[DWH].dbo.FactTransaction`        |

---

## 9. Deployment Instructions

### Prerequisites
- Azure Data Factory instance provisioned
- Self-hosted Integration Runtime installed and registered (for on-premises SQL Server access)
- Azure SQL Database created with DWH schema (run `sql_scripts/01_create_tables.sql`)
- Stored procedures deployed (run `sql_scripts/02_create_procedures.sql`)
- Excel and CSV files uploaded to Azure Blob Storage

### Deployment Order
1. Create Linked Services (see Section 8)
2. Create Datasets (see Section 8)
3. Deploy Data Flows: `dataflow_Transform_DimCustomer`, `dataflow_Transform_FactTransaction`
4. Deploy Pipelines: `pipeline_Load_DimBranch`, `pipeline_Load_DimAccount`, `pipeline_Load_DimCustomer`, `pipeline_Load_FactTransaction`, `pipeline_Master_ETL`
5. Deploy Triggers: `trigger_Daily_ETL`, `trigger_Manual_ETL`
6. Deploy Alert Rules: `alert_rules.json` (ARM template deployment)
7. Deploy Monitoring Dashboard: `monitoring_dashboard.json` (ARM template deployment)

### Deployment via Azure CLI
```bash
# Deploy pipelines (repeat for each pipeline JSON)
az datafactory pipeline create \
  --factory-name <adf-name> \
  --resource-group <rg-name> \
  --name pipeline_Load_DimBranch \
  --pipeline @migration/adf_pipelines/pipeline_Load_DimBranch.json

# Deploy data flows
az datafactory data-flow create \
  --factory-name <adf-name> \
  --resource-group <rg-name> \
  --name dataflow_Transform_DimCustomer \
  --flow-type MappingDataFlow \
  --properties @migration/adf_dataflows/dataflow_Transform_DimCustomer.json

# Deploy triggers
az datafactory trigger create \
  --factory-name <adf-name> \
  --resource-group <rg-name> \
  --name trigger_Daily_ETL \
  --properties @migration/adf_triggers/trigger_Daily_ETL.json

# Deploy alert rules (ARM template)
az deployment group create \
  --resource-group <rg-name> \
  --template-file migration/monitoring/alert_rules.json \
  --parameters dataFactoryName=<adf-name> dataFactoryResourceId=<adf-resource-id> actionGroupResourceId=<action-group-id>
```

---

## 10. Risk Assessment

| Risk                                    | Likelihood | Impact | Mitigation                                              |
|-----------------------------------------|-----------|--------|----------------------------------------------------------|
| Data type precision loss (MONEY→DECIMAL)| Low       | High   | Validate checksums, compare sample records               |
| Deduplication logic difference          | Medium    | High   | Parallel run comparison, manual spot checks              |
| Source schema change during migration   | Low       | Medium | Schema validation enabled in Data Flow sources           |
| Integration Runtime connectivity        | Medium    | High   | Redundant IR setup, network monitoring                   |
| Trigger scheduling conflicts            | Low       | Medium | Single trigger, clear ownership, alert on failure        |
| Performance regression                  | Medium    | Medium | Performance comparison during parallel run               |
