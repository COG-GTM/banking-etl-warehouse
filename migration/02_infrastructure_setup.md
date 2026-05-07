# Migration Phase 2: Infrastructure Setup

## Document Overview

| Field | Value |
|-------|-------|
| Phase | 2 — Infrastructure Setup |
| Status | Ready for deployment |
| Target Platform | Microsoft Azure |
| Source System | Talend Open Studio + SQL Server (on-premises) |
| Target System | Azure Data Factory + Azure SQL Database |

---

## Executive Summary

This document summarizes the infrastructure decisions and configurations for migrating the Banking ETL/Data Warehouse solution from on-premises Talend Open Studio to Azure Data Factory (ADF). The infrastructure is defined as code (IaC) using both Bicep and ARM templates, enabling repeatable, auditable deployments across environments.

---

## Infrastructure Architecture

### Components Provisioned

| Component | Azure Service | Purpose |
|-----------|--------------|---------|
| ETL Engine | Azure Data Factory | Replaces Talend Open Studio for pipeline orchestration |
| Data Warehouse | Azure SQL Database | Hosts the Star Schema DWH (DimAccount, DimBranch, DimCustomer, FactTransaction) |
| SSIS Catalog | Azure SQL Database (SSISDB) | Hosts migrated SSIS packages for legacy compatibility |
| SSIS Runtime | Azure-SSIS Integration Runtime | Executes SSIS packages in the cloud |
| On-Prem Gateway | Self-Hosted Integration Runtime | Secure data bridge to on-premises SQL Server |
| Staging Storage | Azure Blob Storage | Stages CSV and Excel source files for ingestion |
| Monitoring | Azure Monitor / Log Analytics | Centralized logging and metrics for ADF and SQL |

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Azure Resource Group                         │
│                                                                     │
│  ┌──────────────────────┐    ┌──────────────────────────────────┐  │
│  │  Azure Data Factory   │    │       Azure SQL Server           │  │
│  │                       │    │                                  │  │
│  │  ┌─────────────────┐ │    │  ┌────────────┐ ┌────────────┐  │  │
│  │  │ IR-AzureSSIS    │ │◄──►│  │  SSISDB     │ │  DWH DB    │  │  │
│  │  │ (Managed IR)    │ │    │  │  (Catalog)  │ │ (Star      │  │  │
│  │  └─────────────────┘ │    │  │             │ │  Schema)   │  │  │
│  │                       │    │  └────────────┘ └────────────┘  │  │
│  │  ┌─────────────────┐ │    └──────────────────────────────────┘  │
│  │  │ IR-SelfHosted   │ │                                          │
│  │  │ (On-Prem Bridge)│ │    ┌──────────────────────────────────┐  │
│  │  └────────┬────────┘ │    │     Azure Blob Storage           │  │
│  │           │           │    │     (CSV/Excel Staging)          │  │
│  │  Linked Services:     │    └──────────────────────────────────┘  │
│  │  - ls_AzureSqlDB     │                                          │
│  │  - ls_AzureBlobStore │    ┌──────────────────────────────────┐  │
│  │  - ls_OnPremSqlSvr   │    │     Log Analytics Workspace      │  │
│  └──────────┬────────────┘    │     (Diagnostics)                │  │
│             │                 └──────────────────────────────────┘  │
└─────────────┼──────────────────────────────────────────────────────┘
              │
              │ (Outbound via SHIR)
              ▼
┌─────────────────────────────┐
│   On-Premises Network        │
│   ┌───────────────────────┐ │
│   │ SQL Server (sample DB)│ │
│   │ Source: sample.bak    │ │
│   └───────────────────────┘ │
└─────────────────────────────┘
```

---

## Key Design Decisions

### 1. Dual IaC Templates (Bicep + ARM)

**Decision**: Provide both Bicep and equivalent ARM templates.

**Rationale**: Bicep is the recommended authoring language for Azure IaC, offering cleaner syntax and better tooling. However, some teams may have existing ARM-based CI/CD pipelines or may not yet have Bicep tooling available. Providing both ensures adoption flexibility.

### 2. Azure-SSIS Integration Runtime

**Decision**: Provision an Azure-SSIS IR with SSISDB catalog on Azure SQL Database.

**Rationale**: The existing Talend jobs can be converted to SSIS packages as an intermediate migration step. This allows the team to migrate incrementally — running converted SSIS packages first, then progressively refactoring to native ADF data flows.

**Configuration**:
- Node Size: `Standard_D2_v3` (2 vCPU, 8 GB RAM) — sufficient for the current data volumes.
- Node Count: 1 (scale up as needed).
- Max Parallel Executions: 4 per node.
- Edition: Standard (Enterprise if advanced features like data quality are needed later).

### 3. Self-Hosted Integration Runtime

**Decision**: Deploy a Self-Hosted IR for on-premises SQL Server connectivity.

**Rationale**: The source database (`sample.bak`) originates from an on-premises SQL Server. A Self-Hosted IR provides secure, encrypted data transfer without exposing the on-premises database to the public internet.

### 4. Azure SQL Database (not Managed Instance)

**Decision**: Use Azure SQL Database for both the DWH and SSISDB.

**Rationale**:
- The DWH schema is relatively simple (4 tables, 2 stored procedures).
- Azure SQL Database is more cost-effective than Managed Instance for this workload size.
- The S1 tier (20 DTUs) is adequate for development; scale to S3/P1 for production.
- If full SQL Server compatibility is needed later (e.g., cross-database queries, SQL Agent), migration to Managed Instance is straightforward.

### 5. Blob Storage for File Staging

**Decision**: Use Azure Blob Storage as the landing zone for CSV and Excel files.

**Rationale**: The existing pipeline ingests data from CSV (`transaction_csv.csv`) and Excel (`transaction_excel.xlsx`) files. Blob Storage provides a scalable, cost-effective staging layer. ADF has native connectors for both CSV and Excel formats in Blob Storage.

### 6. VNet Integration (Placeholder)

**Decision**: VNet integration is parameterized but not enforced.

**Rationale**: For development, public endpoints are simpler and faster. For production, VNet integration should be enabled to:
- Place the Azure-SSIS IR in a VNet for private connectivity.
- Use Private Endpoints for Azure SQL and Blob Storage.
- Restrict all public network access.

### 7. System-Assigned Managed Identity

**Decision**: ADF uses a system-assigned managed identity.

**Rationale**: Managed identity enables passwordless authentication to Azure SQL Database and Blob Storage via Azure AD, eliminating the need to store connection credentials. The Bicep/ARM templates output the principal ID for RBAC assignment.

### 8. Diagnostic Settings

**Decision**: Forward all ADF and SQL logs/metrics to Log Analytics.

**Rationale**: Centralized monitoring is critical for production ETL workloads. Log Analytics enables:
- Pipeline run success/failure tracking.
- SSIS package execution monitoring.
- SQL Database performance monitoring (DTU usage, query performance).
- Alerting on failures or performance degradation.

---

## Star Schema Mapping

The existing DWH schema is preserved in Azure SQL Database:

| Table | Type | Key Columns | Row Estimate |
|-------|------|-------------|-------------|
| `DimAccount` | Dimension | AccountID (PK), CustomerID | Thousands |
| `DimBranch` | Dimension | BranchID (PK) | Hundreds |
| `DimCustomer` | Dimension | CustomerID (PK) | Thousands |
| `FactTransaction` | Fact | TransactionID (PK), AccountID (FK), BranchID (FK) | Millions |

### Stored Procedures

| Procedure | Purpose | Parameters |
|-----------|---------|------------|
| `sp_DailyTransaction` | Daily transaction volume and amount aggregation | `@start_date`, `@end_date` |
| `sp_BalancePerCustomer` | Current balance calculation per active account | `@customer_name` |

---

## ETL Source-to-Target Mapping

| Source | Format | Target Table | ADF Pipeline |
|--------|--------|-------------|-------------|
| SQL Server (`sample` DB) — branch table | SQL | DimBranch | Load_DimBranch |
| SQL Server (`sample` DB) — account table | SQL | DimAccount | Load_DimAccount |
| SQL Server (`sample` DB) — customer + city + state | SQL | DimCustomer | Load_DimCustomer |
| SQL Server — transactions | SQL | FactTransaction | Load_FactTransaction |
| `transaction_csv.csv` | CSV | FactTransaction | Load_FactTransaction |
| `transaction_excel.xlsx` | Excel | FactTransaction | Load_FactTransaction |

---

## Cost Estimate (Development Environment)

| Resource | SKU | Estimated Monthly Cost (USD) |
|----------|-----|------------------------------|
| Azure SQL Database (DWH) | S1 (20 DTU) | ~$30 |
| Azure SQL Database (SSISDB) | S1 (20 DTU) | ~$30 |
| Azure Data Factory | Pay-per-use | ~$5–20 (low activity) |
| Azure-SSIS IR (Standard_D2_v3) | Per-hour when running | ~$0.84/hr (~$25/mo at 1hr/day) |
| Azure Blob Storage | Hot tier, <1 GB | ~$1 |
| Log Analytics | Per-GB ingestion | ~$5 |
| **Total (Dev)** | | **~$95–115/month** |

> **Note**: The Azure-SSIS IR is billed only when running. Stop it when not in use to minimize costs. Production costs will vary based on data volumes and IR uptime.

---

## Security Considerations

1. **TLS 1.2 Minimum**: Enforced on Azure SQL Server.
2. **Azure AD Authentication**: Recommended for Azure SQL Database connections (via managed identity).
3. **Key Vault Integration**: Secure parameters (passwords, connection strings) referenced from Azure Key Vault.
4. **Network Isolation (Production)**: Enable VNet integration and Private Endpoints.
5. **RBAC**: Grant ADF managed identity the minimum required roles (e.g., `db_datareader`/`db_datawriter` on the DWH, `Storage Blob Data Contributor` on Blob Storage).
6. **Audit Logging**: SQL Server auditing and ADF diagnostic logs forwarded to Log Analytics.

---

## Next Steps

1. **Deploy the infrastructure** using the provided Bicep/ARM templates and deployment script.
2. **Configure linked services** with actual connection strings in ADF Studio.
3. **Install and register** the Self-Hosted IR on an on-premises machine.
4. **Deploy the DWH schema** by executing the SQL scripts against the Azure SQL Database.
5. **Upload source data** (CSV, Excel) to Blob Storage.
6. **Convert Talend jobs** to ADF pipelines (Phase 3 of migration).
7. **Enable VNet integration** and Private Endpoints for production readiness.
8. **Configure CI/CD** with Git integration in ADF Studio.

---

## Files Reference

| File | Description |
|------|-------------|
| `infrastructure/bicep/main.bicep` | Bicep IaC template |
| `infrastructure/bicep/parameters.json` | Bicep parameter file |
| `infrastructure/arm/azuredeploy.json` | ARM IaC template |
| `infrastructure/scripts/deploy.sh` | Azure CLI deployment script |
| `infrastructure/adf/linked_services/ls_AzureSqlDatabase.json` | DWH linked service |
| `infrastructure/adf/linked_services/ls_AzureBlobStorage.json` | Blob Storage linked service |
| `infrastructure/adf/linked_services/ls_OnPremSqlServer.json` | On-prem SQL linked service |
| `infrastructure/adf/integration_runtimes/ir_AzureSSIS.json` | Azure-SSIS IR config |
| `infrastructure/adf/integration_runtimes/ir_SelfHosted.json` | Self-Hosted IR config |
| `infrastructure/docs/setup_guide.md` | Deployment and validation guide |
