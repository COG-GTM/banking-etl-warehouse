# Azure Data Factory & SSIS Integration Runtime — Setup Guide

This guide walks through provisioning, configuring, and validating the Azure infrastructure required to migrate existing SSIS ETL packages from the banking data warehouse project into Azure Data Factory.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Architecture Overview](#2-architecture-overview)
3. [Step-by-Step Provisioning](#3-step-by-step-provisioning)
4. [Network Connectivity Validation](#4-network-connectivity-validation)
5. [Deploying a Proof-of-Concept SSIS Package](#5-deploying-a-proof-of-concept-ssis-package)
6. [Running the Sample ADF Pipeline](#6-running-the-sample-adf-pipeline)
7. [CI/CD Pipeline Setup](#7-cicd-pipeline-setup)
8. [Environment Configuration](#8-environment-configuration)
9. [Troubleshooting](#9-troubleshooting)
10. [Cost Estimation](#10-cost-estimation)

---

## 1. Prerequisites

### Azure Subscription & Permissions

| Requirement | Details |
|---|---|
| **Azure Subscription** | Active subscription with billing enabled |
| **Resource Group** | Pre-created resource group (or permissions to create one) |
| **RBAC Roles** | `Contributor` on the resource group (minimum) |
| **Azure AD** | Permissions to create service principals (for CI/CD) |
| **Resource Providers** | `Microsoft.DataFactory`, `Microsoft.Sql`, `Microsoft.Network` must be registered |

### Local Tooling

| Tool | Version | Purpose |
|---|---|---|
| [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) | >= 2.50 | Deploy Bicep templates |
| [Bicep CLI](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/install) | >= 0.22 | Template authoring & linting |
| [SQL Server Management Studio](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms) | >= 19.0 | Database management & SSIS deployment |
| [Git](https://git-scm.com/) | >= 2.40 | Version control |

### Register Required Resource Providers

```bash
az provider register --namespace Microsoft.DataFactory
az provider register --namespace Microsoft.Sql
az provider register --namespace Microsoft.Network
```

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Azure Resource Group                        │
│                                                                    │
│  ┌──────────────┐    ┌──────────────────────────────────────────┐  │
│  │  Azure Blob  │    │        Azure Data Factory (ADF)          │  │
│  │   Storage    │    │                                          │  │
│  │              │    │  ┌────────────────────────────────────┐  │  │
│  │  - CSV files │◄───┤  │     Azure-SSIS Integration        │  │  │
│  │  - Excel     │    │  │     Runtime (VNet-injected)        │  │  │
│  │              │    │  │                                    │  │  │
│  └──────────────┘    │  │  Runs migrated SSIS packages      │  │  │
│                      │  └──────────┬───────────────────────┘  │  │
│                      │             │                           │  │
│                      │  ┌──────────┴──────────┐               │  │
│                      │  │  ADF Pipelines      │               │  │
│                      │  │  - CsvToFactTxn     │               │  │
│                      │  │  - Linked Services   │               │  │
│                      │  │  - Daily Trigger     │               │  │
│                      │  └──────────┬──────────┘               │  │
│                      └─────────────┼──────────────────────────┘  │
│                                    │                              │
│  ┌─────────────────────────────────▼──────────────────────────┐  │
│  │              Azure SQL Server                              │  │
│  │                                                            │  │
│  │  ┌──────────┐    ┌──────────────────────────────────────┐  │  │
│  │  │  SSISDB  │    │  DWH Database (Star Schema)          │  │  │
│  │  │ (catalog)│    │                                      │  │  │
│  │  │          │    │  DimCustomer ─┐                      │  │  │
│  │  └──────────┘    │  DimAccount  ─┼─► FactTransaction    │  │  │
│  │                  │  DimBranch   ─┘                      │  │  │
│  │                  └──────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Virtual Network                                           │  │
│  │  ┌────────────────────────────┐                            │  │
│  │  │  ssis-ir-subnet            │  NSG: ADF Mgmt + SQL + S3 │  │
│  │  │  (SSIS IR VMs injected)    │                            │  │
│  │  └────────────────────────────┘                            │  │
│  └────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Resources Provisioned

| Resource | Purpose |
|---|---|
| **Virtual Network + NSG** | Network isolation for SSIS IR; NSG rules for ADF management, SQL, and storage traffic |
| **Azure SQL Server** | Hosts both SSISDB catalog and DWH database |
| **SSISDB Database** | Required catalog database for Azure-SSIS IR package management |
| **DWH Database** | Star Schema data warehouse (DimCustomer, DimAccount, DimBranch, FactTransaction) |
| **Azure Data Factory** | Orchestration engine for ETL pipelines and SSIS package execution |
| **Azure-SSIS IR** | Managed compute for running SSIS packages in the cloud |
| **Linked Services** | Connections to Blob Storage (sources) and Azure SQL (DWH target) |

---

## 3. Step-by-Step Provisioning

### 3.1 Create the Resource Group

```bash
# Customize these values for your environment
LOCATION="eastus"
RG_NAME="rg-banking-dwh-dev"

az group create --name "$RG_NAME" --location "$LOCATION"
```

### 3.2 Deploy the Bicep Templates

```bash
cd infrastructure

# Dev environment deployment
az deployment group create \
  --resource-group "$RG_NAME" \
  --template-file bicep/main.bicep \
  --parameters parameters/dev.bicepparam \
  --parameters sqlAdminPassword='<YOUR-SECURE-PASSWORD>' \
  --name "banking-dwh-initial-deploy"
```

> **Security Note:** Never commit passwords. Pass `sqlAdminPassword` via CLI, Key Vault reference, or GitHub Secrets in CI/CD.

### 3.3 Verify Deployment Outputs

```bash
az deployment group show \
  --resource-group "$RG_NAME" \
  --name "banking-dwh-initial-deploy" \
  --query properties.outputs
```

Expected outputs:
- `sqlServerFqdn` — e.g., `bankingdwh-dev-sqlserver.database.windows.net`
- `dataFactoryName` — e.g., `bankingdwh-dev-adf`
- `ssisIrName` — `AzureSSIS-IR`

### 3.4 Initialize the DWH Schema

After deployment, run the existing SQL scripts against the new DWH database:

```bash
# Connect to the DWH database and execute schema creation
sqlcmd -S "<sqlServerFqdn>" -d DWH -U sqladmin -P '<password>' \
  -i ../sql_scripts/01_create_tables.sql

# Deploy stored procedures
sqlcmd -S "<sqlServerFqdn>" -d DWH -U sqladmin -P '<password>' \
  -i ../sql_scripts/02_create_procedures.sql
```

Or use SSMS to connect and run the scripts interactively.

### 3.5 Start the Azure-SSIS Integration Runtime

The IR is provisioned in a stopped state. Start it manually:

```bash
az datafactory integration-runtime start \
  --resource-group "$RG_NAME" \
  --factory-name "bankingdwh-dev-adf" \
  --name "AzureSSIS-IR"
```

> **Note:** Starting the IR takes 20-30 minutes. The IR incurs compute costs while running — stop it when not in use during development.

---

## 4. Network Connectivity Validation

### 4.1 Verify SQL Server Accessibility

```bash
# From your local machine (ensure your IP is allowlisted)
sqlcmd -S "bankingdwh-dev-sqlserver.database.windows.net" \
  -d SSISDB -U sqladmin -P '<password>' \
  -Q "SELECT @@VERSION"
```

### 4.2 Verify NSG Rules

```bash
az network nsg rule list \
  --resource-group "$RG_NAME" \
  --nsg-name "bankingdwh-dev-ssis-nsg" \
  --output table
```

Expected rules:
| Priority | Direction | Name | Port | Source |
|---|---|---|---|---|
| 100 | Inbound | AllowAzureDataFactoryManagement | 29876-29877 | DataFactoryManagement |
| 100 | Outbound | AllowAzureSqlOutbound | 1433 | VirtualNetwork → Sql |
| 110 | Outbound | AllowStorageOutbound | 443 | VirtualNetwork → Storage |
| 120 | Outbound | AllowHttpsOutbound | 443 | VirtualNetwork → AzureCloud |

### 4.3 Verify VNet Service Endpoints

```bash
az network vnet subnet show \
  --resource-group "$RG_NAME" \
  --vnet-name "bankingdwh-dev-vnet" \
  --name "ssis-ir-subnet" \
  --query serviceEndpoints
```

### 4.4 Check IR Status

```bash
az datafactory integration-runtime get-status \
  --resource-group "$RG_NAME" \
  --factory-name "bankingdwh-dev-adf" \
  --name "AzureSSIS-IR"
```

Expected state: `Started` (after starting) with nodes showing `Running`.

---

## 5. Deploying a Proof-of-Concept SSIS Package

### 5.1 Prepare the Package

1. Open one of the existing Talend jobs (e.g., `Load_DimBranch`) as a reference
2. Create an equivalent SSIS package in Visual Studio / SSDT that:
   - Reads from a CSV or SQL source
   - Performs basic transformations
   - Writes to the DWH `DimBranch` table

### 5.2 Deploy via SSMS

1. Connect to the Azure SQL Server in SSMS
2. Navigate to **Integration Services Catalogs → SSISDB**
3. Create a folder (e.g., `BankingETL`)
4. Right-click the folder → **Deploy Project**
5. Select your `.ispac` file and complete the wizard

### 5.3 Execute the Package

```sql
-- In SSMS, connected to SSISDB
DECLARE @execution_id BIGINT;

EXEC [SSISDB].[catalog].[create_execution]
    @package_name = N'Load_DimBranch.dtsx',
    @folder_name = N'BankingETL',
    @project_name = N'BankingETL',
    @use32bitruntime = False,
    @reference_id = NULL,
    @execution_id = @execution_id OUTPUT;

EXEC [SSISDB].[catalog].[start_execution]
    @execution_id = @execution_id;

-- Check execution status
SELECT * FROM [SSISDB].[catalog].[executions]
WHERE execution_id = @execution_id;
```

### 5.4 Execute via ADF

Alternatively, create an **Execute SSIS Package** activity in ADF:

1. Open ADF Studio → Author → Pipelines
2. Add an **Execute SSIS Package** activity
3. Configure:
   - **Integration Runtime**: `AzureSSIS-IR`
   - **SSISDB folder**: `BankingETL`
   - **Project**: `BankingETL`
   - **Package**: `Load_DimBranch.dtsx`
4. Debug or trigger the pipeline

---

## 6. Running the Sample ADF Pipeline

The included `CsvToFactTransaction` pipeline demonstrates native ADF data movement (no SSIS required).

### 6.1 Upload Source Data

Upload the CSV file from `data_sources/` to Azure Blob Storage:

```bash
STORAGE_ACCOUNT="bankingdwhdev"
CONTAINER="banking-sources"

# Create the container
az storage container create \
  --account-name "$STORAGE_ACCOUNT" \
  --name "$CONTAINER"

# Upload the CSV file
az storage blob upload \
  --account-name "$STORAGE_ACCOUNT" \
  --container-name "$CONTAINER" \
  --file ../data_sources/transaction_csv.csv \
  --name "transactions/transaction_csv.csv"
```

### 6.2 Grant ADF Access to Storage

```bash
# Get ADF managed identity object ID
ADF_PRINCIPAL_ID=$(az datafactory show \
  --resource-group "$RG_NAME" \
  --name "bankingdwh-dev-adf" \
  --query identity.principalId -o tsv)

# Assign Storage Blob Data Reader role
az role assignment create \
  --assignee "$ADF_PRINCIPAL_ID" \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/<sub-id>/resourceGroups/$RG_NAME/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"
```

### 6.3 Trigger the Pipeline

In ADF Studio:
1. Navigate to **Author → Pipelines → BankingETL → CsvToFactTransaction**
2. Click **Debug** to run a test execution
3. Monitor progress in the **Monitor** tab
4. Verify data in the DWH:

```sql
SELECT COUNT(*) AS TotalRows FROM DWH.dbo.FactTransaction;

-- Test the stored procedure with the loaded data
EXEC sp_DailyTransaction
    @start_date = '2024-01-01',
    @end_date = '2024-12-31';
```

---

## 7. CI/CD Pipeline Setup

### 7.1 GitHub Secrets Configuration

Add these secrets in your GitHub repository settings (**Settings → Secrets and variables → Actions**):

| Secret Name | Description |
|---|---|
| `AZURE_CREDENTIALS` | Service principal JSON (`az ad sp create-for-rbac --sdk-auth`) |
| `SQL_ADMIN_PASSWORD_DEV` | SQL admin password for dev |
| `SQL_ADMIN_PASSWORD_STG` | SQL admin password for staging |
| `SQL_ADMIN_PASSWORD_PROD` | SQL admin password for production |

### 7.2 GitHub Variables

| Variable Name | Description | Example |
|---|---|---|
| `AZURE_RG_DEV` | Dev resource group | `rg-banking-dwh-dev` |
| `AZURE_RG_STG` | Staging resource group | `rg-banking-dwh-stg` |
| `AZURE_RG_PROD` | Production resource group | `rg-banking-dwh-prod` |

### 7.3 Create the Service Principal

```bash
# Create SP with Contributor role on the subscription (or scope to RG)
az ad sp create-for-rbac \
  --name "sp-banking-dwh-cicd" \
  --role Contributor \
  --scopes /subscriptions/<subscription-id> \
  --sdk-auth
```

Copy the JSON output to the `AZURE_CREDENTIALS` GitHub secret.

### 7.4 GitHub Environments

Configure [GitHub Environments](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment) for approval gates:

- **dev** — No approval required (auto-deploys on merge to main)
- **staging** — Requires 1 reviewer approval
- **production** — Requires 2 reviewer approvals + environment protection rules

### 7.5 Pipeline Workflow

The CI/CD pipeline (`infrastructure/pipelines/deploy-adf-infrastructure.yml`) follows this flow:

```
PR opened → Lint + Validate + What-If (dry run)
Merge to main → Deploy Dev → Deploy Staging (approval) → Deploy Prod (approval)
```

Copy the pipeline file to `.github/workflows/` to activate it:

```bash
cp infrastructure/pipelines/deploy-adf-infrastructure.yml .github/workflows/
```

---

## 8. Environment Configuration

### Parameter Files

Each environment has a dedicated parameter file in `infrastructure/parameters/`:

| File | Env | SSIS IR Nodes | IR Size | SQL SKU |
|---|---|---|---|---|
| `dev.bicepparam` | Development | 1 | Standard_D2_v3 | S1 |
| `staging.bicepparam` | Staging | 2 | Standard_D4_v3 | S2 |
| `prod.bicepparam` | Production | 4 | Standard_D8_v3 | S3/P1 |

### Customization Checklist

Before deploying, update these placeholder values in the parameter files:

- [ ] `location` — Your preferred Azure region
- [ ] `namePrefix` — Organization-specific naming prefix
- [ ] `sqlAdminLogin` — SQL Server admin username
- [ ] `gitAccountName` — Your GitHub organization (dev only)
- [ ] `gitRepositoryName` — Your repository name (dev only)
- [ ] `storageAccountUrl` — Your Azure Blob Storage URL

---

## 9. Troubleshooting

### SSIS IR Fails to Start

**Symptom:** IR stays in `Starting` state for > 45 minutes or transitions to `Failed`.

**Checks:**
1. Verify NSG rules allow ports 29876-29877 inbound from `DataFactoryManagement`
2. Verify subnet has service endpoints for `Microsoft.Sql` and `Microsoft.Storage`
3. Check SSISDB is accessible from the IR subnet (VNet rule + firewall)
4. Ensure the subnet has enough available IP addresses (minimum /27, recommended /24)

```bash
# Check IR detailed status
az datafactory integration-runtime get-status \
  --resource-group "$RG_NAME" \
  --factory-name "<adf-name>" \
  --name "AzureSSIS-IR" \
  --query "properties.typeProperties"
```

### SSISDB Connection Failures

**Symptom:** `Cannot open database "SSISDB" requested by the login`

**Fix:**
1. Verify the SSISDB database exists on the SQL Server
2. Check the SQL admin credentials match what was used during deployment
3. Verify the SQL Server firewall allows Azure services (`0.0.0.0` rule)
4. Test connectivity from your local machine first:

```bash
sqlcmd -S "<server>.database.windows.net" -d SSISDB -U sqladmin -P '<password>' \
  -Q "SELECT 1"
```

### Pipeline Copy Activity Fails

**Symptom:** `The given key was not present in the dictionary` or schema mismatch errors.

**Fix:**
1. Verify the CSV file schema matches the dataset definition in `adf/dataset/CsvTransactionSource.json`
2. Check column names are case-sensitive matches
3. Verify the Blob Storage container and path match the dataset configuration
4. Test with a small subset of data first

### VNet Injection Errors

**Symptom:** `SubnetWithDelegationNotSupportedForSSIS` or similar networking errors.

**Fix:**
1. Ensure the subnet does NOT have a delegation set (Azure-SSIS IR injects VMs directly)
2. Verify no other service is using the subnet
3. Ensure the subnet has at least 16 available IP addresses

### High Costs

**Symptom:** Unexpected Azure charges.

**Mitigation:**
1. Stop the SSIS IR when not in use: `az datafactory integration-runtime stop ...`
2. Use `Standard_D2_v3` (smallest tier) for development
3. Scale down SQL database SKUs for non-prod environments
4. Set up Azure Cost Management alerts

---

## 10. Cost Estimation

Approximate monthly costs by environment (East US pricing, as of 2025):

| Component | Dev (min) | Staging | Prod |
|---|---|---|---|
| Azure SQL (SSISDB) | ~$30 (S1) | ~$75 (S2) | ~$150 (S3) |
| Azure SQL (DWH) | ~$30 (S1) | ~$75 (S2) | ~$465 (P1) |
| SSIS IR (1 node, 8h/day) | ~$120 | ~$480 (2 nodes) | ~$1,920 (4 nodes) |
| ADF (pipeline runs) | ~$5 | ~$25 | ~$100 |
| VNet / NSG | Free | Free | Free |
| **Total (est.)** | **~$185** | **~$655** | **~$2,635** |

> **Cost Savings Tip:** Stop the SSIS IR when not actively running packages. In dev, running the IR only 2-3 hours/day can reduce IR costs by ~75%.

---

## Next Steps

1. **Migrate SSIS Packages** — Use the [Data Migration Assistant](https://learn.microsoft.com/en-us/sql/dma/dma-overview) to assess and migrate existing packages
2. **Set Up Monitoring** — Configure ADF diagnostic logs to Azure Monitor / Log Analytics
3. **Implement Data Quality** — Add data validation activities to the ADF pipelines
4. **Scale** — Adjust IR node count and SQL SKUs based on actual workload
