# Azure Data Factory Infrastructure — Setup Guide

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Deployment Steps](#deployment-steps)
3. [Post-Deployment Validation Checklist](#post-deployment-validation-checklist)
4. [Network Connectivity Validation](#network-connectivity-validation)
5. [CI/CD Pipeline Scaffolding Overview](#cicd-pipeline-scaffolding-overview)

---

## Prerequisites

### Azure Subscription & Permissions

- An active Azure subscription with billing enabled.
- The deploying user/service principal must have:
  - **Contributor** role on the target resource group (or subscription-level for resource group creation).
  - **User Access Administrator** role if configuring RBAC for the ADF managed identity.
  - **Key Vault Secrets Officer** role if using Azure Key Vault for parameter references.

### Required Tools

| Tool | Minimum Version | Installation |
|------|----------------|--------------|
| Azure CLI | 2.50+ | [Install Azure CLI](https://aka.ms/install-azure-cli) |
| Bicep CLI | 0.20+ | `az bicep install` (bundled with Azure CLI) |
| Git | 2.30+ | [Install Git](https://git-scm.com/downloads) |
| SQL Server Management Studio (SSMS) | 19+ | [Download SSMS](https://aka.ms/ssmsfullsetup) |

### Networking

- If VNet integration is required for the Azure-SSIS IR:
  - A VNet with a dedicated subnet (minimum /26 CIDR, 64 addresses).
  - The subnet must NOT have any existing service delegations.
  - NSG rules allowing outbound access to Azure SQL, Azure Storage, and Azure Service Bus.
- For Self-Hosted IR:
  - Outbound HTTPS (port 443) from the on-premises machine to `*.servicebus.windows.net`.
  - Network access from the on-premises IR machine to the source SQL Server.

### Source Data Inventory

The following source data files must be uploaded to Azure Blob Storage before running ETL pipelines:

| File | Format | Description |
|------|--------|-------------|
| `sample.bak` | SQL Server Backup | Source operational database |
| `transaction_csv.csv` | CSV | Transaction records from flat-file system |
| `transaction_excel.xlsx` | Excel (OOXML) | Transaction records from spreadsheet system |

---

## Deployment Steps

### Step 1: Authenticate with Azure

```bash
az login
az account set --subscription "<YOUR_SUBSCRIPTION_ID>"
```

### Step 2: Clone the Repository

```bash
git clone https://github.com/COG-GTM/banking-etl-warehouse.git
cd banking-etl-warehouse
```

### Step 3: Configure Parameters

Edit `infrastructure/bicep/parameters.json`:

1. Replace `<SUBSCRIPTION_ID>` with your Azure subscription ID.
2. Replace `<KEYVAULT_RG>` and `<KEYVAULT_NAME>` with your Key Vault details.
3. Ensure the following secrets exist in Key Vault:
   - `sql-admin-password` — SQL Server administrator password.
   - `blob-storage-connection-string` — Azure Blob Storage connection string.
   - `onprem-sql-connection-string` — On-premises SQL Server connection string.

Alternatively, pass parameters directly via the deployment script (passwords will be prompted interactively).

### Step 4: Deploy Using the Script

```bash
# Make the script executable
chmod +x infrastructure/scripts/deploy.sh

# Deploy with Bicep (default)
./infrastructure/scripts/deploy.sh \
  --template bicep \
  --environment dev \
  --location eastus2

# Or deploy with ARM template
./infrastructure/scripts/deploy.sh \
  --template arm \
  --environment dev

# Validate only (no actual deployment)
./infrastructure/scripts/deploy.sh --validate-only

# Preview changes (what-if)
./infrastructure/scripts/deploy.sh --what-if
```

### Step 5: Configure Self-Hosted Integration Runtime

1. Navigate to **Azure Portal > Data Factory > Manage > Integration Runtimes**.
2. Select **IR-SelfHosted** and click **Option 1: Express setup** to download the installer.
3. Install the Self-Hosted IR on a Windows machine within the on-premises network.
4. Register the IR using the authentication key displayed in the portal.
5. Verify the IR status shows **Running** in the ADF portal.

### Step 6: Start the Azure-SSIS Integration Runtime

```bash
# Start the SSIS IR (takes 20–30 minutes on first start)
az datafactory integration-runtime start \
  --resource-group "rg-bankdwh-dev" \
  --factory-name "<ADF_NAME>" \
  --name "IR-AzureSSIS"
```

### Step 7: Deploy the DWH Schema

Connect to the Azure SQL Database using SSMS or `sqlcmd` and execute the schema scripts:

```bash
# Using sqlcmd
sqlcmd -S "<SQL_SERVER_FQDN>" -d "<DWH_DATABASE_NAME>" \
  -U "<SQL_ADMIN_LOGIN>" -P "<SQL_ADMIN_PASSWORD>" \
  -i sql_scripts/01_create_tables.sql

sqlcmd -S "<SQL_SERVER_FQDN>" -d "<DWH_DATABASE_NAME>" \
  -U "<SQL_ADMIN_LOGIN>" -P "<SQL_ADMIN_PASSWORD>" \
  -i sql_scripts/02_create_procedures.sql
```

### Step 8: Upload Source Data to Blob Storage

```bash
# Create a storage container
az storage container create \
  --name "etl-staging" \
  --account-name "<STORAGE_ACCOUNT_NAME>"

# Upload source files
az storage blob upload-batch \
  --destination "etl-staging" \
  --source "data_sources/" \
  --account-name "<STORAGE_ACCOUNT_NAME>"
```

---

## Post-Deployment Validation Checklist

### Infrastructure Resources

- [ ] Resource group `rg-bankdwh-<env>` exists in the correct region.
- [ ] Azure SQL Server is provisioned with TLS 1.2 minimum.
- [ ] DWH database is created with the correct SKU and collation (`SQL_Latin1_General_CP1_CI_AS`).
- [ ] SSISDB catalog database is created.
- [ ] Azure Data Factory instance is provisioned with system-assigned managed identity.
- [ ] Azure SQL Server firewall rule allows Azure services (`0.0.0.0`).

### Integration Runtimes

- [ ] `IR-AzureSSIS` is configured and can be started successfully.
- [ ] SSISDB catalog is initialized after first IR start.
- [ ] `IR-SelfHosted` is registered and shows **Running** status.
- [ ] Self-Hosted IR can connect to the on-premises SQL Server.

### Linked Services

- [ ] `ls-AzureSqlDatabase` connects to the DWH database successfully (Test Connection in ADF portal).
- [ ] `ls-AzureBlobStorage` connects to the staging storage account.
- [ ] `ls-OnPremSqlServer` connects to the source database via Self-Hosted IR.

### Data Warehouse Schema

- [ ] All four tables exist: `DimAccount`, `DimBranch`, `DimCustomer`, `FactTransaction`.
- [ ] Foreign key constraints on `FactTransaction` are active.
- [ ] Stored procedures `sp_DailyTransaction` and `sp_BalancePerCustomer` are deployed.

### Monitoring

- [ ] Diagnostic settings are forwarding logs and metrics to Log Analytics (if configured).
- [ ] ADF activity runs are visible in the Monitor tab.

---

## Network Connectivity Validation

### Azure-SSIS IR Network Requirements

If VNet-integrated, validate:

```bash
# Verify subnet has no conflicting service delegations
az network vnet subnet show \
  --resource-group "<VNET_RG>" \
  --vnet-name "<VNET_NAME>" \
  --name "<SUBNET_NAME>" \
  --query "delegations"

# Verify NSG allows required outbound traffic
az network nsg rule list \
  --resource-group "<VNET_RG>" \
  --nsg-name "<NSG_NAME>" \
  --output table
```

Required outbound rules for Azure-SSIS IR:

| Destination | Port | Protocol | Purpose |
|------------|------|----------|---------|
| Azure SQL | 1433 | TCP | SSISDB catalog access |
| Azure Storage | 443 | TCP | Package storage and logging |
| Azure Service Bus | 443 | TCP | IR management communication |
| Azure Monitor | 443 | TCP | Diagnostic telemetry |

### Self-Hosted IR Network Requirements

From the on-premises IR machine, verify:

```powershell
# Test connectivity to Azure Service Bus
Test-NetConnection -ComputerName "*.servicebus.windows.net" -Port 443

# Test connectivity to on-premises SQL Server
Test-NetConnection -ComputerName "<ON_PREM_SQL_SERVER>" -Port 1433

# Test connectivity to Azure SQL (for hybrid scenarios)
Test-NetConnection -ComputerName "<SQL_SERVER_FQDN>" -Port 1433
```

---

## CI/CD Pipeline Scaffolding Overview

### Git Integration with Azure Data Factory

ADF supports native Git integration for version-controlling pipelines, datasets, and linked services.

#### Recommended Setup

1. **Repository**: Use this repository (`banking-etl-warehouse`) as the ADF Git source.
2. **Collaboration branch**: `main`
3. **Publish branch**: `adf_publish` (auto-generated ARM templates)
4. **Root folder**: `/infrastructure/adf/`

#### Configuring Git Integration

1. Open the ADF Studio in the Azure portal.
2. Navigate to **Manage > Git configuration**.
3. Select **GitHub** (or Azure DevOps Repos) and authenticate.
4. Configure:
   - Repository: `COG-GTM/banking-etl-warehouse`
   - Collaboration branch: `main`
   - Root folder: `/infrastructure/adf/`
   - Import existing resources: Yes

#### CI/CD Pipeline Structure

```
Feature Branch ──> Pull Request ──> main (collaboration)
                                      │
                                      ▼
                              ADF Publish ──> adf_publish branch
                                      │
                                      ▼
                              ARM Template ──> Deploy to Staging
                                      │
                                      ▼
                              Approval Gate ──> Deploy to Production
```

#### Azure DevOps Pipeline (Example)

```yaml
# azure-pipelines.yml (scaffolding)
trigger:
  branches:
    include:
      - adf_publish

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: DeployToStaging
    jobs:
      - job: Deploy
        steps:
          - task: AzureResourceManagerTemplateDeployment@3
            inputs:
              azureResourceManagerConnection: '<SERVICE_CONNECTION>'
              resourceGroupName: 'rg-bankdwh-staging'
              location: 'eastus2'
              templateLocation: 'Linked artifact'
              csmFile: '$(System.DefaultWorkingDirectory)/infrastructure/arm/azuredeploy.json'
              csmParametersFile: '$(System.DefaultWorkingDirectory)/infrastructure/bicep/parameters.json'

  - stage: DeployToProduction
    dependsOn: DeployToStaging
    condition: succeeded()
    jobs:
      - deployment: Deploy
        environment: 'production'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: AzureResourceManagerTemplateDeployment@3
                  inputs:
                    azureResourceManagerConnection: '<SERVICE_CONNECTION>'
                    resourceGroupName: 'rg-bankdwh-prod'
                    location: 'eastus2'
                    templateLocation: 'Linked artifact'
                    csmFile: '$(System.DefaultWorkingDirectory)/infrastructure/arm/azuredeploy.json'
                    csmParametersFile: '$(System.DefaultWorkingDirectory)/infrastructure/bicep/parameters.json'
```

#### GitHub Actions Pipeline (Example)

```yaml
# .github/workflows/adf-deploy.yml (scaffolding)
name: ADF Infrastructure Deploy

on:
  push:
    branches: [main]
    paths: ['infrastructure/**']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Deploy Bicep
        uses: azure/arm-deploy@v2
        with:
          resourceGroupName: rg-bankdwh-dev
          template: ./infrastructure/bicep/main.bicep
          parameters: ./infrastructure/bicep/parameters.json
```
