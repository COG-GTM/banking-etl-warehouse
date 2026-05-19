// ============================================================================
// Parameter file: staging.bicepparam
// Environment: Staging
// Description: Mid-tier SKUs, multi-node SSIS IR, no Git integration.
//              Mirrors prod configuration at reduced scale for validation.
// ============================================================================
using '../bicep/main.bicep'

// -- Region & naming --------------------------------------------------------
param location = 'eastus'                          // Customize: match prod region
param namePrefix = 'bankingdwh-stg'                // Customize: resource name prefix
param environment = 'staging'

// -- SQL Server credentials -------------------------------------------------
param sqlAdminLogin = 'sqladmin'                   // Customize: SQL admin username
param sqlAdminPassword = ''                        // REQUIRED: pass via CLI --parameters

// -- Compute sizing (staging: moderate) -------------------------------------
param ssisdbSkuName = 'S2'
param dwhSkuName = 'S2'
param ssisIrNodeSize = 'Standard_D4_v3'
param ssisIrNodeCount = 2
param ssisIrMaxParallel = 4

// -- Networking -------------------------------------------------------------
param vnetAddressPrefix = '10.1.0.0/16'
param ssisSubnetPrefix = '10.1.1.0/24'

// -- ADF Git integration (disabled — deploy via CI/CD ARM export) -----------
param enableGitIntegration = false
param gitAccountName = ''
param gitRepositoryName = ''

// -- Source data storage ----------------------------------------------------
param storageAccountUrl = 'https://bankingdwhstg.blob.core.windows.net' // Customize: your storage account
