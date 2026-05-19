// ============================================================================
// Parameter file: prod.bicepparam
// Environment: Production
// Description: Production-grade SKUs, multi-node SSIS IR with higher
//              parallelism, no Git integration (deployed via CI/CD).
// ============================================================================
using '../bicep/main.bicep'

// -- Region & naming --------------------------------------------------------
param location = 'eastus'                          // Customize: your production region
param namePrefix = 'bankingdwh-prod'               // Customize: resource name prefix
param environment = 'prod'

// -- SQL Server credentials -------------------------------------------------
param sqlAdminLogin = 'sqladmin'                   // Customize: SQL admin username
param sqlAdminPassword = ''                        // REQUIRED: pass via CLI --parameters or Key Vault

// -- Compute sizing (prod: high performance) --------------------------------
param ssisdbSkuName = 'S3'
param dwhSkuName = 'P1'
param ssisIrNodeSize = 'Standard_D8_v3'
param ssisIrNodeCount = 4
param ssisIrMaxParallel = 8

// -- Networking -------------------------------------------------------------
param vnetAddressPrefix = '10.2.0.0/16'
param ssisSubnetPrefix = '10.2.1.0/24'

// -- ADF Git integration (disabled — deploy via CI/CD ARM export) -----------
param enableGitIntegration = false
param gitAccountName = ''
param gitRepositoryName = ''

// -- Source data storage ----------------------------------------------------
param storageAccountUrl = 'https://bankingdwhprod.blob.core.windows.net' // Customize: your storage account
