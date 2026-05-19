// ============================================================================
// Parameter file: dev.bicepparam
// Environment: Development
// Description: Lower-cost SKUs, single-node SSIS IR, Git integration enabled.
// ============================================================================
using '../bicep/main.bicep'

// -- Region & naming --------------------------------------------------------
param location = 'eastus'                          // Customize: your preferred Azure region
param namePrefix = 'bankingdwh-dev'                // Customize: resource name prefix
param environment = 'dev'

// -- SQL Server credentials -------------------------------------------------
// IMPORTANT: Override these at deploy time via --parameters or Key Vault.
// Do NOT commit real credentials.
param sqlAdminLogin = 'sqladmin'                   // Customize: SQL admin username
param sqlAdminPassword = ''                        // REQUIRED: pass via CLI --parameters

// -- Compute sizing (dev: minimal) ------------------------------------------
param ssisdbSkuName = 'S1'
param dwhSkuName = 'S1'
param ssisIrNodeSize = 'Standard_D2_v3'
param ssisIrNodeCount = 1
param ssisIrMaxParallel = 2

// -- Networking -------------------------------------------------------------
param vnetAddressPrefix = '10.0.0.0/16'
param ssisSubnetPrefix = '10.0.1.0/24'

// -- ADF Git integration (enabled for dev) ----------------------------------
param enableGitIntegration = true
param gitAccountName = 'COG-GTM'                   // Customize: your GitHub org/account
param gitRepositoryName = 'banking-etl-warehouse'  // Customize: your repo name

// -- Source data storage ----------------------------------------------------
param storageAccountUrl = 'https://bankingdwhdev.blob.core.windows.net' // Customize: your storage account
