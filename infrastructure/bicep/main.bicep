// ============================================================================
// Main Deployment: Azure Data Factory + Azure-SSIS Integration Runtime
// Project: Banking ETL Warehouse — SSIS-to-ADF Migration
//
// This template orchestrates the provisioning of all Azure resources needed
// to run migrated SSIS packages in Azure Data Factory:
//   1. Virtual Network + NSG (network isolation for the SSIS IR)
//   2. Azure SQL Server + SSISDB catalog + DWH database
//   3. Azure Data Factory (with optional GitHub integration)
//   4. Azure-SSIS Integration Runtime (VNet-injected)
//   5. Linked Services (Blob Storage sources, SQL DWH target)
//
// Usage:
//   az deployment group create \
//     --resource-group <rg-name> \
//     --template-file main.bicep \
//     --parameters @../parameters/dev.bicepparam
// ============================================================================

// ---------------------------------------------------------------------------
// Target scope and parameters
// ---------------------------------------------------------------------------
targetScope = 'resourceGroup'

@description('Azure region for all resources (e.g., eastus, westeurope)')
param location string

@description('Base name prefix used for all resource names (e.g., bankingdwh-dev)')
param namePrefix string

@description('Environment identifier')
@allowed(['dev', 'staging', 'prod'])
param environment string

// SQL Server parameters
@description('SQL Server administrator login')
param sqlAdminLogin string

@description('SQL Server administrator password')
@secure()
param sqlAdminPassword string

// Compute sizing
@description('SSISDB database SKU')
@allowed(['S0', 'S1', 'S2', 'S3', 'P1', 'P2'])
param ssisdbSkuName string = 'S2'

@description('DWH database SKU')
@allowed(['S0', 'S1', 'S2', 'S3', 'P1', 'P2'])
param dwhSkuName string = 'S2'

@description('Azure-SSIS IR node size')
@allowed(['Standard_D2_v3', 'Standard_D4_v3', 'Standard_D8_v3', 'Standard_A4_v2', 'Standard_A8_v2'])
param ssisIrNodeSize string = 'Standard_D2_v3'

@description('Number of nodes for the SSIS IR cluster')
@minValue(1)
@maxValue(10)
param ssisIrNodeCount int = 1

@description('Max parallel SSIS package executions per node')
@minValue(1)
@maxValue(16)
param ssisIrMaxParallel int = 4

// Networking parameters
@description('VNet address space')
param vnetAddressPrefix string = '10.0.0.0/16'

@description('Subnet address range for SSIS IR')
param ssisSubnetPrefix string = '10.0.1.0/24'

// ADF Git integration (optional, typically dev only)
@description('Enable GitHub integration for ADF')
param enableGitIntegration bool = false

@description('GitHub account for ADF Git integration')
param gitAccountName string = ''

@description('GitHub repo for ADF Git integration')
param gitRepositoryName string = ''

// Blob Storage source
@description('Azure Blob Storage account URL for source data files')
param storageAccountUrl string = ''

// ---------------------------------------------------------------------------
// Module 1: Networking (VNet + Subnet + NSG)
// ---------------------------------------------------------------------------
module networking 'modules/networking.bicep' = {
  name: 'deploy-networking'
  params: {
    location: location
    namePrefix: namePrefix
    environment: environment
    vnetAddressPrefix: vnetAddressPrefix
    ssisSubnetPrefix: ssisSubnetPrefix
  }
}

// ---------------------------------------------------------------------------
// Module 2: Azure SQL Server + SSISDB + DWH databases
// ---------------------------------------------------------------------------
module sql 'modules/sql.bicep' = {
  name: 'deploy-sql'
  params: {
    location: location
    namePrefix: namePrefix
    environment: environment
    sqlAdminLogin: sqlAdminLogin
    sqlAdminPassword: sqlAdminPassword
    ssisdbSkuName: ssisdbSkuName
    dwhSkuName: dwhSkuName
    ssisSubnetId: networking.outputs.ssisSubnetId
  }
}

// ---------------------------------------------------------------------------
// Module 3: Azure Data Factory
// ---------------------------------------------------------------------------
module adf 'modules/adf.bicep' = {
  name: 'deploy-adf'
  params: {
    location: location
    namePrefix: namePrefix
    environment: environment
    enableGitIntegration: enableGitIntegration
    gitAccountName: gitAccountName
    gitRepositoryName: gitRepositoryName
  }
}

// ---------------------------------------------------------------------------
// Module 4: Azure-SSIS Integration Runtime
// ---------------------------------------------------------------------------
module ssisIr 'modules/ssis-ir.bicep' = {
  name: 'deploy-ssis-ir'
  params: {
    location: location
    dataFactoryName: adf.outputs.dataFactoryName
    catalogServerEndpoint: sql.outputs.sqlServerFqdn
    catalogAdminLogin: sqlAdminLogin
    catalogAdminPassword: sqlAdminPassword
    nodeSize: ssisIrNodeSize
    numberOfNodes: ssisIrNodeCount
    maxParallelExecutionsPerNode: ssisIrMaxParallel
    subnetId: networking.outputs.ssisSubnetId
    environment: environment
  }
}

// ---------------------------------------------------------------------------
// Module 5: Linked Services
// ---------------------------------------------------------------------------
module linkedServices 'modules/linked-services.bicep' = {
  name: 'deploy-linked-services'
  params: {
    dataFactoryName: adf.outputs.dataFactoryName
    dwhConnectionString: 'Server=tcp:${sql.outputs.sqlServerFqdn},1433;Initial Catalog=${sql.outputs.dwhDbName};Persist Security Info=False;User ID=${sqlAdminLogin};Password=${sqlAdminPassword};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;'
    storageAccountUrl: storageAccountUrl
    environment: environment
  }
}

// ---------------------------------------------------------------------------
// Outputs — used by CI/CD pipelines and SETUP_GUIDE validation steps
// ---------------------------------------------------------------------------
output resourceGroupLocation string = location
output vnetName string = networking.outputs.vnetName
output ssisSubnetName string = networking.outputs.ssisSubnetName
output sqlServerFqdn string = sql.outputs.sqlServerFqdn
output ssisdbName string = sql.outputs.ssisdbName
output dwhDbName string = sql.outputs.dwhDbName
output dataFactoryName string = adf.outputs.dataFactoryName
output ssisIrName string = ssisIr.outputs.irName
