// ============================================================================
// Module: sql.bicep
// Description: Provisions an Azure SQL Server and database to host the SSISDB
//              catalog. Also creates the DWH database that mirrors the on-prem
//              SQL Server Data Warehouse from this project.
// ============================================================================

@description('Azure region for SQL resources')
param location string

@description('Base name prefix for resource naming')
param namePrefix string

@description('Environment tag (dev, staging, prod)')
param environment string

@description('SQL Server administrator login name')
param sqlAdminLogin string

@description('SQL Server administrator password')
@secure()
param sqlAdminPassword string

@description('SSISDB pricing tier — use S1/S2 for dev, S3+ for prod')
@allowed(['S0', 'S1', 'S2', 'S3', 'P1', 'P2'])
param ssisdbSkuName string = 'S2'

@description('DWH database pricing tier')
@allowed(['S0', 'S1', 'S2', 'S3', 'P1', 'P2'])
param dwhSkuName string = 'S2'

@description('Subnet ID to allow through the SQL Server firewall (VNet rule)')
param ssisSubnetId string

// ---------------------------------------------------------------------------
// Azure SQL Server
// ---------------------------------------------------------------------------
resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: '${namePrefix}-sqlserver'
  location: location
  tags: {
    environment: environment
    project: 'banking-etl-warehouse'
    component: 'database'
  }
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

// ---------------------------------------------------------------------------
// Firewall: Allow Azure services (e.g., ADF, SSIS IR) to reach SQL Server
// ---------------------------------------------------------------------------
resource firewallAllowAzure 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAllAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// ---------------------------------------------------------------------------
// VNet Rule: Allow traffic from the SSIS IR subnet
// ---------------------------------------------------------------------------
resource vnetRule 'Microsoft.Sql/servers/virtualNetworkRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'allow-ssis-ir-subnet'
  properties: {
    virtualNetworkSubnetId: ssisSubnetId
    ignoreMissingVnetServiceEndpoint: false
  }
}

// ---------------------------------------------------------------------------
// SSISDB Catalog Database — required by Azure-SSIS Integration Runtime
// ---------------------------------------------------------------------------
resource ssisdb 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: 'SSISDB'
  location: location
  tags: {
    environment: environment
    project: 'banking-etl-warehouse'
    component: 'ssisdb-catalog'
  }
  sku: {
    name: ssisdbSkuName
    tier: 'Standard'
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 268435456000 // ~250 GB
  }
}

// ---------------------------------------------------------------------------
// DWH Database — mirrors the on-prem Star Schema warehouse
// ---------------------------------------------------------------------------
resource dwhDb 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: 'DWH'
  location: location
  tags: {
    environment: environment
    project: 'banking-etl-warehouse'
    component: 'data-warehouse'
  }
  sku: {
    name: dwhSkuName
    tier: 'Standard'
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 268435456000
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output sqlServerName string = sqlServer.name
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output ssisdbName string = ssisdb.name
output dwhDbName string = dwhDb.name
output sqlServerId string = sqlServer.id
