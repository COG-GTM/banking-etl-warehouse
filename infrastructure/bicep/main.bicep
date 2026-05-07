// ============================================================================
// Azure Data Factory Infrastructure — Bicep Template
// Project: Banking ETL/Data Warehouse Migration
// Provisions: ADF, Azure SQL Database, Azure-SSIS IR, Linked Services,
//             VNet integration placeholder, and diagnostic settings.
// ============================================================================

targetScope = 'resourceGroup'

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Base name prefix used for naming resources.')
@minLength(3)
@maxLength(20)
param namePrefix string = 'bankdwh'

@description('Environment identifier (dev, staging, prod).')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('SQL Server administrator login.')
param sqlAdminLogin string

@secure()
@description('SQL Server administrator password.')
param sqlAdminPassword string

@description('SKU name for Azure SQL Database.')
@allowed(['Basic', 'S0', 'S1', 'S2', 'S3', 'P1', 'P2'])
param sqlSkuName string = 'S1'

@description('Azure-SSIS IR node size.')
param ssisNodeSize string = 'Standard_D2_v3'

@description('Azure-SSIS IR node count.')
@minValue(1)
@maxValue(10)
param ssisNodeCount int = 1

@description('Azure-SSIS IR max parallel executions per node.')
@minValue(1)
@maxValue(16)
param ssisMaxParallelExecutions int = 4

@description('Azure-SSIS IR edition.')
@allowed(['Standard', 'Enterprise'])
param ssisEdition string = 'Standard'

@description('VNet resource ID for SSIS IR integration (leave empty to skip).')
param vnetResourceId string = ''

@description('Subnet name within the VNet for SSIS IR.')
param subnetName string = 'default'

@description('Azure Blob Storage connection string for staging.')
@secure()
param blobStorageConnectionString string = ''

@description('On-premises SQL Server connection string (via Self-Hosted IR).')
@secure()
param onPremSqlConnectionString string = ''

@description('Log Analytics Workspace ID for diagnostics (leave empty to skip).')
param logAnalyticsWorkspaceId string = ''

@description('Tags applied to all resources.')
param tags object = {
  project: 'banking-etl-warehouse'
  environment: environment
  managedBy: 'bicep'
}

// ---------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------

var uniqueSuffix = uniqueString(resourceGroup().id)
var dataFactoryName = '${namePrefix}-adf-${environment}-${uniqueSuffix}'
var sqlServerName = '${namePrefix}-sql-${environment}-${uniqueSuffix}'
var sqlDatabaseName = '${namePrefix}-dwh-${environment}'
var ssisDbName = 'SSISDB'

// ---------------------------------------------------------------------------
// Azure SQL Server
// ---------------------------------------------------------------------------

resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: location
  tags: tags
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource sqlFirewallAllowAzure 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAllAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// ---------------------------------------------------------------------------
// Azure SQL Database — Data Warehouse
// ---------------------------------------------------------------------------

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: sqlDatabaseName
  location: location
  tags: tags
  sku: {
    name: sqlSkuName
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 268435456000 // ~250 GB
    zoneRedundant: false
  }
}

// ---------------------------------------------------------------------------
// Azure SQL Database — SSISDB Catalog
// ---------------------------------------------------------------------------

resource ssisDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: ssisDbName
  location: location
  tags: tags
  sku: {
    name: sqlSkuName
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 268435456000
    zoneRedundant: false
  }
}

// ---------------------------------------------------------------------------
// Azure Data Factory
// ---------------------------------------------------------------------------

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// ---------------------------------------------------------------------------
// Azure-SSIS Integration Runtime
// ---------------------------------------------------------------------------

var ssisVnetConfig = empty(vnetResourceId) ? {} : {
  vNetId: vnetResourceId
  subnet: subnetName
}

resource ssisIntegrationRuntime 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  parent: dataFactory
  name: 'IR-AzureSSIS'
  properties: {
    type: 'Managed'
    typeProperties: {
      computeProperties: {
        location: location
        nodeSize: ssisNodeSize
        numberOfNodes: ssisNodeCount
        maxParallelExecutionsPerNode: ssisMaxParallelExecutions
        ...(empty(vnetResourceId) ? {} : {
          vNetProperties: {
            vNetId: vnetResourceId
            subnet: subnetName
          }
        })
      }
      ssisProperties: {
        catalogInfo: {
          catalogServerEndpoint: sqlServer.properties.fullyQualifiedDomainName
          catalogDatabaseName: ssisDbName
          catalogAdminUserName: sqlAdminLogin
          catalogAdminPassword: {
            type: 'SecureString'
            value: sqlAdminPassword
          }
          catalogPricingTier: sqlSkuName
        }
        edition: ssisEdition
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Linked Service — Azure SQL Database (DWH target)
// ---------------------------------------------------------------------------

resource linkedServiceAzureSql 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'ls-AzureSqlDatabase'
  properties: {
    type: 'AzureSqlDatabase'
    typeProperties: {
      connectionString: 'Server=tcp:${sqlServer.properties.fullyQualifiedDomainName},1433;Initial Catalog=${sqlDatabaseName};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=Active Directory Default'
    }
    connectVia: {
      referenceName: 'AutoResolveIntegrationRuntime'
      type: 'IntegrationRuntimeReference'
    }
  }
}

// ---------------------------------------------------------------------------
// Linked Service — Azure Blob Storage (CSV/Excel staging)
// ---------------------------------------------------------------------------

resource linkedServiceBlobStorage 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = if (!empty(blobStorageConnectionString)) {
  parent: dataFactory
  name: 'ls-AzureBlobStorage'
  properties: {
    type: 'AzureBlobStorage'
    typeProperties: {
      connectionString: blobStorageConnectionString
    }
  }
}

// ---------------------------------------------------------------------------
// Linked Service — On-Premises SQL Server (via Self-Hosted IR)
// ---------------------------------------------------------------------------

resource selfHostedIR 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  parent: dataFactory
  name: 'IR-SelfHosted'
  properties: {
    type: 'SelfHosted'
    typeProperties: {}
  }
}

resource linkedServiceOnPremSql 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = if (!empty(onPremSqlConnectionString)) {
  parent: dataFactory
  name: 'ls-OnPremSqlServer'
  properties: {
    type: 'SqlServer'
    typeProperties: {
      connectionString: onPremSqlConnectionString
    }
    connectVia: {
      referenceName: selfHostedIR.name
      type: 'IntegrationRuntimeReference'
    }
  }
}

// ---------------------------------------------------------------------------
// Diagnostic Settings — Azure Data Factory
// ---------------------------------------------------------------------------

resource adfDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(logAnalyticsWorkspaceId)) {
  scope: dataFactory
  name: '${dataFactoryName}-diagnostics'
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: 30
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: 30
        }
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// Diagnostic Settings — Azure SQL Database
// ---------------------------------------------------------------------------

resource sqlDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(logAnalyticsWorkspaceId)) {
  scope: sqlDatabase
  name: '${sqlDatabaseName}-diagnostics'
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: 30
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: 30
        }
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

@description('The name of the deployed Data Factory.')
output dataFactoryName string = dataFactory.name

@description('The resource ID of the Data Factory.')
output dataFactoryId string = dataFactory.id

@description('The Data Factory principal ID (managed identity).')
output dataFactoryPrincipalId string = dataFactory.identity.principalId

@description('The fully qualified domain name of the SQL Server.')
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName

@description('The name of the DWH database.')
output sqlDatabaseName string = sqlDatabase.name

@description('The name of the SSISDB catalog database.')
output ssisDatabaseName string = ssisDatabase.name

@description('Azure-SSIS Integration Runtime name.')
output ssisIRName string = ssisIntegrationRuntime.name

@description('Self-Hosted Integration Runtime name.')
output selfHostedIRName string = selfHostedIR.name

@description('DWH connection string (without credentials).')
output dwhConnectionString string = 'Server=tcp:${sqlServer.properties.fullyQualifiedDomainName},1433;Database=${sqlDatabaseName};Encrypt=True;TrustServerCertificate=False;'
