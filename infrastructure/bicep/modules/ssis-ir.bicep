// ============================================================================
// Module: ssis-ir.bicep
// Description: Provisions the Azure-SSIS Integration Runtime within an
//              existing Azure Data Factory. Configures SSISDB catalog
//              connection and VNet injection for network-level access
//              to on-prem or Azure SQL data sources.
// ============================================================================

@description('Azure region for the Integration Runtime')
param location string

@description('Name of the existing Azure Data Factory')
param dataFactoryName string

@description('FQDN of the Azure SQL Server hosting SSISDB')
param catalogServerEndpoint string

@description('Administrator login for the SSISDB SQL Server')
param catalogAdminLogin string

@description('Administrator password for the SSISDB SQL Server')
@secure()
param catalogAdminPassword string

@description('Pricing tier for SSIS IR nodes')
@allowed(['Standard_D2_v3', 'Standard_D4_v3', 'Standard_D8_v3', 'Standard_A4_v2', 'Standard_A8_v2'])
param nodeSize string = 'Standard_D2_v3'

@description('Number of nodes in the IR cluster (1-10)')
@minValue(1)
@maxValue(10)
param numberOfNodes int = 1

@description('Maximum parallel executions per node')
@minValue(1)
@maxValue(16)
param maxParallelExecutionsPerNode int = 4

@description('Resource ID of the VNet subnet for IR injection')
param subnetId string

@description('Environment tag (dev, staging, prod)')
param environment string

// ---------------------------------------------------------------------------
// Reference the existing Data Factory
// ---------------------------------------------------------------------------
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: dataFactoryName
}

// ---------------------------------------------------------------------------
// Azure-SSIS Integration Runtime
// ---------------------------------------------------------------------------
resource ssisIr 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  parent: dataFactory
  name: 'AzureSSIS-IR'
  properties: {
    type: 'Managed'
    description: 'Azure-SSIS IR for banking ETL warehouse — runs migrated SSIS packages'
    managedVirtualNetwork: null
    typeProperties: {
      computeProperties: {
        location: location
        nodeSize: nodeSize
        numberOfNodes: numberOfNodes
        maxParallelExecutionsPerNode: maxParallelExecutionsPerNode
        vNetProperties: {
          vNetId: split(subnetId, '/subnets/')[0]
          subnet: split(subnetId, '/subnets/')[1]
        }
      }
      ssisProperties: {
        catalogInfo: {
          catalogServerEndpoint: catalogServerEndpoint
          catalogAdminUserName: catalogAdminLogin
          catalogAdminPassword: {
            type: 'SecureString'
            value: catalogAdminPassword
          }
          catalogPricingTier: 'S2'
        }
        edition: 'Standard'
        licenseType: 'LicenseIncluded'
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output irName string = ssisIr.name
output irId string = ssisIr.id
