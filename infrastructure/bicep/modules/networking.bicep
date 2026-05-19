// ============================================================================
// Module: networking.bicep
// Description: Provisions VNet, subnet for Azure-SSIS IR, and NSG rules.
//              The subnet is delegated for Azure Data Factory integration
//              runtime injection and configured with required service endpoints.
// ============================================================================

@description('Azure region for all networking resources')
param location string

@description('Base name prefix for resource naming')
param namePrefix string

@description('Environment tag (dev, staging, prod)')
param environment string

@description('Address space for the virtual network (CIDR)')
param vnetAddressPrefix string = '10.0.0.0/16'

@description('Address range for the SSIS Integration Runtime subnet (CIDR)')
param ssisSubnetPrefix string = '10.0.1.0/24'

// ---------------------------------------------------------------------------
// Network Security Group — controls inbound/outbound traffic for the IR subnet
// ---------------------------------------------------------------------------
resource nsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${namePrefix}-ssis-nsg'
  location: location
  tags: {
    environment: environment
    project: 'banking-etl-warehouse'
    component: 'networking'
  }
  properties: {
    securityRules: [
      {
        // Azure Data Factory management traffic (required for IR provisioning)
        name: 'AllowAzureDataFactoryManagement'
        properties: {
          priority: 100
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '29876-29877'
          sourceAddressPrefix: 'DataFactoryManagement'
          destinationAddressPrefix: 'VirtualNetwork'
        }
      }
      {
        // Azure SQL connectivity for SSISDB catalog
        name: 'AllowAzureSqlOutbound'
        properties: {
          priority: 100
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '1433'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'Sql'
        }
      }
      {
        // Azure Storage access for package/log storage
        name: 'AllowStorageOutbound'
        properties: {
          priority: 110
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'Storage'
        }
      }
      {
        // HTTPS outbound for Azure management APIs and package downloads
        name: 'AllowHttpsOutbound'
        properties: {
          priority: 120
          direction: 'Outbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: 'VirtualNetwork'
          destinationAddressPrefix: 'AzureCloud'
        }
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// Virtual Network — hosts the SSIS IR subnet
// ---------------------------------------------------------------------------
resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: '${namePrefix}-vnet'
  location: location
  tags: {
    environment: environment
    project: 'banking-etl-warehouse'
    component: 'networking'
  }
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefix
      ]
    }
    subnets: [
      {
        name: 'ssis-ir-subnet'
        properties: {
          addressPrefix: ssisSubnetPrefix
          networkSecurityGroup: {
            id: nsg.id
          }
          serviceEndpoints: [
            { service: 'Microsoft.Sql' }
            { service: 'Microsoft.Storage' }
          ]
          // Note: Do NOT set delegation here — Azure-SSIS IR does not use
          // formal subnet delegation. ADF injects VMs directly into the subnet.
        }
      }
    ]
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output vnetId string = vnet.id
output vnetName string = vnet.name
output ssisSubnetId string = vnet.properties.subnets[0].id
output ssisSubnetName string = vnet.properties.subnets[0].name
output nsgId string = nsg.id
