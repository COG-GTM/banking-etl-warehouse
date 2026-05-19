// ============================================================================
// Module: linked-services.bicep
// Description: Creates ADF Linked Services for source and target data stores
//              used by the banking ETL warehouse pipelines.
//
//              Sources: Azure Blob Storage (CSV/Excel files)
//              Target:  Azure SQL Database (DWH — Star Schema)
// ============================================================================

@description('Name of the existing Azure Data Factory')
param dataFactoryName string

@description('Connection string for the DWH Azure SQL Database')
@secure()
param dwhConnectionString string

@description('Azure Blob Storage account URL (https://<account>.blob.core.windows.net)')
param storageAccountUrl string

@description('Environment tag (dev, staging, prod)')
param environment string

// ---------------------------------------------------------------------------
// Reference the existing Data Factory
// ---------------------------------------------------------------------------
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: dataFactoryName
}

// ---------------------------------------------------------------------------
// Linked Service: Azure SQL Database (DWH target)
// ---------------------------------------------------------------------------
resource sqlLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'AzureSqlDWH'
  properties: {
    type: 'AzureSqlDatabase'
    description: 'Connection to the DWH database (Star Schema: DimCustomer, DimAccount, DimBranch, FactTransaction)'
    typeProperties: {
      connectionString: dwhConnectionString
    }
    annotations: [
      environment
      'target-dwh'
    ]
  }
}

// ---------------------------------------------------------------------------
// Linked Service: Azure Blob Storage (source files — CSV, Excel)
// ---------------------------------------------------------------------------
resource blobLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'AzureBlobSources'
  properties: {
    type: 'AzureBlobStorage'
    description: 'Blob storage hosting CSV and Excel source files (transaction_csv.csv, transaction_excel.xlsx)'
    typeProperties: {
      serviceUri: storageAccountUrl
      // Uses ADF Managed Identity for authentication — grant
      // "Storage Blob Data Reader" role to the ADF identity
    }
    annotations: [
      environment
      'source-files'
    ]
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output sqlLinkedServiceName string = sqlLinkedService.name
output blobLinkedServiceName string = blobLinkedService.name
