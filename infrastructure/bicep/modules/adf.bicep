// ============================================================================
// Module: adf.bicep
// Description: Provisions Azure Data Factory with managed identity and
//              optional Git integration for source-controlled pipelines.
// ============================================================================

@description('Azure region for the Data Factory')
param location string

@description('Base name prefix for resource naming')
param namePrefix string

@description('Environment tag (dev, staging, prod)')
param environment string

@description('Enable Git integration (set to true for dev, false for higher envs)')
param enableGitIntegration bool = false

@description('GitHub account name for ADF Git integration')
param gitAccountName string = ''

@description('GitHub repository name for ADF Git integration')
param gitRepositoryName string = ''

@description('Git collaboration branch (typically main)')
param gitCollaborationBranch string = 'main'

@description('Root folder in the repo for ADF artifacts')
param gitRootFolder string = '/infrastructure/adf'

// ---------------------------------------------------------------------------
// Azure Data Factory
// ---------------------------------------------------------------------------
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: '${namePrefix}-adf'
  location: location
  tags: {
    environment: environment
    project: 'banking-etl-warehouse'
    component: 'data-factory'
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
    // Git integration — only configure in the dev environment where authors
    // publish changes. Higher environments deploy via CI/CD ARM export.
    repoConfiguration: enableGitIntegration ? {
      type: 'FactoryGitHubConfiguration'
      accountName: gitAccountName
      repositoryName: gitRepositoryName
      collaborationBranch: gitCollaborationBranch
      rootFolder: gitRootFolder
      disablePublish: false
    } : null
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output dataFactoryName string = dataFactory.name
output dataFactoryId string = dataFactory.id
output dataFactoryPrincipalId string = dataFactory.identity.principalId
