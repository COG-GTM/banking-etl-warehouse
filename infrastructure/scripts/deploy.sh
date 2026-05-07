#!/usr/bin/env bash
# ============================================================================
# Azure Data Factory Infrastructure Deployment Script
# Project: Banking ETL/Data Warehouse Migration
#
# Usage:
#   ./deploy.sh [--template bicep|arm] [--environment dev|staging|prod]
#                [--resource-group <name>] [--location <region>]
#                [--validate-only] [--what-if]
# ============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Default Configuration
# ---------------------------------------------------------------------------
TEMPLATE_TYPE="bicep"
ENVIRONMENT="dev"
RESOURCE_GROUP=""
LOCATION="eastus2"
NAME_PREFIX="bankdwh"
VALIDATE_ONLY=false
WHAT_IF=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------------------------------------------------------------------------
# Color output helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC}   $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Deploy Azure Data Factory infrastructure for the Banking ETL/DWH project.

Options:
  --template TYPE        Template type: bicep (default) or arm
  --environment ENV      Target environment: dev (default), staging, or prod
  --resource-group NAME  Resource group name (default: rg-\${NAME_PREFIX}-\${ENV})
  --location REGION      Azure region (default: eastus2)
  --name-prefix PREFIX   Naming prefix (default: bankdwh)
  --validate-only        Only validate the template, do not deploy
  --what-if              Preview changes without deploying (Bicep only)
  -h, --help             Show this help message

Examples:
  $(basename "$0") --template bicep --environment dev
  $(basename "$0") --template arm --environment prod --location westus2
  $(basename "$0") --validate-only
EOF
    exit 0
}

# ---------------------------------------------------------------------------
# Parse Arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --template)       TEMPLATE_TYPE="$2"; shift 2 ;;
        --environment)    ENVIRONMENT="$2"; shift 2 ;;
        --resource-group) RESOURCE_GROUP="$2"; shift 2 ;;
        --location)       LOCATION="$2"; shift 2 ;;
        --name-prefix)    NAME_PREFIX="$2"; shift 2 ;;
        --validate-only)  VALIDATE_ONLY=true; shift ;;
        --what-if)        WHAT_IF=true; shift ;;
        -h|--help)        usage ;;
        *)                log_error "Unknown option: $1"; usage ;;
    esac
done

# Default resource group name
if [[ -z "$RESOURCE_GROUP" ]]; then
    RESOURCE_GROUP="rg-${NAME_PREFIX}-${ENVIRONMENT}"
fi

DEPLOYMENT_NAME="deploy-${NAME_PREFIX}-$(date +%Y%m%d%H%M%S)"

# ---------------------------------------------------------------------------
# Prerequisites Check
# ---------------------------------------------------------------------------
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v az &> /dev/null; then
        log_error "Azure CLI (az) is not installed. Install from https://aka.ms/install-azure-cli"
        exit 1
    fi

    local az_account
    az_account=$(az account show --query "name" -o tsv 2>/dev/null || true)
    if [[ -z "$az_account" ]]; then
        log_error "Not logged in to Azure CLI. Run 'az login' first."
        exit 1
    fi
    log_success "Azure CLI authenticated — subscription: ${az_account}"

    if [[ "$TEMPLATE_TYPE" == "bicep" ]]; then
        if ! az bicep version &> /dev/null; then
            log_warn "Bicep CLI not found. Installing..."
            az bicep install
        fi
        log_success "Bicep CLI available"
    fi
}

# ---------------------------------------------------------------------------
# Create Resource Group
# ---------------------------------------------------------------------------
create_resource_group() {
    log_info "Ensuring resource group '${RESOURCE_GROUP}' exists in '${LOCATION}'..."

    if az group show --name "$RESOURCE_GROUP" &> /dev/null; then
        log_success "Resource group '${RESOURCE_GROUP}' already exists."
    else
        az group create \
            --name "$RESOURCE_GROUP" \
            --location "$LOCATION" \
            --tags project=banking-etl-warehouse environment="$ENVIRONMENT" managedBy=script \
            --output none
        log_success "Resource group '${RESOURCE_GROUP}' created."
    fi
}

# ---------------------------------------------------------------------------
# Prompt for Secure Parameters
# ---------------------------------------------------------------------------
prompt_secure_params() {
    if [[ -z "${SQL_ADMIN_LOGIN:-}" ]]; then
        read -rp "SQL Admin Login [sqladmin]: " SQL_ADMIN_LOGIN
        SQL_ADMIN_LOGIN="${SQL_ADMIN_LOGIN:-sqladmin}"
    fi

    if [[ -z "${SQL_ADMIN_PASSWORD:-}" ]]; then
        read -rsp "SQL Admin Password: " SQL_ADMIN_PASSWORD
        echo
        if [[ -z "$SQL_ADMIN_PASSWORD" ]]; then
            log_error "SQL Admin Password cannot be empty."
            exit 1
        fi
    fi
}

# ---------------------------------------------------------------------------
# Deploy Bicep Template
# ---------------------------------------------------------------------------
deploy_bicep() {
    local bicep_file="${INFRA_DIR}/bicep/main.bicep"
    local params_file="${INFRA_DIR}/bicep/parameters.json"

    if [[ ! -f "$bicep_file" ]]; then
        log_error "Bicep file not found: ${bicep_file}"
        exit 1
    fi

    log_info "Deploying Bicep template..."
    log_info "  Template:       ${bicep_file}"
    log_info "  Resource Group: ${RESOURCE_GROUP}"
    log_info "  Environment:    ${ENVIRONMENT}"
    log_info "  Deployment:     ${DEPLOYMENT_NAME}"

    local base_params=(
        --resource-group "$RESOURCE_GROUP"
        --template-file "$bicep_file"
        --name "$DEPLOYMENT_NAME"
        --parameters
            namePrefix="$NAME_PREFIX"
            environment="$ENVIRONMENT"
            sqlAdminLogin="$SQL_ADMIN_LOGIN"
            sqlAdminPassword="$SQL_ADMIN_PASSWORD"
    )

    if [[ "$VALIDATE_ONLY" == true ]]; then
        log_info "Running validation only..."
        az deployment group validate "${base_params[@]}" --output table
        log_success "Template validation passed."
        return
    fi

    if [[ "$WHAT_IF" == true ]]; then
        log_info "Running what-if analysis..."
        az deployment group what-if "${base_params[@]}" --output table
        return
    fi

    az deployment group create "${base_params[@]}" --output table

    log_success "Bicep deployment completed."
}

# ---------------------------------------------------------------------------
# Deploy ARM Template
# ---------------------------------------------------------------------------
deploy_arm() {
    local arm_file="${INFRA_DIR}/arm/azuredeploy.json"

    if [[ ! -f "$arm_file" ]]; then
        log_error "ARM template not found: ${arm_file}"
        exit 1
    fi

    log_info "Deploying ARM template..."
    log_info "  Template:       ${arm_file}"
    log_info "  Resource Group: ${RESOURCE_GROUP}"
    log_info "  Environment:    ${ENVIRONMENT}"
    log_info "  Deployment:     ${DEPLOYMENT_NAME}"

    local base_params=(
        --resource-group "$RESOURCE_GROUP"
        --template-file "$arm_file"
        --name "$DEPLOYMENT_NAME"
        --parameters
            namePrefix="$NAME_PREFIX"
            environment="$ENVIRONMENT"
            sqlAdminLogin="$SQL_ADMIN_LOGIN"
            sqlAdminPassword="$SQL_ADMIN_PASSWORD"
    )

    if [[ "$VALIDATE_ONLY" == true ]]; then
        log_info "Running validation only..."
        az deployment group validate "${base_params[@]}" --output table
        log_success "Template validation passed."
        return
    fi

    az deployment group create "${base_params[@]}" --output table

    log_success "ARM deployment completed."
}

# ---------------------------------------------------------------------------
# Output Deployment Results
# ---------------------------------------------------------------------------
print_outputs() {
    log_info "Fetching deployment outputs..."

    local outputs
    outputs=$(az deployment group show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$DEPLOYMENT_NAME" \
        --query "properties.outputs" \
        -o json 2>/dev/null || echo "{}")

    if [[ "$outputs" == "{}" ]]; then
        log_warn "No deployment outputs available."
        return
    fi

    echo ""
    echo "============================================================================"
    echo "  DEPLOYMENT OUTPUTS"
    echo "============================================================================"

    local adf_name sql_fqdn dwh_name ssis_db conn_str adf_id shir_name ssis_ir_name

    adf_name=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('dataFactoryName',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    adf_id=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('dataFactoryId',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    sql_fqdn=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('sqlServerFqdn',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    dwh_name=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('sqlDatabaseName',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    ssis_db=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ssisDatabaseName',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    ssis_ir_name=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ssisIRName',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    shir_name=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('selfHostedIRName',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")
    conn_str=$(echo "$outputs" | python3 -c "import sys,json; print(json.load(sys.stdin).get('dwhConnectionString',{}).get('value','N/A'))" 2>/dev/null || echo "N/A")

    printf "  %-30s %s\n" "Data Factory Name:" "$adf_name"
    printf "  %-30s %s\n" "Data Factory Resource ID:" "$adf_id"
    printf "  %-30s %s\n" "SQL Server FQDN:" "$sql_fqdn"
    printf "  %-30s %s\n" "DWH Database:" "$dwh_name"
    printf "  %-30s %s\n" "SSISDB Catalog:" "$ssis_db"
    printf "  %-30s %s\n" "Azure-SSIS IR:" "$ssis_ir_name"
    printf "  %-30s %s\n" "Self-Hosted IR:" "$shir_name"
    printf "  %-30s %s\n" "DWH Connection String:" "$conn_str"
    echo "============================================================================"
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    echo ""
    echo "============================================================================"
    echo "  Banking ETL/DWH — Azure Infrastructure Deployment"
    echo "  Template: ${TEMPLATE_TYPE} | Environment: ${ENVIRONMENT}"
    echo "============================================================================"
    echo ""

    check_prerequisites
    prompt_secure_params

    if [[ "$VALIDATE_ONLY" != true ]]; then
        create_resource_group
    fi

    case "$TEMPLATE_TYPE" in
        bicep) deploy_bicep ;;
        arm)   deploy_arm ;;
        *)     log_error "Invalid template type: ${TEMPLATE_TYPE}. Use 'bicep' or 'arm'."; exit 1 ;;
    esac

    if [[ "$VALIDATE_ONLY" != true && "$WHAT_IF" != true ]]; then
        print_outputs
    fi

    log_success "Done."
}

main "$@"
