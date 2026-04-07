#!/usr/bin/env bash
###############################################################################
# init_databases.sh - Initialize both source and DWH databases
#
# This script automates the full database initialization pipeline:
#   1. Optionally restore source DB from sample.bak (if available)
#   2. Create source DB schema (fallback if .bak restore is skipped)
#   3. Seed source DB with sample data
#   4. Create DWH database and star schema tables
#   5. Deploy stored procedures
#   6. Optionally seed DWH directly (bypasses Talend)
#   7. Run verification checks
#
# Usage:
#   ./scripts/init_databases.sh [OPTIONS]
#
# Options:
#   --server SERVER     SQL Server hostname (default: localhost)
#   --port PORT         SQL Server port (default: 1433)
#   --user USER         SQL Server login (default: sa)
#   --password PASS     SQL Server password (required unless using Windows auth)
#   --windows-auth      Use Windows Authentication (no user/pass needed)
#   --restore-bak       Attempt to restore from sample.bak
#   --bak-path PATH     Path to sample.bak (default: ./data_sources/sample.bak)
#   --seed-dwh          Seed DWH directly (skip Talend, use SQL INSERT scripts)
#   --verify            Run verification script after setup
#   --skip-source       Skip source DB creation (use if restoring from .bak)
#   --help              Show this help message
#
# Examples:
#   # Full setup with seed data and verification (no Talend needed):
#   ./scripts/init_databases.sh --password MyP@ssw0rd --seed-dwh --verify
#
#   # Restore from backup + let Talend handle the DWH load:
#   ./scripts/init_databases.sh --password MyP@ssw0rd --restore-bak --skip-source
#
#   # Windows Authentication:
#   ./scripts/init_databases.sh --windows-auth --seed-dwh --verify
###############################################################################

set -euo pipefail

# ----------------------------------------------------------
# Default configuration
# ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SQL_DIR="$PROJECT_ROOT/sql_scripts"

SERVER="localhost"
PORT="1433"
USER="sa"
PASSWORD=""
WINDOWS_AUTH=false
RESTORE_BAK=false
BAK_PATH="$PROJECT_ROOT/data_sources/sample.bak"
SEED_DWH=false
VERIFY=false
SKIP_SOURCE=false

# ----------------------------------------------------------
# Parse arguments
# ----------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --server)       SERVER="$2";       shift 2 ;;
        --port)         PORT="$2";         shift 2 ;;
        --user)         USER="$2";         shift 2 ;;
        --password)     PASSWORD="$2";     shift 2 ;;
        --windows-auth) WINDOWS_AUTH=true; shift ;;
        --restore-bak)  RESTORE_BAK=true;  shift ;;
        --bak-path)     BAK_PATH="$2";     shift 2 ;;
        --seed-dwh)     SEED_DWH=true;     shift ;;
        --verify)       VERIFY=true;       shift ;;
        --skip-source)  SKIP_SOURCE=true;  shift ;;
        --help)
            head -n 40 "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "ERROR: Unknown option: $1"
            echo "Run with --help for usage information."
            exit 1
            ;;
    esac
done

# ----------------------------------------------------------
# Build sqlcmd connection string
# ----------------------------------------------------------
build_sqlcmd_args() {
    local args=("-S" "${SERVER},${PORT}")
    if [ "$WINDOWS_AUTH" = true ]; then
        args+=("-E")
    else
        if [ -z "$PASSWORD" ]; then
            echo "ERROR: --password is required (or use --windows-auth)." >&2
            exit 1
        fi
        args+=("-U" "$USER" "-P" "$PASSWORD")
    fi
    echo "${args[@]}"
}

SQLCMD_ARGS=$(build_sqlcmd_args)

run_sql_file() {
    local file="$1"
    local description="$2"
    echo ""
    echo "================================================================"
    echo "  $description"
    echo "  File: $file"
    echo "================================================================"
    # shellcheck disable=SC2086
    if sqlcmd $SQLCMD_ARGS -i "$file" -b; then
        echo "  -> SUCCESS"
    else
        echo "  -> FAILED"
        exit 1
    fi
}

# ----------------------------------------------------------
# Check prerequisites
# ----------------------------------------------------------
echo "======================================================================"
echo "  Banking ETL Data Warehouse - Database Initialization"
echo "======================================================================"
echo ""
echo "Configuration:"
echo "  Server:       $SERVER:$PORT"
echo "  Auth:         $([ "$WINDOWS_AUTH" = true ] && echo 'Windows' || echo 'SQL Server')"
echo "  Restore .bak: $RESTORE_BAK"
echo "  Seed DWH:     $SEED_DWH"
echo "  Verify:       $VERIFY"
echo ""

if ! command -v sqlcmd &> /dev/null; then
    echo "ERROR: 'sqlcmd' is not installed or not in PATH."
    echo ""
    echo "Install instructions:"
    echo "  Windows: Included with SQL Server or SSMS"
    echo "  Linux:   https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools"
    echo "  macOS:   brew install microsoft/mssql-release/mssql-tools18"
    exit 1
fi

# ----------------------------------------------------------
# Step 1: Restore from .bak (optional)
# ----------------------------------------------------------
if [ "$RESTORE_BAK" = true ]; then
    echo ""
    echo "================================================================"
    echo "  Step 1: Restoring source database from sample.bak"
    echo "================================================================"
    if [ ! -f "$BAK_PATH" ]; then
        echo "ERROR: Backup file not found: $BAK_PATH"
        exit 1
    fi
    echo "  Restoring from: $BAK_PATH"
    echo "  NOTE: You may need to adjust the MOVE paths for your SQL Server"
    echo "        data directory. Edit the script if the restore fails."
    # shellcheck disable=SC2086
    sqlcmd $SQLCMD_ARGS -Q "
        RESTORE DATABASE [sample]
        FROM DISK = '$BAK_PATH'
        WITH REPLACE,
        MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf',
        MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf';
    " -b
    echo "  -> Source database restored from .bak"
fi

# ----------------------------------------------------------
# Step 2: Create source DB schema (if not restoring from .bak)
# ----------------------------------------------------------
if [ "$SKIP_SOURCE" = false ]; then
    run_sql_file "$SQL_DIR/03_create_source_database.sql" \
        "Step 2: Creating source database schema"

    run_sql_file "$SQL_DIR/04_seed_source_data.sql" \
        "Step 3: Seeding source database with sample data"
fi

# ----------------------------------------------------------
# Step 4: Create DWH database and tables
# ----------------------------------------------------------
run_sql_file "$SQL_DIR/01_create_tables.sql" \
    "Step 4: Creating DWH database and star schema tables"

# ----------------------------------------------------------
# Step 5: Deploy stored procedures
# ----------------------------------------------------------
run_sql_file "$SQL_DIR/02_create_procedures.sql" \
    "Step 5: Deploying stored procedures"

# ----------------------------------------------------------
# Step 6: Seed DWH directly (optional, bypasses Talend)
# ----------------------------------------------------------
if [ "$SEED_DWH" = true ]; then
    run_sql_file "$SQL_DIR/05_seed_dwh_data.sql" \
        "Step 6: Seeding DWH directly (bypassing Talend)"
fi

# ----------------------------------------------------------
# Step 7: Run verification (optional)
# ----------------------------------------------------------
if [ "$VERIFY" = true ]; then
    run_sql_file "$SQL_DIR/06_verify_warehouse.sql" \
        "Step 7: Running verification checks"
fi

# ----------------------------------------------------------
# Done
# ----------------------------------------------------------
echo ""
echo "======================================================================"
echo "  Initialization complete!"
echo "======================================================================"
echo ""
if [ "$SEED_DWH" = true ]; then
    echo "  DWH has been seeded directly. You can now run queries:"
    echo "    EXEC sp_DailyTransaction @start_date='2024-01-18', @end_date='2024-01-22';"
    echo "    EXEC sp_BalancePerCustomer @customer_name='ANDI';"
else
    echo "  Source DB is ready. Next steps:"
    echo "    1. Open Talend Open Studio"
    echo "    2. Import the project from talend_jobs/"
    echo "    3. Run jobs in order: Load_DimBranch, Load_DimAccount,"
    echo "       Load_DimCustomer, then Load_FactTransaction"
    echo "    4. Run: ./scripts/init_databases.sh --verify"
fi
