<#
.SYNOPSIS
    Initialize both source and DWH databases for the Banking ETL Data Warehouse.

.DESCRIPTION
    This PowerShell script automates the full database initialization pipeline:
      1. Optionally restore source DB from sample.bak
      2. Create source DB schema (fallback if .bak restore is skipped)
      3. Seed source DB with sample data
      4. Create DWH database and star schema tables
      5. Deploy stored procedures
      6. Optionally seed DWH directly (bypasses Talend)
      7. Run verification checks

.PARAMETER Server
    SQL Server hostname (default: localhost)

.PARAMETER Port
    SQL Server port (default: 1433)

.PARAMETER User
    SQL Server login (default: sa)

.PARAMETER Password
    SQL Server password (leave empty for Windows Authentication)

.PARAMETER WindowsAuth
    Use Windows Authentication

.PARAMETER RestoreBak
    Attempt to restore source database from sample.bak

.PARAMETER BakPath
    Path to sample.bak (default: .\data_sources\sample.bak)

.PARAMETER SeedDwh
    Seed DWH directly using SQL INSERT scripts (skip Talend)

.PARAMETER Verify
    Run verification script after setup

.PARAMETER SkipSource
    Skip source DB creation (use when restoring from .bak)

.EXAMPLE
    # Full setup with seed data and verification:
    .\scripts\init_databases.ps1 -Password "MyP@ssw0rd" -SeedDwh -Verify

.EXAMPLE
    # Windows Authentication with DWH seeding:
    .\scripts\init_databases.ps1 -WindowsAuth -SeedDwh -Verify

.EXAMPLE
    # Restore from backup, then use Talend for ETL:
    .\scripts\init_databases.ps1 -Password "MyP@ssw0rd" -RestoreBak -SkipSource
#>

param(
    [string]$Server      = "localhost",
    [int]$Port           = 1433,
    [string]$User        = "sa",
    [string]$Password    = "",
    [switch]$WindowsAuth,
    [switch]$RestoreBak,
    [string]$BakPath     = "",
    [switch]$SeedDwh,
    [switch]$Verify,
    [switch]$SkipSource
)

$ErrorActionPreference = "Stop"

# Resolve paths
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$SqlDir      = Join-Path $ProjectRoot "sql_scripts"

if ([string]::IsNullOrEmpty($BakPath)) {
    $BakPath = Join-Path $ProjectRoot "data_sources\sample.bak"
}

# ----------------------------------------------------------
# Helper: Run a SQL file via sqlcmd
# ----------------------------------------------------------
function Invoke-SqlFile {
    param(
        [string]$FilePath,
        [string]$Description
    )

    Write-Host ""
    Write-Host "================================================================"
    Write-Host "  $Description"
    Write-Host "  File: $FilePath"
    Write-Host "================================================================"

    $args = @("-S", "$Server,$Port", "-i", $FilePath, "-b")

    if ($WindowsAuth) {
        $args += "-E"
    } else {
        if ([string]::IsNullOrEmpty($Password)) {
            Write-Error "Password is required (or use -WindowsAuth)."
            exit 1
        }
        $args += @("-U", $User, "-P", $Password)
    }

    & sqlcmd @args
    if ($LASTEXITCODE -ne 0) {
        Write-Error "  -> FAILED: $Description"
        exit 1
    }
    Write-Host "  -> SUCCESS"
}

# ----------------------------------------------------------
# Main execution
# ----------------------------------------------------------
Write-Host "======================================================================"
Write-Host "  Banking ETL Data Warehouse - Database Initialization (PowerShell)"
Write-Host "======================================================================"
Write-Host ""
Write-Host "Configuration:"
Write-Host "  Server:       ${Server}:${Port}"
Write-Host "  Auth:         $(if ($WindowsAuth) { 'Windows' } else { 'SQL Server' })"
Write-Host "  Restore .bak: $RestoreBak"
Write-Host "  Seed DWH:     $SeedDwh"
Write-Host "  Verify:       $Verify"
Write-Host ""

# Check sqlcmd availability
if (-not (Get-Command sqlcmd -ErrorAction SilentlyContinue)) {
    Write-Error @"
'sqlcmd' is not installed or not in PATH.

Install instructions:
  - Included with SQL Server or SSMS
  - Or install: https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility
"@
    exit 1
}

# Step 1: Restore from .bak (optional)
if ($RestoreBak) {
    Write-Host ""
    Write-Host "================================================================"
    Write-Host "  Step 1: Restoring source database from sample.bak"
    Write-Host "================================================================"

    if (-not (Test-Path $BakPath)) {
        Write-Error "Backup file not found: $BakPath"
        exit 1
    }

    $restoreQuery = @"
RESTORE DATABASE [sample]
FROM DISK = '$BakPath'
WITH REPLACE;
"@

    $restoreArgs = @("-S", "$Server,$Port", "-Q", $restoreQuery, "-b")
    if ($WindowsAuth) {
        $restoreArgs += "-E"
    } else {
        $restoreArgs += @("-U", $User, "-P", $Password)
    }

    & sqlcmd @restoreArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "  Restore failed. You may need to specify MOVE options."
        Write-Warning "  Try restoring manually in SSMS instead."
    } else {
        Write-Host "  -> Source database restored from .bak"
    }
}

# Step 2-3: Create and seed source DB
if (-not $SkipSource) {
    Invoke-SqlFile -FilePath (Join-Path $SqlDir "03_create_source_database.sql") `
                   -Description "Step 2: Creating source database schema"

    Invoke-SqlFile -FilePath (Join-Path $SqlDir "04_seed_source_data.sql") `
                   -Description "Step 3: Seeding source database with sample data"
}

# Step 4: Create DWH
Invoke-SqlFile -FilePath (Join-Path $SqlDir "01_create_tables.sql") `
               -Description "Step 4: Creating DWH database and star schema tables"

# Step 5: Stored procedures
Invoke-SqlFile -FilePath (Join-Path $SqlDir "02_create_procedures.sql") `
               -Description "Step 5: Deploying stored procedures"

# Step 6: Seed DWH (optional)
if ($SeedDwh) {
    Invoke-SqlFile -FilePath (Join-Path $SqlDir "05_seed_dwh_data.sql") `
                   -Description "Step 6: Seeding DWH directly (bypassing Talend)"
}

# Step 7: Verify (optional)
if ($Verify) {
    Invoke-SqlFile -FilePath (Join-Path $SqlDir "06_verify_warehouse.sql") `
                   -Description "Step 7: Running verification checks"
}

# Done
Write-Host ""
Write-Host "======================================================================"
Write-Host "  Initialization complete!"
Write-Host "======================================================================"
Write-Host ""

if ($SeedDwh) {
    Write-Host "  DWH has been seeded directly. You can now run queries:"
    Write-Host "    EXEC sp_DailyTransaction @start_date='2024-01-18', @end_date='2024-01-22';"
    Write-Host "    EXEC sp_BalancePerCustomer @customer_name='ANDI';"
} else {
    Write-Host "  Source DB is ready. Next steps:"
    Write-Host "    1. Open Talend Open Studio"
    Write-Host "    2. Import the project from talend_jobs/"
    Write-Host "    3. Run jobs in order: Load_DimBranch, Load_DimAccount,"
    Write-Host "       Load_DimCustomer, then Load_FactTransaction"
    Write-Host "    4. Run: .\scripts\init_databases.ps1 -Verify"
}
