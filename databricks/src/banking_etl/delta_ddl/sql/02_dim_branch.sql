-- Source: sql_scripts/01_create_tables.sql, table DimBranch.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.dim_branch (
  branch_id       INT          NOT NULL COMMENT 'Branch identifier (primary key). Source: DimBranch.BranchID INT PRIMARY KEY',
  branch_name     VARCHAR(100)          COMMENT 'Branch name. Source: DimBranch.BranchName VARCHAR(100)',
  branch_location VARCHAR(255)          COMMENT 'Branch location. Source: DimBranch.BranchLocation VARCHAR(255)'
)
USING DELTA
COMMENT 'Branch dimension: one row per bank branch. Migrated from SQL Server DWH.dbo.DimBranch (loaded by Talend job Load_DimBranch).'
TBLPROPERTIES (
  'banking_etl.layer' = 'gold',
  'banking_etl.source_table' = 'DWH.dbo.DimBranch'
);
