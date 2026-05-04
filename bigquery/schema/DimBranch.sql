-- BigQuery DDL for DimBranch
-- Migrated from SQL Server T-SQL schema

CREATE TABLE IF NOT EXISTS DimBranch (
    BranchID       INT64 NOT NULL,
    BranchName     STRING,
    BranchLocation STRING,

    PRIMARY KEY (BranchID) NOT ENFORCED
);
