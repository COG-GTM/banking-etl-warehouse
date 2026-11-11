-- Gold schema for the banking star schema (SQL Server database DWH, schema dbo).
-- Placeholders ${catalog} and ${schema} are substituted by banking_etl.delta_ddl.
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'Gold layer: banking star schema (dim_account, dim_branch, dim_customer, fact_transaction) migrated from SQL Server DWH.dbo';
