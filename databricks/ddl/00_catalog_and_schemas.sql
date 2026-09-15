-- Unity Catalog container objects for the banking warehouse.
-- Replaces the T-SQL `CREATE DATABASE DWH` in sql_scripts/01_create_tables.sql.

CREATE CATALOG IF NOT EXISTS banking
COMMENT 'Banking ETL warehouse migrated from the Talend + SQL Server `sample`/`DWH` solution';

CREATE SCHEMA IF NOT EXISTS banking.bronze
COMMENT 'Raw landed copies of the Talend sources (SQL Server `sample`, CSV, Excel) with ingest metadata';

CREATE SCHEMA IF NOT EXISTS banking.silver
COMMENT 'Cleansed and conformed entities: typed, deduplicated, one row per business key';

CREATE SCHEMA IF NOT EXISTS banking.gold
COMMENT 'Star schema consumed by reporting: dim_account, dim_branch, dim_customer, fact_transaction';
