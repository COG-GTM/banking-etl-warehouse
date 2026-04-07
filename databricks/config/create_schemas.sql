-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Schema Setup: Create Unity Catalog Objects
-- MAGIC
-- MAGIC Run this SQL notebook once to initialize the Unity Catalog structure
-- MAGIC for the banking DWH medallion architecture.

-- COMMAND ----------

-- Create the catalog (requires CREATE CATALOG privilege)
-- Uncomment if needed:
-- CREATE CATALOG IF NOT EXISTS banking_dwh;

-- COMMAND ----------

USE CATALOG banking_dwh;

-- COMMAND ----------

-- Bronze schema: raw ingested data from source systems
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingested data from source systems (SQL Server exports, CSV, Excel)';

-- COMMAND ----------

-- Silver schema: cleansed and transformed dimension/fact tables
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleansed and transformed Star Schema tables (DimCustomer, DimAccount, DimBranch, FactTransaction)';

-- COMMAND ----------

-- Gold schema: business-level aggregated analytical tables
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-level analytical tables replacing legacy stored procedures';

-- COMMAND ----------

-- Raw data schema: holds the volume for source files
CREATE SCHEMA IF NOT EXISTS raw_data
COMMENT 'Schema for Unity Catalog Volumes holding raw source data files';

-- COMMAND ----------

-- Volume for source files
CREATE VOLUME IF NOT EXISTS raw_data.source_files
COMMENT 'Volume for raw source data files (CSV, Excel) uploaded from legacy systems';

-- COMMAND ----------

-- Verify setup
SHOW SCHEMAS;
