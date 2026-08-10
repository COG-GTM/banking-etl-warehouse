-- =====================================================================================
-- Unity Catalog provisioning for the banking data warehouse (Databricks migration)
--
-- Creates the `dwh` catalog and the four medallion schemas that replace the legacy
-- SQL Server `DWH` database. Idempotent: safe to re-run on every deployment.
--
-- Parameters (substituted by the Databricks Asset Bundle / job widgets):
--   ${catalog}      -- target catalog name           (default: dwh)
--   ${storage_root} -- external location URL used as MANAGED LOCATION,
--                      e.g. abfss://dwh@<account>.dfs.core.windows.net/managed
--                      or   s3://<bucket>/dwh/managed
--
-- MANAGED LOCATION notes:
--   * The clauses below are intentionally left as documented placeholders. Unity
--     Catalog requires an EXTERNAL LOCATION (with a matching storage credential)
--     to already exist before it can be referenced.
--   * If the metastore already has a default root storage and you are happy for
--     the catalog to inherit it, drop the MANAGED LOCATION clause entirely.
--   * To enable it, uncomment the MANAGED LOCATION lines and supply ${storage_root}
--     (see databricks/conf/{dev,prod}.yml -> storage_root).
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Catalog
-- -------------------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS ${catalog}
-- MANAGED LOCATION '${storage_root}'
COMMENT 'Banking data warehouse migrated from SQL Server DWH + Talend. Medallion layout: bronze / silver / gold / analytics.';

-- -------------------------------------------------------------------------------------
-- Schemas (medallion layers)
-- -------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
-- MANAGED LOCATION '${storage_root}/bronze'
COMMENT 'Raw landed data, one table per source object: SQL Server `sample` tables (account, branch, city, customer, state, transaction_db), transaction_csv, transaction_excel. No business logic applied.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
-- MANAGED LOCATION '${storage_root}/silver'
COMMENT 'Cleansed and conformed dimensions: dim_branch, dim_account, dim_customer. Replaces the Talend Load_Dim* jobs.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
-- MANAGED LOCATION '${storage_root}/gold'
COMMENT 'Star-schema serving layer: fact_transaction, unified and de-duplicated across the three transaction sources. Replaces the Talend Load_FactTransaction job.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.analytics
-- MANAGED LOCATION '${storage_root}/analytics'
COMMENT 'Stored-procedure replacements as parameterized Spark SQL / views: daily transaction summary (sp_DailyTransaction) and balance per customer (sp_BalancePerCustomer).';
