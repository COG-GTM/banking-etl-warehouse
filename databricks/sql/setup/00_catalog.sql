-- Optional: needs CREATE CATALOG on the metastore (admin). Run only with create_catalog=true.
CREATE CATALOG IF NOT EXISTS ${catalog}
  COMMENT 'Banking DWH migrated from Talend + SQL Server (COG-GTM/banking-etl-warehouse).';
