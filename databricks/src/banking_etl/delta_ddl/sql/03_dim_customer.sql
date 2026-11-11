-- Source: sql_scripts/01_create_tables.sql, table DimCustomer.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.dim_customer (
  customer_id   INT          NOT NULL COMMENT 'Customer identifier (primary key). Source: DimCustomer.CustomerID INT PRIMARY KEY',
  customer_name VARCHAR(100)          COMMENT 'Customer full name. Source: DimCustomer.CustomerName VARCHAR(100)',
  address       VARCHAR(255)          COMMENT 'Street address. Source: DimCustomer.Address VARCHAR(255)',
  city_name     VARCHAR(100)          COMMENT 'City name (denormalised from the city table). Source: DimCustomer.CityName VARCHAR(100)',
  state_name    VARCHAR(100)          COMMENT 'State name (denormalised from the state table). Source: DimCustomer.StateName VARCHAR(100)',
  age           INT                   COMMENT 'Customer age in years. Source: DimCustomer.Age INT',
  gender        VARCHAR(10)           COMMENT 'Customer gender. Source: DimCustomer.Gender VARCHAR(10)',
  email         VARCHAR(100)          COMMENT 'Customer email address. Source: DimCustomer.Email VARCHAR(100)'
)
USING DELTA
COMMENT 'Customer dimension: one row per customer, combining customer, city and state. Migrated from SQL Server DWH.dbo.DimCustomer (loaded by Talend job Load_DimCustomer).'
TBLPROPERTIES (
  'banking_etl.layer' = 'gold',
  'banking_etl.source_table' = 'DWH.dbo.DimCustomer'
);
