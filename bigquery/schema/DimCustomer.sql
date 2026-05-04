-- BigQuery DDL for DimCustomer
-- Migrated from SQL Server T-SQL schema

CREATE TABLE IF NOT EXISTS DimCustomer (
    CustomerID   INT64 NOT NULL,
    CustomerName STRING,
    Address      STRING,
    CityName     STRING,
    StateName    STRING,
    Age          INT64,
    Gender       STRING,
    Email        STRING,

    PRIMARY KEY (CustomerID) NOT ENFORCED
);
