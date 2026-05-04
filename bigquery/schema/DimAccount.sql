-- BigQuery DDL for DimAccount
-- Migrated from SQL Server T-SQL schema

CREATE TABLE IF NOT EXISTS DimAccount (
    AccountID   INT64 NOT NULL,
    CustomerID  INT64,
    AccountType STRING,
    Balance     NUMERIC,
    DateOpened  DATE,
    Status      STRING,

    PRIMARY KEY (AccountID) NOT ENFORCED
);
