-- Optional, Unity Catalog only: informational (NOT ENFORCED) PK/FK constraints that mirror
-- the SQL Server keys. Hive metastore does not support these; skip this file there.
-- Run after 01_create_tables.sql with the target catalog selected (USE CATALOG <catalog>).

ALTER TABLE dwh.dim_account      ADD CONSTRAINT pk_dim_account      PRIMARY KEY (AccountID)     NOT ENFORCED;
ALTER TABLE dwh.dim_branch       ADD CONSTRAINT pk_dim_branch       PRIMARY KEY (BranchID)      NOT ENFORCED;
ALTER TABLE dwh.dim_customer     ADD CONSTRAINT pk_dim_customer     PRIMARY KEY (CustomerID)    NOT ENFORCED;
ALTER TABLE dwh.fact_transaction ADD CONSTRAINT pk_fact_transaction PRIMARY KEY (TransactionID) NOT ENFORCED;

ALTER TABLE dwh.fact_transaction ADD CONSTRAINT fk_fact_transaction_dim_account
    FOREIGN KEY (AccountID) REFERENCES dwh.dim_account (AccountID) NOT ENFORCED;
ALTER TABLE dwh.fact_transaction ADD CONSTRAINT fk_fact_transaction_dim_branch
    FOREIGN KEY (BranchID) REFERENCES dwh.dim_branch (BranchID) NOT ENFORCED;
