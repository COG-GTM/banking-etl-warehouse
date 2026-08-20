-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DWH Star Schema — Delta Lake DDL
-- MAGIC
-- MAGIC Databricks (Delta Lake) port of `sql_scripts/01_create_tables.sql`.
-- MAGIC
-- MAGIC Type mapping from SQL Server:
-- MAGIC
-- MAGIC | SQL Server   | Delta / Spark    |
-- MAGIC |--------------|------------------|
-- MAGIC | `INT`        | `INT`            |
-- MAGIC | `VARCHAR(n)` | `STRING`         |
-- MAGIC | `MONEY`      | `DECIMAL(19,4)`  |
-- MAGIC | `DATE`       | `DATE`           |
-- MAGIC | `DATETIME`   | `TIMESTAMP`      |
-- MAGIC
-- MAGIC Delta Lake does not enforce PRIMARY KEY / FOREIGN KEY constraints. The original
-- MAGIC keys are kept as informational documentation (table/column comments) and the key
-- MAGIC columns are declared `NOT NULL` so that null keys still fail on write.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

-- COMMAND ----------

-- Dimension: Account
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.DimAccount (
    AccountID   INT            NOT NULL COMMENT 'Informational PRIMARY KEY',
    CustomerID  INT                     COMMENT 'Informational FOREIGN KEY -> DimCustomer.CustomerID',
    AccountType STRING,
    Balance     DECIMAL(19,4)           COMMENT 'SQL Server MONEY',
    DateOpened  DATE,
    Status      STRING
)
USING DELTA
COMMENT 'Account dimension. Informational PK: AccountID.';

-- COMMAND ----------

-- Dimension: Branch
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.DimBranch (
    BranchID       INT NOT NULL COMMENT 'Informational PRIMARY KEY',
    BranchName     STRING,
    BranchLocation STRING
)
USING DELTA
COMMENT 'Branch dimension. Informational PK: BranchID.';

-- COMMAND ----------

-- Dimension: Customer (customer + city + state, denormalized)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.DimCustomer (
    CustomerID   INT NOT NULL COMMENT 'Informational PRIMARY KEY',
    CustomerName STRING,
    Address      STRING,
    CityName     STRING,
    StateName    STRING,
    Age          INT,
    Gender       STRING,
    Email        STRING
)
USING DELTA
COMMENT 'Customer dimension. Informational PK: CustomerID.';

-- COMMAND ----------

-- Fact: Transaction
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.FactTransaction (
    TransactionID   INT       NOT NULL COMMENT 'Informational PRIMARY KEY',
    AccountID       INT                COMMENT 'Informational FOREIGN KEY -> DimAccount.AccountID',
    TransactionDate TIMESTAMP          COMMENT 'SQL Server DATETIME',
    Amount          DECIMAL(19,4)      COMMENT 'SQL Server MONEY',
    TransactionType STRING,
    BranchID        INT                COMMENT 'Informational FOREIGN KEY -> DimBranch.BranchID'
)
USING DELTA
COMMENT 'Transaction fact table. Informational PK: TransactionID; FKs: AccountID, BranchID.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC On Unity Catalog the informational constraints below can additionally be declared
-- MAGIC (they are documented in the metastore but never enforced at write time):
-- MAGIC
-- MAGIC ```sql
-- MAGIC ALTER TABLE DimAccount       ADD CONSTRAINT pk_dimaccount       PRIMARY KEY (AccountID);
-- MAGIC ALTER TABLE DimBranch        ADD CONSTRAINT pk_dimbranch        PRIMARY KEY (BranchID);
-- MAGIC ALTER TABLE DimCustomer      ADD CONSTRAINT pk_dimcustomer      PRIMARY KEY (CustomerID);
-- MAGIC ALTER TABLE FactTransaction  ADD CONSTRAINT pk_facttransaction  PRIMARY KEY (TransactionID);
-- MAGIC ALTER TABLE FactTransaction  ADD CONSTRAINT fk_fact_account     FOREIGN KEY (AccountID) REFERENCES DimAccount;
-- MAGIC ALTER TABLE FactTransaction  ADD CONSTRAINT fk_fact_branch      FOREIGN KEY (BranchID)  REFERENCES DimBranch;
-- MAGIC ```
