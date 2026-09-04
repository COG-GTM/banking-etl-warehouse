-- Gold layer: star-schema tables migrated from sql_scripts/01_create_tables.sql (SQL Server DWH).
-- Type mapping: INT->INT, VARCHAR(n)->STRING, MONEY->DECIMAL(19,4), DATE->DATE, DATETIME->TIMESTAMP.
-- PRIMARY KEY / FOREIGN KEY constraints are informational (NOT ENFORCED) per Unity Catalog semantics.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

-- Tabel Dimensi untuk Akun
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.DimAccount (
    AccountID INT NOT NULL COMMENT 'Surrogate/business key of the account',
    CustomerID INT COMMENT 'Owning customer (DimCustomer.CustomerID)',
    AccountType STRING COMMENT 'Account product type',
    Balance DECIMAL(19,4) COMMENT 'Initial balance at load time (was MONEY)',
    DateOpened DATE COMMENT 'Date the account was opened',
    Status STRING COMMENT 'Account status, e.g. active / closed',
    CONSTRAINT pk_dimaccount PRIMARY KEY (AccountID) NOT ENFORCED
)
USING DELTA
COMMENT 'Dimension table for accounts (Tabel Dimensi untuk Akun)';

-- Tabel Dimensi untuk Cabang
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.DimBranch (
    BranchID INT NOT NULL COMMENT 'Branch identifier',
    BranchName STRING COMMENT 'Branch name',
    BranchLocation STRING COMMENT 'Branch location / address',
    CONSTRAINT pk_dimbranch PRIMARY KEY (BranchID) NOT ENFORCED
)
USING DELTA
COMMENT 'Dimension table for branches (Tabel Dimensi untuk Cabang)';

-- Tabel Dimensi untuk Pelanggan (hasil gabungan dari customer, city, state)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.DimCustomer (
    CustomerID INT NOT NULL COMMENT 'Customer identifier',
    CustomerName STRING COMMENT 'Customer full name (upper-cased by ETL)',
    Address STRING COMMENT 'Street address (upper-cased by ETL)',
    CityName STRING COMMENT 'Denormalized city name from source city table',
    StateName STRING COMMENT 'Denormalized state name from source state table',
    Age INT COMMENT 'Customer age',
    Gender STRING COMMENT 'Customer gender',
    Email STRING COMMENT 'Customer e-mail address',
    CONSTRAINT pk_dimcustomer PRIMARY KEY (CustomerID) NOT ENFORCED
)
USING DELTA
COMMENT 'Dimension table for customers, denormalized from customer + city + state (Tabel Dimensi untuk Pelanggan)';

-- Tabel Fakta untuk Transaksi (inti dari DWH)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.FactTransaction (
    TransactionID INT NOT NULL COMMENT 'Transaction identifier (deduplicated across DB/CSV/XLSX sources)',
    AccountID INT COMMENT 'Account the transaction belongs to',
    TransactionDate TIMESTAMP COMMENT 'Transaction timestamp (was DATETIME)',
    Amount DECIMAL(19,4) COMMENT 'Transaction amount (was MONEY)',
    TransactionType STRING COMMENT 'Deposit / Withdrawal / Transfer ...',
    BranchID INT COMMENT 'Branch where the transaction occurred',
    CONSTRAINT pk_facttransaction PRIMARY KEY (TransactionID) NOT ENFORCED,
    CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (AccountID) REFERENCES ${catalog}.${schema}.DimAccount (AccountID) NOT ENFORCED,
    CONSTRAINT FK_FactTransaction_DimBranch FOREIGN KEY (BranchID) REFERENCES ${catalog}.${schema}.DimBranch (BranchID) NOT ENFORCED
)
USING DELTA
COMMENT 'Fact table for transactions, the core of the DWH (Tabel Fakta untuk Transaksi)';
