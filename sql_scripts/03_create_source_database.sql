/************************************************************************************
-- SCRIPT TO CREATE THE SOURCE DATABASE (sample) AND ALL SOURCE TABLES
-- Project: Banking ETL Data Warehouse
-- Description: This script recreates the source database schema that would
--              normally be restored from sample.bak. Use this as an alternative
--              when the .bak file cannot be restored (e.g., version mismatch).
--
-- Source Tables:
--   1. state        - US state reference data
--   2. city         - City reference data (FK -> state)
--   3. branch       - Bank branch locations
--   4. customer     - Customer master data (FK -> city)
--   5. account      - Customer accounts (FK -> customer)
--   6. transaction_db - Transaction records from the SQL Server source
************************************************************************************/

-- Step 1: Create the source database (if it doesn't exist)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'sample')
BEGIN
    CREATE DATABASE sample;
END
GO

-- Step 2: Switch to the sample database
USE sample;
GO

-- Step 3: Create reference tables first (no FK dependencies)

PRINT 'Creating table state...';
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'state')
BEGIN
    CREATE TABLE state (
        state_id   INT          PRIMARY KEY,
        state_name VARCHAR(50)  NULL
    );
END
GO

PRINT 'Creating table city...';
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'city')
BEGIN
    CREATE TABLE city (
        city_id    INT          PRIMARY KEY,
        city_name  VARCHAR(50)  NULL,
        state_id   INT          NOT NULL,
        CONSTRAINT FK_city_state FOREIGN KEY (state_id) REFERENCES state(state_id)
    );
END
GO

PRINT 'Creating table branch...';
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'branch')
BEGIN
    CREATE TABLE branch (
        branch_id       INT          PRIMARY KEY,
        branch_name     VARCHAR(50)  NULL,
        branch_location VARCHAR(50)  NULL
    );
END
GO

PRINT 'Creating table customer...';
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer')
BEGIN
    CREATE TABLE customer (
        customer_id   INT            PRIMARY KEY,
        customer_name VARCHAR(50)    NULL,
        address       VARCHAR(MAX)   NULL,
        city_id       INT            NULL,
        age           VARCHAR(3)     NULL,
        gender        VARCHAR(10)    NULL,
        email         VARCHAR(50)    NULL,
        CONSTRAINT FK_customer_city FOREIGN KEY (city_id) REFERENCES city(city_id)
    );
END
GO

PRINT 'Creating table account...';
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'account')
BEGIN
    CREATE TABLE account (
        account_id   INT          PRIMARY KEY,
        customer_id  INT          NULL,
        account_type VARCHAR(10)  NULL,
        balance      INT          NULL,
        date_opened  DATETIME2    NULL,
        status       VARCHAR(10)  NULL,
        CONSTRAINT FK_account_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
    );
END
GO

PRINT 'Creating table transaction_db...';
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'transaction_db')
BEGIN
    CREATE TABLE transaction_db (
        transaction_id   INT          PRIMARY KEY,
        account_id       INT          NULL,
        transaction_date DATETIME2    NULL,
        amount           INT          NULL,
        transaction_type VARCHAR(50)  NULL,
        branch_id        INT          NULL,
        CONSTRAINT FK_txn_account FOREIGN KEY (account_id) REFERENCES account(account_id),
        CONSTRAINT FK_txn_branch  FOREIGN KEY (branch_id) REFERENCES branch(branch_id)
    );
END
GO

PRINT 'All source tables for [sample] database have been created.';
