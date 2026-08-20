/************************************************************************************
-- SCRIPT UNTUK MEMBUAT DATABASE DAN TABEL-TABEL DATA WAREHOUSE (DWH)
-- Proyek: Final Task Data Engineer - ID/X Partners
-- Deskripsi: Skrip ini akan membuat database DWH dan 4 tabel inti:
--            1. DimAccount
--            2. DimBranch
--            3. DimCustomer
--            4. FactTransaction
--            5. DimExchangeRate (historical FX rates)
************************************************************************************/

-- Langkah 1: Membuat Database baru bernama DWH (jika belum ada)
-- Note: Beberapa sistem mungkin memerlukan izin khusus untuk ini.
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DWH')
BEGIN
    CREATE DATABASE DWH;
END
GO

-- Langkah 2: Menggunakan database DWH untuk semua perintah selanjutnya
USE DWH;
GO

-- Langkah 3: Membuat Tabel-Tabel Dimensi dan Fakta

-- Tabel Dimensi untuk Akun
PRINT 'Creating table DimAccount...';
CREATE TABLE DimAccount (
    AccountID INT PRIMARY KEY,
    CustomerID INT,
    AccountType VARCHAR(50),
    Balance MONEY,
    DateOpened DATE,
    Status VARCHAR(50)
);
PRINT 'Table DimAccount created successfully.';
GO

-- Tabel Dimensi untuk Cabang
PRINT 'Creating table DimBranch...';
CREATE TABLE DimBranch (
    BranchID INT PRIMARY KEY,
    BranchName VARCHAR(100),
    BranchLocation VARCHAR(255)
);
PRINT 'Table DimBranch created successfully.';
GO

-- Tabel Dimensi untuk Pelanggan (hasil gabungan dari customer, city, state)
PRINT 'Creating table DimCustomer...';
CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    Address VARCHAR(255),
    CityName VARCHAR(100),
    StateName VARCHAR(100),
    Age INT,
    Gender VARCHAR(10),
    Email VARCHAR(100)
);
PRINT 'Table DimCustomer created successfully.';
GO

-- Tabel Fakta untuk Transaksi (inti dari DWH)
PRINT 'Creating table FactTransaction...';
CREATE TABLE FactTransaction (
    TransactionID INT PRIMARY KEY,
    AccountID INT,
    TransactionDate DATETIME,
    Amount MONEY,
    -- Currency in which Amount is denominated. Existing rows predate FX support,
    -- so the base currency USD is used as the default.
    CurrencyCode VARCHAR(3) NOT NULL CONSTRAINT DF_FactTransaction_CurrencyCode DEFAULT 'USD',
    TransactionType VARCHAR(50),
    BranchID INT,
    
    -- Mendefinisikan Foreign Keys (Relasi antar tabel)
    CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (AccountID) REFERENCES DimAccount(AccountID),
    CONSTRAINT FK_FactTransaction_DimBranch FOREIGN KEY (BranchID) REFERENCES DimBranch(BranchID)
);
PRINT 'Table FactTransaction created successfully.';
GO

-- Tabel Dimensi untuk Kurs Historis (FX exchange rates)
-- Stores one observed rate per currency pair per date. The sampling procedure
-- sp_SampleExchangeRate draws from the trailing 6 months of this history.
PRINT 'Creating table DimExchangeRate...';
CREATE TABLE DimExchangeRate (
    RateID INT PRIMARY KEY,
    CurrencyFrom VARCHAR(3) NOT NULL,
    CurrencyTo VARCHAR(3) NOT NULL,
    RateDate DATE NOT NULL,
    Rate DECIMAL(19,6) NOT NULL,

    CONSTRAINT UQ_DimExchangeRate_Pair_Date UNIQUE (CurrencyFrom, CurrencyTo, RateDate)
);
PRINT 'Table DimExchangeRate created successfully.';
GO

-- Index supporting the trailing-window lookup by currency pair and date
CREATE INDEX IX_DimExchangeRate_Pair_Date
    ON DimExchangeRate (CurrencyFrom, CurrencyTo, RateDate) INCLUDE (Rate);
GO

PRINT 'All tables for DWH have been created.';