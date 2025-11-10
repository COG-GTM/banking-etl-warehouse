/************************************************************************************
-- SCRIPT UNTUK MEMBUAT DATABASE DAN TABEL-TABEL DATA WAREHOUSE (DWH)
-- Proyek: Final Task Data Engineer - ID/X Partners
-- Deskripsi: Skrip ini akan membuat database DWH dan 4 tabel inti:
--            1. DimAccount
--            2. DimBranch
--            3. DimCustomer
--            4. FactTransaction
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
    TransactionType VARCHAR(50),
    BranchID INT,
    
    -- Mendefinisikan Foreign Keys (Relasi antar tabel)
    CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (AccountID) REFERENCES DimAccount(AccountID),
    CONSTRAINT FK_FactTransaction_DimBranch FOREIGN KEY (BranchID) REFERENCES DimBranch(BranchID)
);
PRINT 'Table FactTransaction created successfully.';
GO

PRINT 'All tables for DWH have been created.';