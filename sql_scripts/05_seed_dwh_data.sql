/************************************************************************************
-- SCRIPT TO SEED THE DWH DATABASE DIRECTLY (BYPASS TALEND)
-- Project: Banking ETL Data Warehouse
-- Description: For environments where Talend is not available, this script loads
--              representative data directly into the DWH star schema tables.
--              This mirrors what the four Talend ETL jobs would produce.
--
-- Prerequisites: Run 01_create_tables.sql first to create the DWH database
--                and its tables.
--
-- This script populates:
--   1. DimBranch       (from: branch table in sample DB)
--   2. DimAccount      (from: account table in sample DB)
--   3. DimCustomer     (from: customer + city + state tables, with uppercase names)
--   4. FactTransaction (from: transaction_db + CSV + Excel, deduplicated)
************************************************************************************/

USE DWH;
GO

-- ============================================================================
-- DimBranch (equivalent to Talend job: Load_DimBranch)
-- Source: sample.dbo.branch
-- ============================================================================
PRINT 'Loading DimBranch...';

MERGE INTO DimBranch AS target
USING (VALUES
    (1, 'Branch Bandung',         'Jl. Braga No.1, Bandung'),
    (2, 'Branch Jakarta Selatan', 'Jl. Sudirman No.10, Jakarta'),
    (3, 'Branch Semarang',        'Jl. Pemuda No.5, Semarang'),
    (4, 'Branch Surabaya',        'Jl. Tunjungan No.8, Surabaya'),
    (5, 'Branch Tangerang',       'Jl. Raya Serpong No.3, Tangerang')
) AS source (BranchID, BranchName, BranchLocation)
ON target.BranchID = source.BranchID
WHEN NOT MATCHED THEN
    INSERT (BranchID, BranchName, BranchLocation)
    VALUES (source.BranchID, source.BranchName, source.BranchLocation);
GO

-- ============================================================================
-- DimAccount (equivalent to Talend job: Load_DimAccount)
-- Source: sample.dbo.account
-- ============================================================================
PRINT 'Loading DimAccount...';

MERGE INTO DimAccount AS target
USING (VALUES
    (1,  1,  'savings',  5000000,  '2023-01-15', 'active'),
    (2,  2,  'checking', 3000000,  '2023-02-20', 'active'),
    (3,  3,  'savings',  8000000,  '2023-03-10', 'active'),
    (4,  4,  'checking', 2000000,  '2023-04-05', 'active'),
    (5,  5,  'savings',  10000000, '2023-05-12', 'active'),
    (6,  6,  'checking', 1500000,  '2023-06-01', 'active'),
    (7,  7,  'savings',  6000000,  '2023-06-15', 'active'),
    (8,  8,  'checking', 4000000,  '2023-07-20', 'active'),
    (9,  9,  'savings',  7500000,  '2023-08-08', 'active'),
    (10, 10, 'checking', 2500000,  '2023-09-01', 'active'),
    (11, 11, 'savings',  9000000,  '2023-09-15', 'active'),
    (12, 12, 'checking', 3500000,  '2023-10-01', 'active'),
    (13, 13, 'savings',  5500000,  '2023-10-20', 'active'),
    (14, 14, 'checking', 4500000,  '2023-11-01', 'active'),
    (15, 15, 'savings',  6500000,  '2023-11-15', 'active'),
    (16, 16, 'checking', 1800000,  '2023-12-01', 'active'),
    (17, 17, 'savings',  7000000,  '2023-12-10', 'active'),
    (18, 18, 'checking', 2200000,  '2024-01-02', 'active'),
    (19, 19, 'savings',  8500000,  '2024-01-05', 'active'),
    (20, 20, 'checking', 3200000,  '2024-01-08', 'active'),
    (21, 21, 'savings',  4800000,  '2024-01-10', 'active'),
    (22, 22, 'checking', 1200000,  '2024-01-12', 'active'),
    (23, 23, 'savings',  9500000,  '2024-01-14', 'active'),
    (24, 24, 'checking', 2800000,  '2024-01-15', 'inactive'),
    (25, 25, 'savings',  6200000,  '2024-01-16', 'inactive')
) AS source (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
ON target.AccountID = source.AccountID
WHEN NOT MATCHED THEN
    INSERT (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
    VALUES (source.AccountID, source.CustomerID, source.AccountType, source.Balance,
            CAST(source.DateOpened AS DATE), source.Status);
GO

-- ============================================================================
-- DimCustomer (equivalent to Talend job: Load_DimCustomer)
-- Source: sample.dbo.customer JOIN city JOIN state
-- Note: Talend job converts customer_name to UPPERCASE
-- ============================================================================
PRINT 'Loading DimCustomer...';

MERGE INTO DimCustomer AS target
USING (VALUES
    (1,  'ANDI PRATAMA',    'Jl. Merdeka No.1',       'Bandung',          'Jawa Barat',   30, 'Male',   'andi.pratama@email.com'),
    (2,  'BUDI SANTOSO',    'Jl. Pahlawan No.2',      'Bekasi',           'Jawa Barat',   35, 'Male',   'budi.santoso@email.com'),
    (3,  'CITRA DEWI',      'Jl. Melati No.3',        'Semarang',         'Jawa Tengah',  28, 'Female', 'citra.dewi@email.com'),
    (4,  'DIAN SAPUTRA',    'Jl. Kenanga No.4',       'Solo',             'Jawa Tengah',  42, 'Male',   'dian.saputra@email.com'),
    (5,  'EKA RAHMAWATI',   'Jl. Mawar No.5',         'Surabaya',         'Jawa Timur',   25, 'Female', 'eka.rahmawati@email.com'),
    (6,  'FAJAR HIDAYAT',   'Jl. Dahlia No.6',        'Malang',           'Jawa Timur',   33, 'Male',   'fajar.hidayat@email.com'),
    (7,  'GITA PURNAMA',    'Jl. Anggrek No.7',       'Jakarta Selatan',  'DKI Jakarta',  29, 'Female', 'gita.purnama@email.com'),
    (8,  'HENDRA WIJAYA',   'Jl. Cempaka No.8',       'Jakarta Pusat',    'DKI Jakarta',  38, 'Male',   'hendra.wijaya@email.com'),
    (9,  'INDAH LESTARI',   'Jl. Tulip No.9',         'Tangerang',        'Banten',       31, 'Female', 'indah.lestari@email.com'),
    (10, 'JOKO SUSILO',     'Jl. Flamboyan No.10',    'Serang',           'Banten',       45, 'Male',   'joko.susilo@email.com'),
    (11, 'KARTIKA SARI',    'Jl. Bougenville No.11',  'Bandung',          'Jawa Barat',   27, 'Female', 'kartika.sari@email.com'),
    (12, 'LUKMAN HAKIM',    'Jl. Seroja No.12',       'Bekasi',           'Jawa Barat',   36, 'Male',   'lukman.hakim@email.com'),
    (13, 'MAYA ANGGRAINI',  'Jl. Teratai No.13',      'Semarang',         'Jawa Tengah',  32, 'Female', 'maya.anggraini@email.com'),
    (14, 'NUGROHO ADI',     'Jl. Kamboja No.14',      'Solo',             'Jawa Tengah',  40, 'Male',   'nugroho.adi@email.com'),
    (15, 'OKTAVIA PUTRI',   'Jl. Sakura No.15',       'Surabaya',         'Jawa Timur',   24, 'Female', 'oktavia.putri@email.com'),
    (16, 'PUTRA WIBOWO',    'Jl. Lotus No.16',        'Malang',           'Jawa Timur',   37, 'Male',   'putra.wibowo@email.com'),
    (17, 'RATNA KUSUMA',    'Jl. Lavender No.17',     'Jakarta Selatan',  'DKI Jakarta',  26, 'Female', 'ratna.kusuma@email.com'),
    (18, 'SURYA DARMA',     'Jl. Jasmine No.18',      'Jakarta Pusat',    'DKI Jakarta',  34, 'Male',   'surya.darma@email.com'),
    (19, 'TIKA AMALIA',     'Jl. Orchid No.19',       'Tangerang',        'Banten',       39, 'Female', 'tika.amalia@email.com'),
    (20, 'UMAR FADHIL',     'Jl. Lily No.20',         'Serang',           'Banten',       41, 'Male',   'umar.fadhil@email.com'),
    (21, 'VINA MAHARANI',   'Jl. Iris No.21',         'Bandung',          'Jawa Barat',   23, 'Female', 'vina.maharani@email.com'),
    (22, 'WAHYU PRASETYO',  'Jl. Peony No.22',        'Semarang',         'Jawa Tengah',  44, 'Male',   'wahyu.prasetyo@email.com'),
    (23, 'XENA PERMATA',    'Jl. Violet No.23',       'Surabaya',         'Jawa Timur',   28, 'Female', 'xena.permata@email.com'),
    (24, 'YOGA FIRMANSYAH', 'Jl. Aster No.24',        'Jakarta Selatan',  'DKI Jakarta',  35, 'Male',   'yoga.firmansyah@email.com'),
    (25, 'ZAHRA NABILAH',   'Jl. Magnolia No.25',     'Tangerang',        'Banten',       30, 'Female', 'zahra.nabilah@email.com')
) AS source (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email)
ON target.CustomerID = source.CustomerID
WHEN NOT MATCHED THEN
    INSERT (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email)
    VALUES (source.CustomerID, source.CustomerName, source.Address, source.CityName,
            source.StateName, source.Age, source.Gender, source.Email);
GO

-- ============================================================================
-- FactTransaction (equivalent to Talend job: Load_FactTransaction)
-- Source: transaction_db UNION transaction_csv UNION transaction_excel
-- After deduplication by transaction_id (tUniqRow), the combined set is:
--   IDs 1-25 (13 from DB, 12 from CSV, 7 from Excel; overlapping IDs deduplicated)
-- ============================================================================
PRINT 'Loading FactTransaction...';

MERGE INTO FactTransaction AS target
USING (VALUES
    -- From SQL Server (transaction_db): IDs 1-13
    (1,  1,  '2024-01-18 09:00:00', 500000,   'Deposit',    1),
    (2,  2,  '2024-01-18 10:30:00', 200000,   'Withdrawal', 2),
    (3,  3,  '2024-01-18 11:00:00', 1000000,  'Transfer',   3),
    (4,  4,  '2024-01-18 14:00:00', 300000,   'Payment',    4),
    (5,  5,  '2024-01-19 09:30:00', 750000,   'Deposit',    5),
    (6,  6,  '2024-01-18 13:10:00', 50000,    'Withdrawal', 1),
    (7,  6,  '2024-01-19 14:00:00', 100000,   'Payment',    1),
    (8,  7,  '2024-01-19 10:00:00', 250000,   'Deposit',    2),
    (9,  8,  '2024-01-19 11:45:00', 180000,   'Transfer',   3),
    (10, 9,  '2024-01-20 08:30:00', 600000,   'Deposit',    4),
    (11, 10, '2024-01-20 15:00:00', 1000000,  'Transfer',   1),
    (12, 11, '2024-01-20 10:00:00', 500000,   'Deposit',    1),
    (13, 12, '2024-01-20 12:10:00', 500000,   'Withdrawal', 5),
    -- From CSV (transaction_csv.csv): IDs 14-25
    (14, 13, '2024-01-21 14:00:00', 1500000,  'Deposit',    4),
    (15, 14, '2024-01-21 08:00:00', 500000,   'Transfer',   3),
    (16, 15, '2024-01-22 09:00:00', 100000,   'Deposit',    1),
    (17, 16, '2024-01-22 13:10:00', 100000,   'Withdrawal', 5),
    (18, 17, '2024-01-22 10:20:00', 700000,   'Deposit',    5),
    (19, 18, '2024-01-22 11:00:00', 30000,    'Payment',    2),
    (20, 19, '2024-01-22 15:00:00', 2500000,  'Deposit',    2),
    (21, 20, '2024-01-22 11:30:00', 150000,   'Payment',    4),
    (22, 21, '2024-01-22 10:45:00', 800000,   'Withdrawal', 5),
    (23, 22, '2024-01-22 10:50:00', 100000,   'Withdrawal', 1),
    (24, 23, '2024-01-22 11:10:00', 300000,   'Payment',    1),
    (25, 23, '2024-01-22 14:30:00', 400000,   'Deposit',    1)
    -- Note: Excel rows (IDs 6-7, 11-15) overlap with DB/CSV and are
    -- deduplicated by tUniqRow, so only the first occurrence is kept.
) AS source (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
ON target.TransactionID = source.TransactionID
WHEN NOT MATCHED THEN
    INSERT (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES (source.TransactionID, source.AccountID,
            CAST(source.TransactionDate AS DATETIME), source.Amount,
            source.TransactionType, source.BranchID);
GO

PRINT 'All DWH seed data has been loaded.';
PRINT '';
PRINT 'Summary:';
PRINT '  - DimBranch:       5 rows';
PRINT '  - DimAccount:      25 rows';
PRINT '  - DimCustomer:     25 rows';
PRINT '  - FactTransaction: 25 rows';
PRINT '';
PRINT 'You can now test the stored procedures:';
PRINT '  EXEC sp_DailyTransaction @start_date = ''2024-01-18'', @end_date = ''2024-01-22'';';
PRINT '  EXEC sp_BalancePerCustomer @customer_name = ''ANDI'';';
