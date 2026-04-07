/************************************************************************************
-- SCRIPT TO SEED THE SOURCE DATABASE WITH REPRESENTATIVE SAMPLE DATA
-- Project: Banking ETL Data Warehouse
-- Description: Populates the [sample] database with realistic banking data so the
--              full ETL pipeline can be tested without restoring sample.bak.
--
-- Data volumes (small but sufficient to exercise every ETL path):
--   - 5 states, 10 cities, 5 branches
--   - 25 customers, 25 accounts
--   - 13 transactions (from SQL Server; CSV and Excel add more)
--
-- The seed data is intentionally designed so that:
--   1. Every branch, account, and customer has at least one transaction.
--   2. Multiple transaction types exist (Deposit, Withdrawal, Transfer, Payment).
--   3. Date ranges span 2024-01-18 to 2024-01-22 to match CSV/Excel sources.
--   4. The stored procedures (sp_DailyTransaction, sp_BalancePerCustomer)
--      return meaningful results on this data.
************************************************************************************/

USE sample;
GO

-- ============================================================================
-- STATE reference data
-- ============================================================================
PRINT 'Seeding state data...';

MERGE INTO state AS target
USING (VALUES
    (1, 'Jawa Barat'),
    (2, 'Jawa Tengah'),
    (3, 'Jawa Timur'),
    (4, 'DKI Jakarta'),
    (5, 'Banten')
) AS source (state_id, state_name)
ON target.state_id = source.state_id
WHEN NOT MATCHED THEN
    INSERT (state_id, state_name) VALUES (source.state_id, source.state_name);
GO

-- ============================================================================
-- CITY reference data
-- ============================================================================
PRINT 'Seeding city data...';

MERGE INTO city AS target
USING (VALUES
    (1,  'Bandung',     1),
    (2,  'Bekasi',      1),
    (3,  'Semarang',    2),
    (4,  'Solo',        2),
    (5,  'Surabaya',    3),
    (6,  'Malang',      3),
    (7,  'Jakarta Selatan', 4),
    (8,  'Jakarta Pusat',   4),
    (9,  'Tangerang',   5),
    (10, 'Serang',      5)
) AS source (city_id, city_name, state_id)
ON target.city_id = source.city_id
WHEN NOT MATCHED THEN
    INSERT (city_id, city_name, state_id) VALUES (source.city_id, source.city_name, source.state_id);
GO

-- ============================================================================
-- BRANCH data
-- ============================================================================
PRINT 'Seeding branch data...';

MERGE INTO branch AS target
USING (VALUES
    (1, 'Branch Bandung',         'Jl. Braga No.1, Bandung'),
    (2, 'Branch Jakarta Selatan', 'Jl. Sudirman No.10, Jakarta'),
    (3, 'Branch Semarang',        'Jl. Pemuda No.5, Semarang'),
    (4, 'Branch Surabaya',        'Jl. Tunjungan No.8, Surabaya'),
    (5, 'Branch Tangerang',       'Jl. Raya Serpong No.3, Tangerang')
) AS source (branch_id, branch_name, branch_location)
ON target.branch_id = source.branch_id
WHEN NOT MATCHED THEN
    INSERT (branch_id, branch_name, branch_location) VALUES (source.branch_id, source.branch_name, source.branch_location);
GO

-- ============================================================================
-- CUSTOMER data
-- ============================================================================
PRINT 'Seeding customer data...';

MERGE INTO customer AS target
USING (VALUES
    (1,  'Andi Pratama',    'Jl. Merdeka No.1',     1, '30', 'Male',   'andi.pratama@email.com'),
    (2,  'Budi Santoso',    'Jl. Pahlawan No.2',    2, '35', 'Male',   'budi.santoso@email.com'),
    (3,  'Citra Dewi',      'Jl. Melati No.3',      3, '28', 'Female', 'citra.dewi@email.com'),
    (4,  'Dian Saputra',    'Jl. Kenanga No.4',     4, '42', 'Male',   'dian.saputra@email.com'),
    (5,  'Eka Rahmawati',   'Jl. Mawar No.5',       5, '25', 'Female', 'eka.rahmawati@email.com'),
    (6,  'Fajar Hidayat',   'Jl. Dahlia No.6',      6, '33', 'Male',   'fajar.hidayat@email.com'),
    (7,  'Gita Purnama',    'Jl. Anggrek No.7',     7, '29', 'Female', 'gita.purnama@email.com'),
    (8,  'Hendra Wijaya',   'Jl. Cempaka No.8',     8, '38', 'Male',   'hendra.wijaya@email.com'),
    (9,  'Indah Lestari',   'Jl. Tulip No.9',       9, '31', 'Female', 'indah.lestari@email.com'),
    (10, 'Joko Susilo',     'Jl. Flamboyan No.10', 10, '45', 'Male',   'joko.susilo@email.com'),
    (11, 'Kartika Sari',    'Jl. Bougenville No.11', 1, '27', 'Female', 'kartika.sari@email.com'),
    (12, 'Lukman Hakim',    'Jl. Seroja No.12',      2, '36', 'Male',   'lukman.hakim@email.com'),
    (13, 'Maya Anggraini',  'Jl. Teratai No.13',     3, '32', 'Female', 'maya.anggraini@email.com'),
    (14, 'Nugroho Adi',     'Jl. Kamboja No.14',     4, '40', 'Male',   'nugroho.adi@email.com'),
    (15, 'Oktavia Putri',   'Jl. Sakura No.15',      5, '24', 'Female', 'oktavia.putri@email.com'),
    (16, 'Putra Wibowo',    'Jl. Lotus No.16',       6, '37', 'Male',   'putra.wibowo@email.com'),
    (17, 'Ratna Kusuma',    'Jl. Lavender No.17',    7, '26', 'Female', 'ratna.kusuma@email.com'),
    (18, 'Surya Darma',     'Jl. Jasmine No.18',     8, '34', 'Male',   'surya.darma@email.com'),
    (19, 'Tika Amalia',     'Jl. Orchid No.19',      9, '39', 'Female', 'tika.amalia@email.com'),
    (20, 'Umar Fadhil',     'Jl. Lily No.20',       10, '41', 'Male',   'umar.fadhil@email.com'),
    (21, 'Vina Maharani',   'Jl. Iris No.21',        1, '23', 'Female', 'vina.maharani@email.com'),
    (22, 'Wahyu Prasetyo',  'Jl. Peony No.22',       3, '44', 'Male',   'wahyu.prasetyo@email.com'),
    (23, 'Xena Permata',    'Jl. Violet No.23',      5, '28', 'Female', 'xena.permata@email.com'),
    (24, 'Yoga Firmansyah', 'Jl. Aster No.24',       7, '35', 'Male',   'yoga.firmansyah@email.com'),
    (25, 'Zahra Nabilah',   'Jl. Magnolia No.25',    9, '30', 'Female', 'zahra.nabilah@email.com')
) AS source (customer_id, customer_name, address, city_id, age, gender, email)
ON target.customer_id = source.customer_id
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, address, city_id, age, gender, email)
    VALUES (source.customer_id, source.customer_name, source.address, source.city_id,
            source.age, source.gender, source.email);
GO

-- ============================================================================
-- ACCOUNT data
-- ============================================================================
PRINT 'Seeding account data...';

MERGE INTO account AS target
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
) AS source (account_id, customer_id, account_type, balance, date_opened, status)
ON target.account_id = source.account_id
WHEN NOT MATCHED THEN
    INSERT (account_id, customer_id, account_type, balance, date_opened, status)
    VALUES (source.account_id, source.customer_id, source.account_type, source.balance,
            CAST(source.date_opened AS DATETIME2), source.status);
GO

-- ============================================================================
-- TRANSACTION_DB data (SQL Server source transactions)
-- These are the transactions that come from the relational DB source.
-- The CSV and Excel files supply additional transactions (IDs 6-7, 11-25).
-- Together they form the complete FactTransaction after ETL deduplication.
-- ============================================================================
PRINT 'Seeding transaction_db data...';

MERGE INTO transaction_db AS target
USING (VALUES
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
    (13, 12, '2024-01-20 12:10:00', 500000,   'Withdrawal', 5)
) AS source (transaction_id, account_id, transaction_date, amount, transaction_type, branch_id)
ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED THEN
    INSERT (transaction_id, account_id, transaction_date, amount, transaction_type, branch_id)
    VALUES (source.transaction_id, source.account_id,
            CAST(source.transaction_date AS DATETIME2), source.amount,
            source.transaction_type, source.branch_id);
GO

PRINT 'All seed data for [sample] database has been loaded.';
PRINT '';
PRINT 'Summary:';
PRINT '  - 5 states';
PRINT '  - 10 cities';
PRINT '  - 5 branches';
PRINT '  - 25 customers';
PRINT '  - 25 accounts';
PRINT '  - 13 transactions (SQL Server source)';
PRINT '';
PRINT 'Additional transactions come from:';
PRINT '  - data_sources/transaction_csv.csv  (IDs 14-25)';
PRINT '  - data_sources/transaction_excel.xlsx (IDs 6-7, 11-15)';
PRINT 'These are loaded by the Talend Load_FactTransaction job.';
