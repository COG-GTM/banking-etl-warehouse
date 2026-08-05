/************************************************************************************
-- UNIT TEST: sp_BalancePerCustomer
-- Deskripsi: Menguji logika saldo bertanda (Deposit menambah, selain itu
--            mengurangi), perhitungan CurrentBalance, perilaku ISNULL untuk
--            rekening tanpa transaksi (LEFT JOIN), filter Status = 'active',
--            dan filter pencarian nama LIKE '%nama%'.
--
-- Prasyarat: 10_ProcedureTests_class.sql sudah dijalankan (membuat class + SetUp
--            yang mem-fake FactTransaction, DimAccount, DimCustomer).
************************************************************************************/
USE DWH;
GO

-------------------------------------------------------------------------------------
-- Test 1: Deposit menambah saldo, transaksi non-Deposit mengurangi saldo.
--         CurrentBalance = InitialBalance + total perubahan bertanda.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_BalancePerCustomer menjumlahkan transaksi bertanda ke saldo awal]
AS
BEGIN
    INSERT INTO dbo.DimCustomer (CustomerID, CustomerName) VALUES (1, 'Budi Santoso');
    INSERT INTO dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, Status)
        VALUES (10, 1, 'Savings', 1000.00, 'active');
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 10, '2024-01-01 10:00:00', 500.00, 'Deposit',    1),
        (2, 10, '2024-01-02 10:00:00', 200.00, 'Withdrawal', 1),
        (3, 10, '2024-01-03 10:00:00', 100.00, 'Transfer',   1);

    CREATE TABLE #Actual (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Actual EXEC dbo.sp_BalancePerCustomer @customer_name = 'Budi Santoso';

    CREATE TABLE #Expected (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    -- 1000 + 500 - 200 - 100 = 1200
    INSERT INTO #Expected VALUES ('Budi Santoso', 'Savings', 1000.00, 1200.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 2: Rekening tanpa transaksi tetap muncul (LEFT JOIN) dengan
--         CurrentBalance = InitialBalance berkat ISNULL(..., 0).
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_BalancePerCustomer menampilkan rekening tanpa transaksi dengan saldo awal]
AS
BEGIN
    INSERT INTO dbo.DimCustomer (CustomerID, CustomerName) VALUES (1, 'Siti Aminah');
    INSERT INTO dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, Status)
        VALUES (20, 1, 'Checking', 750.50, 'active');
    -- Sengaja tidak ada baris pada FactTransaction untuk AccountID 20.

    CREATE TABLE #Actual (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Actual EXEC dbo.sp_BalancePerCustomer @customer_name = 'Siti';

    CREATE TABLE #Expected (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Expected VALUES ('Siti Aminah', 'Checking', 750.50, 750.50);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 3: Rekening dengan Status selain 'active' tidak ditampilkan.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_BalancePerCustomer mengecualikan rekening tidak aktif]
AS
BEGIN
    INSERT INTO dbo.DimCustomer (CustomerID, CustomerName) VALUES (1, 'Agus Wijaya');
    INSERT INTO dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, Status)
    VALUES
        (30, 1, 'Savings',  100.00, 'active'),
        (31, 1, 'Checking', 900.00, 'inactive');
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 30, '2024-01-01 10:00:00', 50.00, 'Deposit', 1),
        (2, 31, '2024-01-01 10:00:00', 50.00, 'Deposit', 1);

    CREATE TABLE #Actual (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Actual EXEC dbo.sp_BalancePerCustomer @customer_name = 'Agus';

    CREATE TABLE #Expected (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Expected VALUES ('Agus Wijaya', 'Savings', 100.00, 150.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 4: Filter nama bersifat pencarian sebagian (LIKE '%nama%'); nasabah lain
--         tidak ikut muncul.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_BalancePerCustomer mencocokkan nama secara sebagian]
AS
BEGIN
    INSERT INTO dbo.DimCustomer (CustomerID, CustomerName)
    VALUES
        (1, 'Budi Santoso'),
        (2, 'Siti Aminah');
    INSERT INTO dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, Status)
    VALUES
        (40, 1, 'Savings',  100.00, 'active'),
        (41, 2, 'Checking', 200.00, 'active');

    CREATE TABLE #Actual (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    -- 'udi' hanya cocok di tengah nama 'Budi Santoso'
    INSERT INTO #Actual EXEC dbo.sp_BalancePerCustomer @customer_name = 'udi';

    CREATE TABLE #Expected (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Expected VALUES ('Budi Santoso', 'Savings', 100.00, 100.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 5: Semua rekening aktif milik satu nasabah dihitung terpisah per rekening.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_BalancePerCustomer menghitung saldo per rekening aktif nasabah]
AS
BEGIN
    INSERT INTO dbo.DimCustomer (CustomerID, CustomerName) VALUES (1, 'Dewi Lestari');
    INSERT INTO dbo.DimAccount (AccountID, CustomerID, AccountType, Balance, Status)
    VALUES
        (50, 1, 'Savings',  1000.00, 'active'),
        (51, 1, 'Checking',  500.00, 'active');
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 50, '2024-01-01 10:00:00', 250.00, 'Deposit',    1),
        (2, 51, '2024-01-01 10:00:00', 100.00, 'Withdrawal', 1);

    CREATE TABLE #Actual (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Actual EXEC dbo.sp_BalancePerCustomer @customer_name = 'Dewi';

    CREATE TABLE #Expected (CustomerName VARCHAR(100), AccountType VARCHAR(50), InitialBalance MONEY, CurrentBalance MONEY);
    INSERT INTO #Expected VALUES
        ('Dewi Lestari', 'Savings',  1000.00, 1250.00),
        ('Dewi Lestari', 'Checking',  500.00,  400.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO
