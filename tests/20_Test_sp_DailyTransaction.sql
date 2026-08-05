/************************************************************************************
-- UNIT TEST: sp_DailyTransaction
-- Deskripsi: Menguji agregasi harian (COUNT/SUM), filter rentang tanggal yang
--            inklusif, penanganan komponen jam pada TransactionDate, urutan hasil,
--            dan hasil kosong ketika tidak ada transaksi pada rentang tersebut.
--
-- Prasyarat: 10_ProcedureTests_class.sql sudah dijalankan (membuat class + SetUp
--            yang mem-fake FactTransaction, DimAccount, DimCustomer).
************************************************************************************/
USE DWH;
GO

-------------------------------------------------------------------------------------
-- Test 1: Transaksi dikelompokkan per hari dengan COUNT dan SUM yang benar.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_DailyTransaction mengelompokkan dan mengagregasi per hari]
AS
BEGIN
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 100, '2024-01-01 09:00:00', 100.00, 'Deposit',    1),
        (2, 100, '2024-01-01 18:30:00',  50.00, 'Withdrawal', 1),
        (3, 101, '2024-01-02 12:00:00', 200.00, 'Deposit',    2);

    CREATE TABLE #Actual ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Actual EXEC dbo.sp_DailyTransaction @start_date = '2024-01-01', @end_date = '2024-01-31';

    CREATE TABLE #Expected ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Expected VALUES
        ('2024-01-01', 2, 150.00),
        ('2024-01-02', 1, 200.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 2: Filter rentang tanggal bersifat inklusif pada kedua batas, dan
--         transaksi di luar rentang tidak ikut terhitung.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_DailyTransaction memfilter inklusif pada tanggal batas]
AS
BEGIN
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 100, '2023-12-31 23:59:59',  10.00, 'Deposit', 1), -- sebelum rentang
        (2, 100, '2024-01-01 00:00:00',  20.00, 'Deposit', 1), -- batas awal
        (3, 100, '2024-01-03 10:00:00',  30.00, 'Deposit', 1), -- di dalam rentang
        (4, 100, '2024-01-05 08:00:00',  40.00, 'Deposit', 1), -- batas akhir
        (5, 100, '2024-01-06 00:00:00',  50.00, 'Deposit', 1); -- setelah rentang

    CREATE TABLE #Actual ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Actual EXEC dbo.sp_DailyTransaction @start_date = '2024-01-01', @end_date = '2024-01-05';

    CREATE TABLE #Expected ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Expected VALUES
        ('2024-01-01', 1, 20.00),
        ('2024-01-03', 1, 30.00),
        ('2024-01-05', 1, 40.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 3: Komponen jam diabaikan (CAST ke DATE), termasuk transaksi larut malam
--         pada tanggal batas akhir.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_DailyTransaction mengabaikan komponen jam pada TransactionDate]
AS
BEGIN
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 100, '2024-02-10 00:00:00',  15.00, 'Deposit',    1),
        (2, 100, '2024-02-10 13:45:12',  25.00, 'Withdrawal', 1),
        (3, 100, '2024-02-10 23:59:59',  60.00, 'Deposit',    1);

    CREATE TABLE #Actual ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Actual EXEC dbo.sp_DailyTransaction @start_date = '2024-02-10', @end_date = '2024-02-10';

    CREATE TABLE #Expected ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Expected VALUES ('2024-02-10', 3, 100.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 4: Hasil kosong bila tidak ada transaksi pada rentang tanggal.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_DailyTransaction mengembalikan hasil kosong bila tidak ada transaksi pada rentang]
AS
BEGIN
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES (1, 100, '2024-03-15 10:00:00', 99.00, 'Deposit', 1);

    CREATE TABLE #Actual ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Actual EXEC dbo.sp_DailyTransaction @start_date = '2024-04-01', @end_date = '2024-04-30';

    CREATE TABLE #Expected ([Date] DATE, TotalTransactions INT, TotalAmount MONEY);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO

-------------------------------------------------------------------------------------
-- Test 5: Hasil terurut menaik berdasarkan tanggal (ORDER BY [Date]).
--         Kolom IDENTITY dipakai untuk merekam urutan baris yang dikembalikan,
--         karena AssertEqualsTable membandingkan set tanpa memperhatikan urutan.
-------------------------------------------------------------------------------------
CREATE PROCEDURE ProcedureTests.[test sp_DailyTransaction mengurutkan hasil berdasarkan tanggal menaik]
AS
BEGIN
    INSERT INTO dbo.FactTransaction (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
    VALUES
        (1, 100, '2024-05-03 10:00:00', 30.00, 'Deposit', 1),
        (2, 100, '2024-05-01 10:00:00', 10.00, 'Deposit', 1),
        (3, 100, '2024-05-02 10:00:00', 20.00, 'Deposit', 1);

    CREATE TABLE #Actual (RowNo INT IDENTITY(1,1), [Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Actual ([Date], TotalTransactions, TotalAmount)
        EXEC dbo.sp_DailyTransaction @start_date = '2024-05-01', @end_date = '2024-05-31';

    CREATE TABLE #Expected (RowNo INT, [Date] DATE, TotalTransactions INT, TotalAmount MONEY);
    INSERT INTO #Expected VALUES
        (1, '2024-05-01', 1, 10.00),
        (2, '2024-05-02', 1, 20.00),
        (3, '2024-05-03', 1, 30.00);

    EXEC tSQLt.AssertEqualsTable '#Expected', '#Actual';
END;
GO
