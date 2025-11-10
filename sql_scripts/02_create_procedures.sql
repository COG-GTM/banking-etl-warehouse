/************************************************************************************
-- SCRIPT UNTUK MEMBUAT STORED PROCEDURES
-- Proyek: Final Task Data Engineer - ID/X Partners
-- Deskripsi: Skrip ini akan membuat dua Stored Procedure di dalam database DWH:
--            1. sp_DailyTransaction
--            2. sp_BalancePerCustomer
************************************************************************************/

-- Pastikan kita bekerja di database yang benar
USE DWH;
GO

-------------------------------------------------------------------------------------
-- STORED PROCEDURE 1: DailyTransaction
-- Menghasilkan ringkasan jumlah transaksi dan total nominal per hari.
-------------------------------------------------------------------------------------
PRINT 'Creating Stored Procedure sp_DailyTransaction...';
CREATE PROCEDURE sp_DailyTransaction
    -- Mendefinisikan dua parameter input: tanggal mulai dan tanggal selesai
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    -- Mencegah pesan "(x baris terpengaruh)" muncul di hasil
    SET NOCOUNT ON;

    -- Query utama untuk agregasi harian
    SELECT
        CAST(TransactionDate AS DATE) AS [Date],
        COUNT(TransactionID) AS TotalTransactions,
        SUM(Amount) AS TotalAmount
    FROM
        FactTransaction
    WHERE
        CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
    GROUP BY
        CAST(TransactionDate AS DATE)
    ORDER BY
        [Date];
END;
GO
PRINT 'Stored Procedure sp_DailyTransaction created successfully.';
GO

-------------------------------------------------------------------------------------
-- STORED PROCEDURE 2: BalancePerCustomer
-- Menghitung saldo akhir setiap rekening aktif milik seorang nasabah.
-------------------------------------------------------------------------------------
PRINT 'Creating Stored Procedure sp_BalancePerCustomer...';
CREATE PROCEDURE sp_BalancePerCustomer
    -- Mendefinisikan satu parameter input: nama customer yang ingin dicari
    @customer_name VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    -- Menggunakan Common Table Expression (CTE) untuk menghitung total perubahan saldo per rekening
    WITH TransactionSummary AS (
        SELECT
            AccountID,
            -- Logika: Deposit menambah, selain itu mengurangi
            SUM(
                CASE
                    WHEN TransactionType = 'Deposit' THEN Amount
                    ELSE -Amount
                END
            ) AS TotalTransactionAmount
        FROM
            FactTransaction
        GROUP BY
            AccountID
    )
    -- Query utama untuk menampilkan hasil akhir
    SELECT
        c.CustomerName,
        a.AccountType,
        a.Balance AS InitialBalance,
        -- Saldo Akhir = Saldo Awal + Total Perubahan Saldo
        a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
    FROM
        DimCustomer c
    JOIN
        DimAccount a ON c.CustomerID = a.CustomerID
    LEFT JOIN
        TransactionSummary ts ON a.AccountID = ts.AccountID
    WHERE
        -- Filter berdasarkan nama dari parameter dan hanya untuk rekening yang aktif
        c.CustomerName LIKE '%' + @customer_name + '%'
        AND a.Status = 'active';
END;
GO
PRINT 'Stored Procedure sp_BalancePerCustomer created successfully.';
GO

PRINT 'All Stored Procedures have been created.';