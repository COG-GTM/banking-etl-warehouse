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
GO
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
-- Menghitung saldo akhir setiap rekening aktif milik SATU nasabah.
-- Nasabah dicari berdasarkan @customer_id (direkomendasikan) atau nama lengkap yang
-- persis sama (@customer_name). Pencarian substring/wildcard (LIKE) sengaja tidak
-- didukung agar prosedur tidak bisa dipakai untuk menarik saldo seluruh nasabah
-- (mis. @customer_name = '%' atau '').
-------------------------------------------------------------------------------------
PRINT 'Creating Stored Procedure sp_BalancePerCustomer...';
GO
CREATE PROCEDURE dbo.sp_BalancePerCustomer
    -- Parameter input: nama lengkap nasabah (pencocokan persis, tanpa wildcard)
    @customer_name VARCHAR(100) = NULL,
    -- Parameter input: ID nasabah (kunci unik, lebih disarankan daripada nama)
    @customer_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @name VARCHAR(100) = NULLIF(LTRIM(RTRIM(@customer_name)), '');

    -- Wajib tepat satu kriteria: ID atau nama lengkap. Input kosong ditolak.
    IF (@customer_id IS NULL AND @name IS NULL)
       OR (@customer_id IS NOT NULL AND @name IS NOT NULL)
    BEGIN
        THROW 50001, 'sp_BalancePerCustomer: provide exactly one of @customer_id or a non-empty @customer_name.', 1;
    END;

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
        -- Filter persis berdasarkan ID atau nama lengkap (operator '=' tidak
        -- menafsirkan %, _ atau [ sebagai wildcard) dan hanya rekening aktif
        (
            (@customer_id IS NOT NULL AND c.CustomerID = @customer_id)
            OR (@customer_id IS NULL AND c.CustomerName = @name)
        )
        AND a.Status = 'active';
END;
GO
PRINT 'Stored Procedure sp_BalancePerCustomer created successfully.';
GO

-------------------------------------------------------------------------------------
-- HAK AKSES sp_BalancePerCustomer
-- Hanya anggota role balance_lookup_role yang boleh menjalankan prosedur ini.
-- Catatan: sengaja memakai REVOKE (bukan DENY) dari public, karena DENY ke public
-- juga akan memblokir anggota balance_lookup_role (DENY mengalahkan GRANT).
-------------------------------------------------------------------------------------
IF DATABASE_PRINCIPAL_ID('balance_lookup_role') IS NULL
    CREATE ROLE balance_lookup_role;
GO
REVOKE EXECUTE ON dbo.sp_BalancePerCustomer FROM public;
GRANT EXECUTE ON dbo.sp_BalancePerCustomer TO balance_lookup_role;
GO

PRINT 'All Stored Procedures have been created.';