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

-------------------------------------------------------------------------------------
-- STORED PROCEDURE 3: SampleExchangeRate
-- Returns ONE exchange rate for a currency pair, randomly sampled from the rates
-- observed in DimExchangeRate over the 6 months ending at @AsOfDate.
--
-- !! NON-DETERMINISTIC !!
-- The rate is drawn at random (ORDER BY NEWID()) from the trailing 6-month window,
-- so two identical calls will usually return different rates. This is intended for
-- simulation, stress-testing and what-if analysis only. Do NOT use it for
-- accounting, settlement, regulatory reporting or any other use case that requires
-- the exact historical rate for a given date: for those, query DimExchangeRate
-- directly on the specific RateDate.
-------------------------------------------------------------------------------------
PRINT 'Creating Stored Procedure sp_SampleExchangeRate...';
GO
CREATE PROCEDURE sp_SampleExchangeRate
    @CurrencyFrom VARCHAR(3),
    @CurrencyTo VARCHAR(3),
    @AsOfDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Sampling window: the 6 months up to and including @AsOfDate
    DECLARE @WindowStart DATE = DATEADD(MONTH, -6, @AsOfDate);

    SELECT TOP 1
        r.RateID,
        r.CurrencyFrom,
        r.CurrencyTo,
        r.RateDate       AS SampledRateDate,
        r.Rate           AS SampledRate,
        @AsOfDate        AS AsOfDate,
        @WindowStart     AS WindowStartDate
    FROM
        DimExchangeRate r
    WHERE
        r.CurrencyFrom = @CurrencyFrom
        AND r.CurrencyTo = @CurrencyTo
        AND r.RateDate BETWEEN @WindowStart AND @AsOfDate
    -- Random draw: NEWID() assigns a fresh random value to every candidate row
    ORDER BY
        NEWID();

    -- No history in the window: surface it instead of silently returning nothing
    IF @@ROWCOUNT = 0
    BEGIN
        DECLARE @WindowStartText VARCHAR(10) = CONVERT(VARCHAR(10), @WindowStart, 23);
        DECLARE @AsOfDateText VARCHAR(10) = CONVERT(VARCHAR(10), @AsOfDate, 23);

        RAISERROR('No exchange rates found for %s/%s between %s and %s.', 16, 1,
                  @CurrencyFrom, @CurrencyTo,
                  @WindowStartText, @AsOfDateText);
    END
END;
GO
PRINT 'Stored Procedure sp_SampleExchangeRate created successfully.';
GO

-------------------------------------------------------------------------------------
-- VIEW: vw_FactTransactionSampledUSD
-- Demonstrates converting FactTransaction.Amount with a rate sampled at random from
-- the trailing 6 months of history for the transaction's own currency pair.
--
-- !! NON-DETERMINISTIC !!
-- Every execution re-samples, so ConvertedAmount changes between runs. Simulation
-- and what-if analysis only; never use it as a source for reported figures.
-------------------------------------------------------------------------------------
PRINT 'Creating View vw_FactTransactionSampledUSD...';
GO
CREATE VIEW vw_FactTransactionSampledUSD
AS
    SELECT
        f.TransactionID,
        f.AccountID,
        f.TransactionDate,
        f.Amount,
        f.CurrencyCode,
        s.SampledRateDate,
        -- Transactions already in the base currency convert at parity
        COALESCE(s.SampledRate, CASE WHEN f.CurrencyCode = 'USD' THEN 1.0 END) AS SampledRate,
        CAST(f.Amount * COALESCE(s.SampledRate, CASE WHEN f.CurrencyCode = 'USD' THEN 1.0 END)
             AS DECIMAL(19,6)) AS ConvertedAmountUSD
    FROM
        FactTransaction f
    OUTER APPLY (
        SELECT TOP 1
            r.RateDate AS SampledRateDate,
            r.Rate     AS SampledRate
        FROM
            DimExchangeRate r
        WHERE
            r.CurrencyFrom = f.CurrencyCode
            AND r.CurrencyTo = 'USD'
            AND r.RateDate BETWEEN DATEADD(MONTH, -6, CAST(f.TransactionDate AS DATE))
                               AND CAST(f.TransactionDate AS DATE)
        ORDER BY
            NEWID()
    ) s;
GO
PRINT 'View vw_FactTransactionSampledUSD created successfully.';
GO

PRINT 'All Stored Procedures have been created.';