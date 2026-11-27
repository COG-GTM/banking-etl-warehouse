-- =================================================================
-- SCRIPT UNTUK MEMBUAT INDEKS PADA DATA WAREHOUSE (DWH)
-- Deskripsi: Aman dijalankan ulang, baik pada DWH baru maupun DWH yang sudah ada.
-- =================================================================

USE DWH;
GO

-- Indeks agar sp_DailyTransaction mencari rentang tanggal tanpa memindai seluruh tabel fakta
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_FactTransaction_TransactionDate'
      AND object_id = OBJECT_ID('dbo.FactTransaction')
)
BEGIN
    PRINT 'Creating index IX_FactTransaction_TransactionDate...';
    CREATE NONCLUSTERED INDEX IX_FactTransaction_TransactionDate
        ON FactTransaction (TransactionDate)
        INCLUDE (Amount);
    PRINT 'Index IX_FactTransaction_TransactionDate created successfully.';
END
ELSE
    PRINT 'Index IX_FactTransaction_TransactionDate already exists.';
GO
