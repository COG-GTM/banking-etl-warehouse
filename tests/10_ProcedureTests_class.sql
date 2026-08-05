/************************************************************************************
-- TEST CLASS: ProcedureTests
-- Deskripsi: Membuat (ulang) test class beserta prosedur SetUp yang dipakai oleh
--            seluruh unit test stored procedure DWH.
--
-- PENTING: tSQLt.NewTestClass menghapus schema beserta seluruh test di dalamnya,
-- sehingga skrip ini HARUS dijalankan sebelum 20_* dan 30_*.
************************************************************************************/
USE DWH;
GO

EXEC tSQLt.NewTestClass 'ProcedureTests';
GO

/*
  SetUp dijalankan otomatis oleh tSQLt sebelum setiap test di class ini.
  FakeTable mengganti tabel asli dengan tabel kosong tanpa constraint/FK,
  sehingga test berjalan di atas fixture data yang terkontrol dan tidak
  bergantung pada hasil ETL.
*/
CREATE PROCEDURE ProcedureTests.[SetUp]
AS
BEGIN
    EXEC tSQLt.FakeTable @TableName = 'dbo.FactTransaction';
    EXEC tSQLt.FakeTable @TableName = 'dbo.DimAccount';
    EXEC tSQLt.FakeTable @TableName = 'dbo.DimCustomer';
END;
GO
