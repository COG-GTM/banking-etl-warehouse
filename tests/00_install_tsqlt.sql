/************************************************************************************
-- SCRIPT BOOTSTRAP tSQLt
-- Proyek: Final Task Data Engineer - ID/X Partners
-- Deskripsi: Menyiapkan instance & database DWH agar framework unit test tSQLt
--            dapat diinstal dan dijalankan.
--
-- Prasyarat (dijalankan SEBELUM skrip ini):
--   1. sql_scripts/01_create_tables.sql
--   2. sql_scripts/02_create_procedures.sql
--
-- Cara mendapatkan framework tSQLt (tidak di-commit ke repo ini karena
-- berlisensi Apache 2.0 dan dirilis sebagai artifact terpisah):
--   curl -L -o tSQLt.zip https://tsqlt.org/download/tsqlt/
--   unzip tSQLt.zip -d tsqlt
-- Isi paket: tSQLt.class.sql (framework) dan PrepareServer.sql
-- (pada rilis lama file prasyarat ini bernama SetClrEnabled.sql - keduanya
--  melakukan hal yang sama: mengaktifkan CLR pada instance SQL Server).
--
-- Urutan eksekusi tSQLt:
--   a. tsqlt/PrepareServer.sql   (atau SetClrEnabled.sql) -> level instance
--   b. skrip ini (00_install_tsqlt.sql)                   -> level database DWH
--   c. tsqlt/tSQLt.class.sql     dijalankan di database DWH
************************************************************************************/

-- Langkah 1: Aktifkan CLR di level instance (idempoten).
-- Catatan: PrepareServer.sql / SetClrEnabled.sql bawaan tSQLt melakukan hal yang
-- sama; blok ini disertakan agar skrip ini tetap aman dijalankan sendiri.
EXEC sys.sp_configure @configname = 'show advanced options', @configvalue = 1;
RECONFIGURE;
GO
EXEC sys.sp_configure @configname = 'clr enabled', @configvalue = 1;
RECONFIGURE;
GO
-- SQL Server 2017+ : 'clr strict security' harus dimatikan agar assembly tSQLt
-- yang tidak ditandatangani dapat dimuat.
IF EXISTS (SELECT 1 FROM sys.configurations WHERE name = 'clr strict security')
BEGIN
    EXEC sys.sp_configure @configname = 'clr strict security', @configvalue = 0;
    RECONFIGURE;
END
GO

-- Langkah 2: Database DWH harus TRUSTWORTHY agar assembly tSQLt dapat dijalankan.
USE master;
GO
ALTER DATABASE DWH SET TRUSTWORTHY ON;
GO

USE DWH;
GO
PRINT 'Database DWH siap untuk instalasi tSQLt. Jalankan tsqlt/tSQLt.class.sql pada database DWH.';
GO
