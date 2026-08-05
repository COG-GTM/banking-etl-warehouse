/************************************************************************************
-- RUNNER: menjalankan seluruh unit test tSQLt pada database DWH.
--
-- Jalankan setelah (berurutan):
--   1. sql_scripts/01_create_tables.sql
--   2. sql_scripts/02_create_procedures.sql
--   3. tsqlt/PrepareServer.sql  (prasyarat CLR dari paket tSQLt)
--   4. tests/00_install_tsqlt.sql
--   5. tsqlt/tSQLt.class.sql    (dijalankan pada database DWH)
--   6. tests/10_ProcedureTests_class.sql
--   7. tests/20_Test_sp_DailyTransaction.sql
--   8. tests/30_Test_sp_BalancePerCustomer.sql
--
-- Lihat tests/README.md untuk perintah docker + sqlcmd lengkap.
************************************************************************************/
USE DWH;
GO

EXEC tSQLt.RunAll;
GO
