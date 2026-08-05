# Unit Test Stored Procedure (tSQLt)

Automated test suite untuk kedua stored procedure DWH (`sp_DailyTransaction` dan
`sp_BalancePerCustomer`), dibangun dengan [tSQLt](https://tsqlt.org/) — framework
unit test standar untuk T-SQL.

Semua test memakai `tSQLt.FakeTable` untuk mengganti `FactTransaction`,
`DimAccount`, dan `DimCustomer` dengan tabel kosong tanpa constraint, sehingga
test berjalan di atas fixture data yang terkontrol dan **tidak** membutuhkan data
hasil ETL. Setiap test berjalan dalam transaksi tersendiri dan otomatis di-rollback
oleh tSQLt.

## Isi folder

| File | Keterangan |
| --- | --- |
| `00_install_tsqlt.sql` | Prasyarat instance/database (CLR, `TRUSTWORTHY`) sebelum framework tSQLt dipasang. |
| `10_ProcedureTests_class.sql` | Membuat test class `ProcedureTests` + prosedur `SetUp` (FakeTable). |
| `20_Test_sp_DailyTransaction.sql` | 5 test untuk `sp_DailyTransaction`. |
| `30_Test_sp_BalancePerCustomer.sql` | 5 test untuk `sp_BalancePerCustomer`. |
| `run_tests.sql` | Runner: `EXEC tSQLt.RunAll;`. |
| `run_tests.sh` | Skrip otomatis: container Docker + unduh tSQLt + jalankan semua skrip di atas. |

## Cakupan test

**`sp_DailyTransaction`**
1. Pengelompokan per hari dengan `COUNT`/`SUM` yang benar.
2. Filter rentang tanggal inklusif pada kedua batas; baris di luar rentang dikecualikan.
3. Komponen jam pada `TransactionDate` diabaikan (`CAST ... AS DATE`), termasuk transaksi 23:59 pada tanggal batas akhir.
4. Hasil kosong bila tidak ada transaksi dalam rentang.
5. Hasil terurut menaik berdasarkan tanggal.

**`sp_BalancePerCustomer`**
1. Logika nominal bertanda: `Deposit` menambah, tipe lain (`Withdrawal`, `Transfer`) mengurangi; `CurrentBalance = InitialBalance + total perubahan`.
2. Rekening tanpa transaksi tetap tampil dengan `CurrentBalance = InitialBalance` (LEFT JOIN + `ISNULL(...,0)`).
3. Rekening dengan `Status <> 'active'` dikecualikan.
4. Pencocokan nama sebagian (`LIKE '%nama%'`); nasabah lain tidak ikut tampil.
5. Setiap rekening aktif milik satu nasabah dihitung terpisah.

## Cara menjalankan (otomatis)

Membutuhkan Docker dan koneksi internet (untuk mengunduh image SQL Server dan
paket tSQLt):

```bash
./tests/run_tests.sh
```

Skrip akan menyiapkan container, membuat ulang database `DWH`, memasang tSQLt,
memuat seluruh test, lalu menjalankan `EXEC tSQLt.RunAll;`.

## Cara menjalankan (manual)

### 1. Provision SQL Server

```bash
docker run -d --name mssql-tsqlt \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=Devin_Test_2024!" \
  -e "MSSQL_PID=Developer" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

### 2. Unduh framework tSQLt

Framework tidak di-commit ke repo ini; unduh paketnya (berisi `tSQLt.class.sql`
dan `PrepareServer.sql`; pada rilis lama file prasyaratnya bernama
`SetClrEnabled.sql`):

```bash
curl -L -o tSQLt.zip https://tsqlt.org/download/tsqlt/
unzip tSQLt.zip -d .tsqlt
```

### 3. Salin skrip ke dalam container

```bash
docker exec mssql-tsqlt mkdir -p /tmp/scripts
docker cp sql_scripts/. mssql-tsqlt:/tmp/scripts/sql_scripts/
docker cp tests/.       mssql-tsqlt:/tmp/scripts/tests/
docker cp .tsqlt/tSQLt.class.sql   mssql-tsqlt:/tmp/scripts/tSQLt.class.sql
docker cp .tsqlt/PrepareServer.sql mssql-tsqlt:/tmp/scripts/PrepareServer.sql
```

### 4. Jalankan skrip secara berurutan

```bash
SQLCMD="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P Devin_Test_2024! -C -b"

docker exec mssql-tsqlt bash -c "$SQLCMD -d master -i /tmp/scripts/sql_scripts/01_create_tables.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d DWH    -i /tmp/scripts/sql_scripts/02_create_procedures.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d master -i /tmp/scripts/PrepareServer.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d master -i /tmp/scripts/tests/00_install_tsqlt.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d DWH    -i /tmp/scripts/tSQLt.class.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d DWH    -i /tmp/scripts/tests/10_ProcedureTests_class.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d DWH    -i /tmp/scripts/tests/20_Test_sp_DailyTransaction.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d DWH    -i /tmp/scripts/tests/30_Test_sp_BalancePerCustomer.sql"
docker exec mssql-tsqlt bash -c "$SQLCMD -d DWH    -i /tmp/scripts/tests/run_tests.sql"
```

Untuk instance SQL Server yang sudah ada (bukan Docker), jalankan urutan file yang
sama dengan `sqlcmd -S <server> -i <file>` atau via SSMS dengan urutan:
`01_create_tables.sql` → `02_create_procedures.sql` → `PrepareServer.sql` →
`00_install_tsqlt.sql` → `tSQLt.class.sql` (di database `DWH`) →
`10_*` → `20_*` → `30_*` → `run_tests.sql`.

## Hasil eksekusi terakhir

```
+----------------------+
|Test Execution Summary|
+----------------------+

|No|Test Case Name                                                                                              |Dur(ms)|Result |
+--+------------------------------------------------------------------------------------------------------------+-------+-------+
|1 |[ProcedureTests].[test sp_BalancePerCustomer menampilkan rekening tanpa transaksi dengan saldo awal]        |     41|Success|
|2 |[ProcedureTests].[test sp_BalancePerCustomer mencocokkan nama secara sebagian]                              |     49|Success|
|3 |[ProcedureTests].[test sp_BalancePerCustomer mengecualikan rekening tidak aktif]                            |     41|Success|
|4 |[ProcedureTests].[test sp_BalancePerCustomer menghitung saldo per rekening aktif nasabah]                   |    781|Success|
|5 |[ProcedureTests].[test sp_BalancePerCustomer menjumlahkan transaksi bertanda ke saldo awal]                 |     37|Success|
|6 |[ProcedureTests].[test sp_DailyTransaction memfilter inklusif pada tanggal batas]                           |     37|Success|
|7 |[ProcedureTests].[test sp_DailyTransaction mengabaikan komponen jam pada TransactionDate]                   |     40|Success|
|8 |[ProcedureTests].[test sp_DailyTransaction mengelompokkan dan mengagregasi per hari]                        |     40|Success|
|9 |[ProcedureTests].[test sp_DailyTransaction mengembalikan hasil kosong bila tidak ada transaksi pada rentang]|     25|Success|
|10|[ProcedureTests].[test sp_DailyTransaction mengurutkan hasil berdasarkan tanggal menaik]                    |     33|Success|
------------------------------------------------------------------------------------------
Test Case Summary: 10 test case(s) executed, 10 succeeded, 0 skipped, 0 failed, 0 errored.
------------------------------------------------------------------------------------------
```

## Catatan: perbaikan pada `02_create_procedures.sql`

Saat pertama kali dijalankan, `sql_scripts/02_create_procedures.sql` gagal dengan:

```
Msg 111, Level 15, State 1 - 'CREATE/ALTER PROCEDURE' must be the first statement in a query batch.
```

Penyebabnya adalah statement `PRINT 'Creating Stored Procedure ...'` berada pada
batch yang sama dengan `CREATE PROCEDURE`. Perbaikannya: menambahkan pemisah batch
`GO` setelah kedua statement `PRINT` tersebut. Tanpa perbaikan ini kedua stored
procedure tidak pernah terbentuk.
