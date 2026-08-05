#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Menjalankan seluruh unit test tSQLt terhadap SQL Server di dalam Docker.
#
# Pemakaian:
#   ./tests/run_tests.sh
#
# Variabel lingkungan opsional:
#   SA_PASSWORD   password sa (default: Devin_Test_2024!)
#   CONTAINER     nama container (default: mssql-tsqlt)
#   IMAGE         image SQL Server (default: mcr.microsoft.com/mssql/server:2022-latest)
#
# Skrip ini idempoten: container dan framework tSQLt dibuat ulang bila perlu.
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SA_PASSWORD="${SA_PASSWORD:-Devin_Test_2024!}"
CONTAINER="${CONTAINER:-mssql-tsqlt}"
IMAGE="${IMAGE:-mcr.microsoft.com/mssql/server:2022-latest}"
TSQLT_DIR="${TSQLT_DIR:-$REPO_ROOT/.tsqlt}"
SQLCMD="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -C -b"

# 1. Unduh framework tSQLt (tidak di-commit ke repo).
if [ ! -f "$TSQLT_DIR/tSQLt.class.sql" ]; then
  echo "==> Mengunduh framework tSQLt ..."
  mkdir -p "$TSQLT_DIR"
  curl -sL -o "$TSQLT_DIR/tSQLt.zip" https://tsqlt.org/download/tsqlt/
  unzip -o -q "$TSQLT_DIR/tSQLt.zip" -d "$TSQLT_DIR"
fi

# 2. Jalankan container SQL Server.
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  echo "==> Menjalankan container SQL Server ..."
  docker run -d --name "$CONTAINER" \
    -e "ACCEPT_EULA=Y" \
    -e "MSSQL_SA_PASSWORD=$SA_PASSWORD" \
    -e "MSSQL_PID=Developer" \
    -p 1433:1433 "$IMAGE" >/dev/null
fi

echo "==> Menunggu SQL Server siap ..."
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" bash -c "$SQLCMD -Q 'SELECT 1' " >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# 3. Salin skrip ke dalam container dan jalankan secara berurutan.
docker exec "$CONTAINER" mkdir -p /tmp/scripts
docker cp "$REPO_ROOT/sql_scripts/." "$CONTAINER:/tmp/scripts/sql_scripts/" >/dev/null
docker cp "$REPO_ROOT/tests/." "$CONTAINER:/tmp/scripts/tests/" >/dev/null
docker cp "$TSQLT_DIR/tSQLt.class.sql" "$CONTAINER:/tmp/scripts/tSQLt.class.sql" >/dev/null
docker cp "$TSQLT_DIR/PrepareServer.sql" "$CONTAINER:/tmp/scripts/PrepareServer.sql" >/dev/null

run_sql() { # run_sql <path-di-container> [database]
  local file="$1" db="${2:-master}"
  echo "==> sqlcmd -d $db -i $file"
  docker exec "$CONTAINER" bash -c "$SQLCMD -d $db -i $file"
}

# Database DWH dibuat ulang agar setiap eksekusi bersih.
docker exec "$CONTAINER" bash -c "$SQLCMD -Q \"IF DB_ID('DWH') IS NOT NULL BEGIN ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE DWH; END\""

run_sql /tmp/scripts/sql_scripts/01_create_tables.sql
run_sql /tmp/scripts/sql_scripts/02_create_procedures.sql DWH
run_sql /tmp/scripts/PrepareServer.sql
run_sql /tmp/scripts/tests/00_install_tsqlt.sql
run_sql /tmp/scripts/tSQLt.class.sql DWH
run_sql /tmp/scripts/tests/10_ProcedureTests_class.sql DWH
run_sql /tmp/scripts/tests/20_Test_sp_DailyTransaction.sql DWH
run_sql /tmp/scripts/tests/30_Test_sp_BalancePerCustomer.sql DWH
run_sql /tmp/scripts/tests/run_tests.sql DWH
