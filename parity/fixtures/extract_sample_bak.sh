#!/usr/bin/env bash
# Regenerates parity/fixtures/source/*.csv from data_sources/sample.bak.
#
# The fixtures are committed, so this only needs to be re-run when sample.bak
# changes. Requires Docker (the script restores the backup into a throwaway
# SQL Server 2022 container).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONTAINER=parity_mssql_extract
PASSWORD='Parity!Pass123'
SQLCMD="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${PASSWORD} -C"

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" \
  -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="$PASSWORD" \
  -v "$REPO_ROOT/data_sources:/bak:ro" \
  mcr.microsoft.com/mssql/server:2022-latest >/dev/null

until docker exec "$CONTAINER" $SQLCMD -Q "SELECT 1" >/dev/null 2>&1; do sleep 2; done

docker exec "$CONTAINER" $SQLCMD -Q "RESTORE DATABASE sample FROM DISK='/bak/sample.bak' \
  WITH MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', \
       MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf'" >/dev/null

declare -A HEADERS=(
  [state]='state_id,state_name'
  [city]='city_id,city_name,state_id'
  [customer]='customer_id,customer_name,address,city_id,age,gender,email'
  [account]='account_id,customer_id,account_type,balance,date_opened,status'
  [branch]='branch_id,branch_name,branch_location'
  [transaction_db]='transaction_id,account_id,transaction_date,amount,transaction_type,branch_id'
)

OUT="$REPO_ROOT/parity/fixtures/source"
mkdir -p "$OUT"
for table in "${!HEADERS[@]}"; do
  {
    echo "${HEADERS[$table]}"
    docker exec "$CONTAINER" $SQLCMD -d sample -h-1 -W -s"|" \
      -Q "SET NOCOUNT ON; SELECT * FROM [$table]" \
      | sed '/^$/d' | tr '|' ','
  } > "$OUT/$table.csv"
  echo "wrote $OUT/$table.csv"
done

docker rm -f "$CONTAINER" >/dev/null
