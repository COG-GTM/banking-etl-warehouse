#!/usr/bin/env bash
# Restore data_sources/sample.bak into a local SQL Server 2022 container.
#   MSSQL_SA_PASSWORD=... ./restore_sample_bak.sh
# Exposes the database `sample` on localhost:${MSSQL_PORT:-14333}.
set -euo pipefail

: "${MSSQL_SA_PASSWORD:?set MSSQL_SA_PASSWORD (SQL Server password policy applies)}"
CONTAINER="${MSSQL_CONTAINER:-banking-sample-mssql}"
PORT="${MSSQL_PORT:-14333}"
IMAGE="${MSSQL_IMAGE:-mcr.microsoft.com/mssql/server:2022-latest}"
DATA_DIR="$(cd "$(dirname "$0")/../../data_sources" && pwd)"

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$MSSQL_SA_PASSWORD" \
  -p "$PORT:1433" -v "$DATA_DIR:/backup:ro" "$IMAGE" >/dev/null

sqlcmd() { docker exec "$CONTAINER" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" "$@"; }

for _ in $(seq 1 60); do
  if sqlcmd -Q "SELECT 1" >/dev/null 2>&1; then break; fi
  sleep 2
done

sqlcmd -Q "RESTORE DATABASE sample FROM DISK='/backup/sample.bak' WITH \
  MOVE 'sample' TO '/var/opt/mssql/data/sample.mdf', \
  MOVE 'sample_log' TO '/var/opt/mssql/data/sample_log.ldf', REPLACE"

sqlcmd -d sample -W -Q "SET NOCOUNT ON; SELECT s.name + '.' + t.name AS table_name, SUM(p.rows) AS row_count \
  FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id \
  JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1) \
  WHERE t.name <> 'sysdiagrams' GROUP BY s.name, t.name ORDER BY 1"

echo "JDBC URL: jdbc:sqlserver://localhost:$PORT;databaseName=sample;encrypt=true;trustServerCertificate=true"
