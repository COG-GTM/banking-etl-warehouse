#!/usr/bin/env bash
# Upload the repo's sample source files into the bronze landing volume.
#   ./stage_landing_files.sh [landing_root]
# landing_root defaults to /Volumes/${CATALOG:-banking_dwh}/${BRONZE_SCHEMA:-bronze}/landing
# Layout expected by banking_etl.ingestion:
#   <landing>/transaction_csv/*.csv
#   <landing>/transaction_excel/*.xlsx
#   <landing>/sample_db/<table>/*.csv     (sample.bak extract fallback)
set -euo pipefail

LANDING="${1:-/Volumes/${CATALOG:-banking_dwh}/${BRONZE_SCHEMA:-bronze}/landing}"
DATA_DIR="$(cd "$(dirname "$0")/../../data_sources" && pwd)"

databricks fs mkdirs "dbfs:$LANDING/transaction_csv"
databricks fs mkdirs "dbfs:$LANDING/transaction_excel"
databricks fs cp --overwrite "$DATA_DIR/transaction_csv.csv" "dbfs:$LANDING/transaction_csv/transaction_csv.csv"
databricks fs cp --overwrite "$DATA_DIR/transaction_excel.xlsx" "dbfs:$LANDING/transaction_excel/transaction_excel.xlsx"
for f in "$DATA_DIR"/sample_db/*.csv; do
  table="$(basename "$f" .csv)"
  databricks fs mkdirs "dbfs:$LANDING/sample_db/$table"
  databricks fs cp --overwrite "$f" "dbfs:$LANDING/sample_db/$table/$table.csv"
done
echo "staged into $LANDING"
