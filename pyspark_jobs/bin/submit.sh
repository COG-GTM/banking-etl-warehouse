#!/usr/bin/env bash
# spark-submit wrapper that adds the SQL Server JDBC driver (and spark-excel when
# EXCEL_READER=crealytics) to the classpath. Usage:
#   pyspark_jobs/bin/submit.sh pyspark_jobs/run_all.py --full-refresh
#   pyspark_jobs/bin/submit.sh pyspark_jobs/reports/daily_transaction.py --start-date 2024-01-18 --end-date 2024-01-20
set -euo pipefail

JDBC_PACKAGE="${SPARK_JDBC_PACKAGES:-com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11}"
EXCEL_PACKAGE="${SPARK_EXCEL_PACKAGE:-com.crealytics:spark-excel_2.12:3.5.1_0.20.4}"

args=()
packages=()
if [[ -n "${SPARK_JDBC_JARS:-}" ]]; then
  args+=(--jars "${SPARK_JDBC_JARS}")
else
  packages+=("${JDBC_PACKAGE}")
fi
if [[ "${EXCEL_READER:-pandas}" == "crealytics" ]]; then
  packages+=("${EXCEL_PACKAGE}")
fi
if (( ${#packages[@]} )); then
  args+=(--packages "$(IFS=,; echo "${packages[*]}")")
fi
if [[ -n "${SPARK_JARS_REPOSITORIES:-}" ]]; then
  args+=(--repositories "${SPARK_JARS_REPOSITORIES}")
fi
if [[ -n "${SPARK_MASTER:-}" ]]; then
  args+=(--master "${SPARK_MASTER}")
fi

exec spark-submit "${args[@]}" ${SPARK_SUBMIT_EXTRA_ARGS:-} "$@"
