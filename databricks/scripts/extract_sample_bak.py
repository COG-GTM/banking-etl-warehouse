"""Extract the sample.bak tables to CSV (the extract-to-files fallback).

Reads every table in ``banking_etl.ingestion.sources.SAMPLE_DB_TABLES`` over
JDBC (``read_table_jdbc``, the same reader the live bronze path uses) from a
restored ``sample`` database (see ``restore_sample_bak.sh``) and writes one
header CSV per table, all values as text, ordered by primary key:

    SAMPLE_DB_JDBC_URL='jdbc:sqlserver://localhost:14333;databaseName=sample;encrypt=true;trustServerCertificate=true' \
    SAMPLE_DB_USER=sa SAMPLE_DB_PASSWORD=... \
    python databricks/scripts/extract_sample_bak.py --out data_sources/sample_db

Upload the files to ``<landing>/sample_db/<table>/`` (``stage_landing_files.sh``)
and run the bronze job with ``sample_db_mode=files``.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from banking_etl.ingestion.sources import SAMPLE_DB_TABLES
from banking_etl.ingestion.sqlserver import SqlServerConnection, read_table_jdbc

MSSQL_JDBC = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"


def connection_from_url(url: str, user: str, password: str) -> SqlServerConnection:
    m = re.match(r"jdbc:sqlserver://([^:;]+)(?::(\d+))?(.*)$", url)
    if not m:
        raise SystemExit(f"unsupported JDBC URL: {url}")
    props = dict(p.split("=", 1) for p in m.group(3).split(";") if "=" in p)
    return SqlServerConnection(
        host=m.group(1),
        port=int(m.group(2) or 1433),
        database=props.get("databaseName", "sample"),
        user=user,
        password=password,
        encrypt=props.get("encrypt", "true").lower() == "true",
        trust_server_certificate=props.get("trustServerCertificate", "false").lower() == "true",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", default=str(ROOT.parent / "data_sources" / "sample_db"))
    args = parser.parse_args()

    conn = connection_from_url(
        os.environ["SAMPLE_DB_JDBC_URL"], os.environ["SAMPLE_DB_USER"], os.environ["SAMPLE_DB_PASSWORD"]
    )

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("extract_sample_bak")
        .config("spark.jars.packages", MSSQL_JDBC)
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    try:
        for table in SAMPLE_DB_TABLES:
            df = read_table_jdbc(spark, conn, table)
            rows = (
                df.orderBy(*table.primary_key)
                .selectExpr(*[f"CAST(`{c}` AS STRING) AS `{c}`" for c in table.column_names])
                .collect()
            )
            path = out / f"{table.name}.csv"
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh, lineterminator="\n")
                writer.writerow(table.column_names)
                writer.writerows([[r[c] for c in table.column_names] for r in rows])
            print(f"{table.qualified_source_name}: {len(rows)} rows -> {path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
