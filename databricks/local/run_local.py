"""Run the whole pipeline on a local Spark + Delta session (no Databricks needed).

Usage (SQL Server restored from data_sources/sample.bak, credentials in env vars):

    export JDBC_HOST=localhost SQLSERVER_USER=sa SQLSERVER_PASSWORD=...
    python databricks/local/run_local.py

Requires pyspark 3.5.x, delta-spark 3.2.x and Java 17. Tables are stored under
databricks/local/.warehouse (git-ignored).
"""

import os
import runpy
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
DATABRICKS_DIR = os.path.dirname(HERE)
REPO_DIR = os.path.dirname(DATABRICKS_DIR)
WAREHOUSE = os.path.join(HERE, ".warehouse")

STEPS = [
    "jobs/create_tables.py",
    "jobs/load_dim_branch.py",
    "jobs/load_dim_account.py",
    "jobs/load_dim_customer.py",
    "jobs/load_fact_transaction.py",
    "analytics/daily_transaction.py",
    "analytics/balance_per_customer.py",
]


def build_spark() -> SparkSession:
    os.makedirs(WAREHOUSE, exist_ok=True)
    os.chdir(WAREHOUSE)  # keeps derby.log / metastore_db out of the repo root
    builder = (
        SparkSession.builder.appName("banking-dwh-local")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(WAREHOUSE, "tables"))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={WAREHOUSE}/metastore_db;create=true",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
    )
    repos = os.environ.get("SPARK_JARS_REPOSITORIES")
    if repos:
        builder = builder.config("spark.jars.repositories", repos)
    extra = [
        "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11",
        "com.crealytics:spark-excel_2.12:3.5.1_0.20.4",
    ]
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()


def main() -> None:
    os.environ.setdefault("CSV_PATH", os.path.join(REPO_DIR, "data_sources", "transaction_csv.csv"))
    os.environ.setdefault("EXCEL_PATH", os.path.join(REPO_DIR, "data_sources", "transaction_excel.xlsx"))
    os.environ.setdefault("JDBC_EXTRA_OPTIONS", "encrypt=true;trustServerCertificate=true")
    build_spark().sparkContext.setLogLevel("WARN")
    for step in sys.argv[1:] or STEPS:
        print(f"\n=== {step}")
        runpy.run_path(os.path.join(DATABRICKS_DIR, step), run_name="__main__")


if __name__ == "__main__":
    main()
