from __future__ import annotations

import csv
import os
import shutil
import sys
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import openpyxl
import pytest

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "src"))

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PARITY_CSV = ROOT / "fixtures" / "parity" / "fact_transaction.csv"

if not os.environ.get("JAVA_HOME") and Path("/usr/lib/jvm/java-17-openjdk-amd64").exists():
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="t7-warehouse-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("banking-etl-fact-transaction-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
    )
    if os.environ.get("SPARK_JARS_REPOSITORIES"):
        builder = builder.config("spark.jars.repositories", os.environ["SPARK_JARS_REPOSITORIES"])
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def schema(spark, request):
    name = "t7_" + "".join(ch if ch.isalnum() else "_" for ch in request.node.name)[:60]
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    spark.sql(f"CREATE SCHEMA {name}")
    yield name
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def sqlserver_rows():
    with open(FIXTURES / "sqlserver_transaction_db.csv", newline="") as fh:
        return [
            (
                int(r["transaction_id"]),
                int(r["account_id"]),
                datetime.fromisoformat(r["transaction_date"]),
                int(r["amount"]),
                r["transaction_type"],
                int(r["branch_id"]),
                datetime(2026, 1, 1),
            )
            for r in csv.DictReader(fh)
        ]


def excel_rows():
    ws = openpyxl.load_workbook(REPO / "data_sources" / "transaction_excel.xlsx", data_only=True)["Sheet1"]
    return [tuple(r[:6]) for r in ws.iter_rows(min_row=2, values_only=True) if any(v is not None for v in r)]


def csv_rows():
    with open(REPO / "data_sources" / "transaction_csv.csv", newline="", encoding="iso-8859-1") as fh:
        return [tuple(r.values()) for r in csv.DictReader(fh)]


SQLSERVER_DDL = (
    "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount INT, "
    "transaction_type STRING, branch_id INT, _ingested_at TIMESTAMP"
)
EXCEL_DDL = "transaction_id BIGINT, account_id BIGINT, transaction_date TIMESTAMP, amount BIGINT, transaction_type STRING, branch_id BIGINT"
CSV_DDL = "transaction_id STRING, account_id STRING, transaction_date STRING, amount STRING, transaction_type STRING, branch_id STRING"


@pytest.fixture
def bronze_frames(spark):
    return (
        spark.createDataFrame(sqlserver_rows(), SQLSERVER_DDL),
        spark.createDataFrame(excel_rows(), EXCEL_DDL),
        spark.createDataFrame(csv_rows(), CSV_DDL),
    )


def dim_frames(spark):
    with open(FIXTURES / "dim_account.csv", newline="") as fh:
        acc = [
            (
                int(r["account_id"]),
                int(r["customer_id"]),
                r["account_type"],
                Decimal(r["balance"]),
                datetime.fromisoformat(r["date_opened"]).date(),
                r["status"],
            )
            for r in csv.DictReader(fh)
        ]
    with open(FIXTURES / "dim_branch.csv", newline="") as fh:
        br = [(int(r["branch_id"]), r["branch_name"], r["branch_location"]) for r in csv.DictReader(fh)]
    return (
        spark.createDataFrame(
            acc,
            "account_id INT, customer_id INT, account_type STRING, balance DECIMAL(19,4), "
            "date_opened DATE, status STRING",
        ),
        spark.createDataFrame(br, "branch_id INT, branch_name STRING, branch_location STRING"),
    )


@pytest.fixture
def seeded(spark, schema, bronze_frames):
    """Bronze + gold dims in one local schema, laid out like the live ``banking_mig_t7`` seed."""
    from banking_etl.facts.transaction import resolve_tables

    tables = resolve_tables(
        catalog=None, bronze_schema=schema, silver_schema=schema, gold_schema=schema, ops_schema=schema
    )
    sql_df, excel_df, csv_df = bronze_frames
    sql_df.write.format("delta").saveAsTable(tables.bronze_sqlserver)
    excel_df.write.format("delta").saveAsTable(tables.bronze_excel)
    csv_df.write.format("delta").saveAsTable(tables.bronze_csv)
    acc, br = dim_frames(spark)
    acc.write.format("delta").saveAsTable(tables.dim_account)
    br.write.format("delta").saveAsTable(tables.dim_branch)
    return tables
