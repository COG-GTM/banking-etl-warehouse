import csv
import uuid
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import types as T

from banking_etl.dims.branch import resolve_layers

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
PARITY_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "parity"

BRONZE_BRANCH_SCHEMA = T.StructType(
    [
        T.StructField("branch_id", T.IntegerType(), False),
        T.StructField("branch_name", T.StringType()),
        T.StructField("branch_location", T.StringType()),
    ]
)
BRONZE_ACCOUNT_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.IntegerType(), False),
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("account_type", T.StringType()),
        T.StructField("balance", T.IntegerType()),
        T.StructField("date_opened", T.TimestampType()),
        T.StructField("status", T.StringType()),
    ]
)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def source_branch_rows():
    return [
        (int(r["branch_id"]), r["branch_name"], r["branch_location"])
        for r in read_csv(DATA_DIR / "sqlserver_branch.csv")
    ]


def source_account_rows():
    return [
        (
            int(r["account_id"]),
            int(r["customer_id"]),
            r["account_type"],
            int(r["balance"]),
            datetime.strptime(r["date_opened"], "%Y-%m-%d %H:%M:%S"),
            r["status"],
        )
        for r in read_csv(DATA_DIR / "sqlserver_account.csv")
    ]


def parity_branch_rows():
    return [
        (int(r["BranchID"]), r["BranchName"], r["BranchLocation"])
        for r in read_csv(PARITY_DIR / "dim_branch.csv")
    ]


def parity_account_rows():
    return [
        (
            int(r["AccountID"]),
            int(r["CustomerID"]),
            r["AccountType"],
            Decimal(r["Balance"]),
            date.fromisoformat(r["DateOpened"]),
            r["Status"],
        )
        for r in read_csv(PARITY_DIR / "dim_account.csv")
    ]


def fresh_layers(spark):
    """Isolated bronze/silver/gold schemas per test (two-part names, local catalog)."""
    prefix = f"t5_{uuid.uuid4().hex[:8]}_"
    layers = resolve_layers(catalog=None, schema_prefix=prefix)
    for schema in (layers.bronze, layers.silver, layers.gold):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return layers


def seed_bronze(spark, layers, branch_rows=None, account_rows=None):
    branch_rows = source_branch_rows() if branch_rows is None else branch_rows
    account_rows = source_account_rows() if account_rows is None else account_rows
    spark.createDataFrame(branch_rows, BRONZE_BRANCH_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(layers.table("bronze", "sqlserver_branch"))
    spark.createDataFrame(account_rows, BRONZE_ACCOUNT_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(layers.table("bronze", "sqlserver_account"))


def table_rows(spark, table, key):
    return [tuple(r) for r in spark.table(table).orderBy(key).collect()]
