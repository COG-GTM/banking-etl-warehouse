import csv
import itertools
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, TimestampType

from banking_etl.bronze.fixtures import stage_fixtures
from banking_etl.bronze.sqlserver import ingest_table
from banking_etl.config import Settings
from banking_etl.dims.account import (
    DIM_ACCOUNT_MAPPING,
    bronze_account_table,
    dim_account_table,
    ensure_dim_account,
    load_dim_account,
    load_silver_account,
    silver_account_table,
    to_dim_account,
    transform_silver_account,
)

from conftest import FIXTURES

ROOT = Path(__file__).resolve().parents[2]
PARITY_CSV = FIXTURES / "parity" / "dwh_dim_account.csv"
DIM_COLUMNS = ["AccountKey", "AccountID", "CustomerID", "AccountType", "Balance", "DateOpened", "Status"]

_ids = itertools.count()


def make_env(spark, tmp_path) -> Settings:
    """Isolated prefixed schemas with bronze.sqlserver_account loaded from the sample_db fixture."""
    settings = Settings(catalog=None, schema_prefix=f"t6_{next(_ids)}_")
    for layer in ("bronze", "silver"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.schema(layer)}")
    root = tmp_path / "sample_db"
    stage_fixtures(root, ["account"])
    ingest_table(spark, settings, "account", fixture_root=str(root))
    return settings


def load_all(spark, settings):
    return load_silver_account(spark, settings), load_dim_account(spark, settings)


def dim_by_id(spark, settings):
    return {r.AccountID: r.asDict() for r in spark.table(dim_account_table(settings)).collect()}


def parity_rows():
    with PARITY_CSV.open(newline="") as fh:
        return {
            int(r["AccountID"]): {
                "AccountID": int(r["AccountID"]),
                "CustomerID": int(r["CustomerID"]),
                "AccountType": r["AccountType"],
                "Balance": Decimal(r["Balance"]),
                "DateOpened": date.fromisoformat(r["DateOpened"]),
                "Status": r["Status"],
            }
            for r in csv.DictReader(fh)
        }


@pytest.fixture
def env(spark, tmp_path):
    return make_env(spark, tmp_path)


# --------------------------------------------------------------------------- silver


def test_silver_transform_types_and_values(spark, env):
    df = transform_silver_account(spark.table(bronze_account_table(env)))
    types = {f.name: f.dataType for f in df.schema.fields}
    assert types == {
        "account_id": IntegerType(),
        "customer_id": IntegerType(),
        "account_type": StringType(),
        "balance": DecimalType(19, 4),
        "date_opened": DateType(),
        "status": StringType(),
        "_ingested_at": TimestampType(),
        "_source": StringType(),
    }
    rows = {r.account_id: r for r in df.collect()}
    assert len(rows) == 21
    r4 = rows[4]
    assert (r4.customer_id, r4.account_type, r4.balance, r4.date_opened, r4.status) == (
        3, "checking", Decimal("4500000.0000"), date(2021, 6, 24), "terminated"
    )
    # Talend passes strings through untouched: lowercase status values are what sp_BalancePerCustomer filters on.
    assert {r.status for r in rows.values()} == {"active", "terminated"}
    assert sum(r.balance for r in rows.values()) == Decimal("624000000")


def test_silver_date_opened_drops_time_of_day(spark):
    bronze = spark.createDataFrame(
        [(1, 1, "saving", 10, datetime(2020, 5, 1, 23, 59, 59), "active"), (2, None, None, None, None, None)],
        "account_id INT, customer_id INT, account_type STRING, balance INT, date_opened TIMESTAMP, status STRING",
    )
    rows = {r.account_id: r for r in transform_silver_account(bronze).collect()}
    assert rows[1].date_opened == date(2020, 5, 1) and rows[1].balance == Decimal("10.0000")
    assert rows[2].asDict() == {
        "account_id": 2, "customer_id": None, "account_type": None, "balance": None, "date_opened": None, "status": None
    }


def test_silver_preserves_whitespace_and_case(spark):
    bronze = spark.createDataFrame(
        [(1, 1, " Saving ", 1, None, "Active ")],
        "account_id INT, customer_id INT, account_type STRING, balance INT, date_opened TIMESTAMP, status STRING",
    )
    (row,) = transform_silver_account(bronze).collect()
    assert (row.account_type, row.status) == (" Saving ", "Active ")


@pytest.mark.parametrize(
    "rows, match",
    [
        ([(None, 1), (2, 1)], "NULL account_id"),
        ([(1, 1), (1, 2)], "duplicate account_id"),
    ],
)
def test_silver_rejects_bad_keys(spark, rows, match):
    bronze = spark.createDataFrame(rows, "account_id INT, customer_id INT").select(
        "account_id", "customer_id", F.lit("saving").alias("account_type"), F.lit(1).alias("balance"),
        F.lit(None).cast("timestamp").alias("date_opened"), F.lit("active").alias("status"),
    )
    with pytest.raises(ValueError, match=match):
        transform_silver_account(bronze)


def test_silver_missing_columns(spark):
    with pytest.raises(ValueError, match="missing columns"):
        transform_silver_account(spark.createDataFrame([(1,)], "account_id INT"))


def test_load_silver_account_overwrites(spark, env):
    first = load_silver_account(spark, env)
    second = load_silver_account(spark, env)
    assert first["rows"] == second["rows"] == 21
    assert first["target"] == silver_account_table(env) == f"{env.schema_prefix}silver.account"


def test_to_dim_account_matches_tmap_output():
    assert list(DIM_ACCOUNT_MAPPING.values()) == DIM_COLUMNS[1:]


# --------------------------------------------------------------------------- gold


def test_gold_initial_load_parity_with_talend_dwh(spark, env):
    assert not spark.catalog.tableExists(dim_account_table(env))
    _, gold = load_all(spark, env)  # creates the star schema (CREATE_IF_NOT_EXISTS)
    assert (gold["rows"], gold["numTargetRowsInserted"], gold["numTargetRowsUpdated"]) == (21, 21, 0)

    dim = spark.table(dim_account_table(env))
    assert dim.columns == DIM_COLUMNS
    types = {f.name: f.dataType.simpleString() for f in dim.schema.fields}
    assert types["Balance"] == "decimal(19,4)" and types["DateOpened"] == "date" and types["AccountKey"] == "bigint"

    rows = dim_by_id(spark, env)
    keys = [r.pop("AccountKey") for r in rows.values()]
    assert None not in keys and len(set(keys)) == 21
    # DWH.DimAccount as loaded by the Talend job on SQL Server 2022 (sample.bak restored).
    assert rows == parity_rows()


def test_gold_rerun_is_idempotent(spark, env):
    load_all(spark, env)
    before = dim_by_id(spark, env)
    _, gold = load_all(spark, env)
    assert (gold["numTargetRowsInserted"], gold["numTargetRowsUpdated"], gold["numTargetRowsDeleted"]) == (0, 0, 0)
    assert dim_by_id(spark, env) == before


def test_gold_scd1_update_keeps_account_key(spark, env):
    load_all(spark, env)
    before = dim_by_id(spark, env)

    bronze = bronze_account_table(env)
    changed = (
        spark.table(bronze)
        .where("account_id <> 21")  # dropped from the source: must be kept in gold (no hard deletes)
        .withColumn("balance", F.when(F.col("account_id") == 4, F.lit(99)).otherwise(F.col("balance")))
        .withColumn("status", F.when(F.col("account_id") == 5, F.lit("terminated")).otherwise(F.col("status")))
    )
    new = changed.where("account_id = 1").withColumn("account_id", F.lit(22)).withColumn("customer_id", F.lit(16))
    changed.unionByName(new).localCheckpoint().write.format("delta").mode("overwrite").saveAsTable(bronze)

    _, gold = load_all(spark, env)
    assert (gold["numTargetRowsInserted"], gold["numTargetRowsUpdated"], gold["numTargetRowsDeleted"]) == (1, 2, 0)
    after = dim_by_id(spark, env)
    assert set(after) == set(range(1, 23))
    assert after[4]["Balance"] == Decimal("99.0000") and after[5]["Status"] == "terminated"
    assert after[21] == before[21]
    for account_id, row in before.items():
        assert after[account_id]["AccountKey"] == row["AccountKey"]
    assert after[22]["AccountKey"] not in {r["AccountKey"] for r in before.values()}
    assert after[22]["CustomerID"] == 16


def test_ensure_dim_account_only_creates_when_missing(spark, env):
    assert ensure_dim_account(spark, env) is True
    assert spark.catalog.tableExists(dim_account_table(env))
    assert ensure_dim_account(spark, env) is False


def test_load_dim_account_without_ensure_requires_table(spark, env):
    load_silver_account(spark, env)
    with pytest.raises(Exception):
        load_dim_account(spark, env, ensure_table=False)


def test_to_dim_account_has_no_surrogate_key(spark, env):
    load_silver_account(spark, env)
    assert to_dim_account(spark.table(silver_account_table(env))).columns == DIM_COLUMNS[1:]


# --------------------------------------------------------------------------- bundle resource


def test_job_resource_runs_silver_then_gold_on_serverless():
    doc = yaml.safe_load((ROOT / "resources" / "ticket-06-dim-account.yml").read_text())
    job = doc["resources"]["jobs"]["load_dim_account"]
    assert {p["name"] for p in job["parameters"]} == {"catalog", "schema_prefix"}
    silver, gold = job["tasks"]
    assert (ROOT / "resources" / silver["notebook_task"]["notebook_path"]).resolve() == (
        ROOT / "notebooks" / "silver" / "silver_account.py"
    )
    assert (ROOT / "resources" / gold["notebook_task"]["notebook_path"]).resolve() == (
        ROOT / "notebooks" / "gold" / "load_dim_account.py"
    )
    assert gold["depends_on"] == [{"task_key": silver["task_key"]}]
    for task in job["tasks"]:
        assert not ({"new_cluster", "job_cluster_key", "existing_cluster_id"} & set(task))
