import datetime
import os
from decimal import Decimal

import pytest
from conftest import REPO_ROOT, FakeDbutils

from common.config import SOURCE_TABLES, TARGET_SCHEMA, load_config
from common.readers import read_staged_file
from load_dim_account import transform_dim_account
from load_dim_branch import transform_dim_branch
from load_dim_customer import transform_dim_customer

SECRETS = {
    ("banking-etl", "sqlserver-host"): "sql.example.net",
    ("banking-etl", "sqlserver-user"): "etl_user",
    ("banking-etl", "sqlserver-password"): "s3cret",
}


def test_config_reads_widgets_and_secrets(spark):
    cfg = load_config(spark, FakeDbutils(widgets={"catalog": "banking"}, secrets=SECRETS))
    assert TARGET_SCHEMA == "dwh"
    assert set(SOURCE_TABLES) == {"customer", "city", "state", "account", "branch", "transaction"}
    assert cfg.jdbc_url.startswith("jdbc:sqlserver://sql.example.net:1433;databaseName=sample")
    assert cfg.jdbc_properties["user"] == "etl_user"
    assert "s3cret" not in repr(cfg)
    assert cfg.target_table("dim_branch") == "banking.dwh.dim_branch"
    assert cfg.source_table("transaction") == "dbo.[transaction_db]"
    with pytest.raises(ValueError):
        cfg.source_table("nope")


def test_jdbc_host_widget_overrides_secret(spark):
    cfg = load_config(spark, FakeDbutils(widgets={"jdbc_host": "localhost"}, secrets=SECRETS))
    assert cfg.jdbc_host == "localhost"


def test_dim_branch(spark):
    src = spark.createDataFrame(
        [(1, "KC Jakarta", "Jl. Gatot Subroto No 13")], ["branch_id", "branch_name", "branch_location"]
    )
    out = transform_dim_branch(src)
    assert out.columns == ["BranchID", "BranchName", "BranchLocation"]
    assert out.collect()[0].asDict() == {
        "BranchID": 1,
        "BranchName": "KC Jakarta",
        "BranchLocation": "Jl. Gatot Subroto No 13",
    }


def test_dim_account(spark):
    src = spark.createDataFrame(
        [(1, 1, "saving", 1500000, datetime.datetime(2020, 5, 1, 9), "active")],
        ["account_id", "customer_id", "account_type", "balance", "date_opened", "status"],
    )
    row = transform_dim_account(src).collect()[0].asDict()
    assert row == {
        "AccountID": 1,
        "CustomerID": 1,
        "AccountType": "saving",
        "Balance": Decimal("1500000.0000"),
        "DateOpened": datetime.date(2020, 5, 1),
        "Status": "active",
    }


def test_dim_customer_tmap_join_and_uppercase(spark):
    customer = spark.createDataFrame(
        [
            (1, "Shelly Juwita", "Jl. Boulevard No. 31", 2, "25", "female", "shelly@gmail.com"),
            (2, "No City", "Jl. X", 99, "n/a", "male", "x@gmail.com"),
        ],
        ["customer_id", "customer_name", "address", "city_id", "age", "gender", "email"],
    )
    city = spark.createDataFrame([(2, "KOTA JAKARTA PUSAT", 1)], ["city_id", "city_name", "state_id"])
    state = spark.createDataFrame([(1, "DKI JAKARTA")], ["state_id", "state_name"])
    out = transform_dim_customer(customer, city, state)
    assert out.columns == ["CustomerID", "CustomerName", "Address", "CityName", "StateName", "Age", "Gender", "Email"]
    rows = {r.CustomerID: r.asDict() for r in out.collect()}
    assert rows[1] == {
        "CustomerID": 1,
        "CustomerName": "SHELLY JUWITA",
        "Address": "JL. BOULEVARD NO. 31",
        "CityName": "KOTA JAKARTA PUSAT",
        "StateName": "DKI JAKARTA",
        "Age": 25,
        "Gender": "FEMALE",
        "Email": "shelly@gmail.com",
    }
    assert rows[2]["CityName"] is None and rows[2]["StateName"] is None and rows[2]["Age"] is None


def test_read_staged_csv(spark):
    df = read_staged_file(os.path.join(REPO_ROOT, "data_sources", "transaction_csv.csv"), spark)
    assert "transaction_id" in df.columns and df.count() > 0


def test_read_staged_excel_falls_back_to_pandas(spark):
    df = read_staged_file(os.path.join(REPO_ROOT, "data_sources", "transaction_excel.xlsx"), spark)
    assert "transaction_id" in df.columns and df.count() > 0
