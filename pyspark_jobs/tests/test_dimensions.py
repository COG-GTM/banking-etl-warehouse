import datetime as dt
from decimal import Decimal

from pyspark_jobs import load_dim_account, load_dim_branch, load_dim_customer


def test_dim_branch_renames_to_dwh_columns(spark):
    branch = spark.createDataFrame(
        [(1, "KC Jakarta", "Jl. Gatot Subroto No 13")], "branch_id int, branch_name string, branch_location string"
    )
    out = load_dim_branch.transform(branch)
    assert out.columns == ["BranchID", "BranchName", "BranchLocation"]
    assert out.collect()[0].asDict() == {
        "BranchID": 1,
        "BranchName": "KC Jakarta",
        "BranchLocation": "Jl. Gatot Subroto No 13",
    }


def test_dim_account_casts_balance_and_date(spark):
    account = spark.createDataFrame(
        [(1, 1, "saving", 1500000, dt.datetime(2020, 5, 1, 9, 0), "active")],
        "account_id int, customer_id int, account_type string, balance int, date_opened timestamp, status string",
    )
    out = load_dim_account.transform(account)
    assert out.columns == ["AccountID", "CustomerID", "AccountType", "Balance", "DateOpened", "Status"]
    row = out.collect()[0]
    assert row.Balance == Decimal("1500000.0000")
    assert row.DateOpened == dt.date(2020, 5, 1)


def test_dim_customer_left_joins_and_uppercases(spark):
    customer = spark.createDataFrame(
        [
            (1, "Shelly Juwita", "Jl. Boulevard No. 31", 2, "25", "female", "shelly@gmail.com"),
            (2, "No City", "Jl. X", 99, " 40", "male", "x@gmail.com"),
        ],
        "customer_id int, customer_name string, address string, city_id int, age string, gender string, email string",
    )
    city = spark.createDataFrame([(2, "Kelapa Gading", 1)], "city_id int, city_name string, state_id int")
    state = spark.createDataFrame([(1, "Jakarta Utara")], "state_id int, state_name string")

    out = load_dim_customer.transform(customer, city, state)
    assert out.columns == ["CustomerID", "CustomerName", "Address", "CityName", "StateName", "Age", "Gender", "Email"]
    rows = {r.CustomerID: r.asDict() for r in out.collect()}
    assert rows[1] == {
        "CustomerID": 1,
        "CustomerName": "SHELLY JUWITA",
        "Address": "JL. BOULEVARD NO. 31",
        "CityName": "Kelapa Gading",
        "StateName": "Jakarta Utara",
        "Age": 25,
        "Gender": "FEMALE",
        "Email": "shelly@gmail.com",
    }
    # Talend tMap lookups are LEFT OUTER: an unknown city keeps the customer with NULL city/state.
    assert rows[2]["CityName"] is None and rows[2]["StateName"] is None and rows[2]["Age"] == 40
