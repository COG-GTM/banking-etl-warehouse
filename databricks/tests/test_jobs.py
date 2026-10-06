import datetime as dt
from decimal import Decimal

from jobs import load_dim_customer, load_fact_transaction

TX_COLS = "transaction_id int, account_id int, transaction_date {}, amount int, transaction_type string, branch_id int"


def test_dim_customer_joins_and_upcases(spark):
    customer = spark.createDataFrame(
        [
            (1, "Shelly Juwita", "Jl. Boulevard", 2, "25", "female", "s@x.com"),
            (2, "No City", "a", 99, "30", "male", "n@x.com"),
        ],
        "customer_id int, customer_name string, address string, city_id int, age string, gender string, email string",
    )
    city = spark.createDataFrame([(2, "Jakarta", 1)], "city_id int, city_name string, state_id int")
    state = spark.createDataFrame([(1, "DKI Jakarta")], "state_id int, state_name string")
    rows = {r.CustomerID: r for r in load_dim_customer.transform(customer, city, state).collect()}
    assert (rows[1].CustomerName, rows[1].Address, rows[1].Gender) == ("SHELLY JUWITA", "JL. BOULEVARD", "FEMALE")
    assert (rows[1].CityName, rows[1].StateName, rows[1].Age) == ("Jakarta", "DKI Jakarta", 25)
    assert rows[2].CityName is None


def test_fact_union_dedups_preferring_sql_source(spark):
    sql_src = spark.createDataFrame(
        [(6, 6, dt.datetime(2022, 2, 21, 13, 10), 50000, "Withdrawal", 1)], TX_COLS.format("timestamp")
    )
    excel_src = spark.createDataFrame(
        [
            (6, 6, dt.datetime(2024, 1, 1, 0, 0), 50000, "Withdrawal", 1),
            (11, 10, dt.datetime(2024, 1, 2), 5, "Transfer", 1),
        ],
        TX_COLS.format("timestamp"),
    )
    csv_src = spark.createDataFrame(
        [("14", "13", "21-01-2024 14:00:00", "1500000", "Deposit", "4")],
        "transaction_id string, account_id string, transaction_date string, "
        "amount string, transaction_type string, branch_id string",
    )
    rows = {r.TransactionID: r for r in load_fact_transaction.transform(sql_src, excel_src, csv_src).collect()}
    assert sorted(rows) == [6, 11, 14]
    assert rows[6].TransactionDate == dt.datetime(2022, 2, 21, 13, 10)
    assert rows[14].TransactionDate == dt.datetime(2024, 1, 21, 14, 0)
    assert rows[14].Amount == Decimal("1500000")


def test_fact_rejects_rows_without_dimension_keys(spark):
    fact = spark.createDataFrame([(1, 1, 1), (2, 99, 1), (3, 1, 99)], "TransactionID int, AccountID int, BranchID int")
    accounts = spark.createDataFrame([(1,)], "AccountID int")
    branches = spark.createDataFrame([(1,)], "BranchID int")
    valid, rejected = load_fact_transaction.split_foreign_key_rejects(fact, accounts, branches)
    assert [r.TransactionID for r in valid.collect()] == [1]
    assert sorted(r.TransactionID for r in rejected.collect()) == [2, 3]
