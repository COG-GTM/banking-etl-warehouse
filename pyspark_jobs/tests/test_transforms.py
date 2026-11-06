import datetime as dt
from decimal import Decimal

import pytest

from pyspark_jobs import load_dim_account, load_dim_branch, load_dim_customer, load_fact_transaction
from pyspark_jobs.common.config import REPO_ROOT, FileSources
from pyspark_jobs.common.io import read_csv, read_excel
from pyspark_jobs.reports.balance_per_customer import balance_per_customer
from pyspark_jobs.reports.daily_transaction import daily_transaction


def test_dim_branch_columns(spark):
    df = spark.createDataFrame(
        [(1, "KC Jakarta", "Jl. Gatot Subroto")], ["branch_id", "branch_name", "branch_location"]
    )
    out = load_dim_branch.transform(df)
    assert out.columns == ["BranchID", "BranchName", "BranchLocation"]
    assert out.collect()[0].asDict() == {
        "BranchID": 1,
        "BranchName": "KC Jakarta",
        "BranchLocation": "Jl. Gatot Subroto",
    }


def test_dim_account_types(spark):
    df = spark.createDataFrame(
        [(1, 1, "saving", 1500000, dt.datetime(2020, 5, 1, 9), "active")],
        ["account_id", "customer_id", "account_type", "balance", "date_opened", "status"],
    )
    row = load_dim_account.transform(df).collect()[0]
    assert list(row.asDict()) == ["AccountID", "CustomerID", "AccountType", "Balance", "DateOpened", "Status"]
    assert row.Balance == Decimal("1500000.0000")
    assert row.DateOpened == dt.date(2020, 5, 1)


def test_dim_customer_join_and_uppercase(spark):
    customer = spark.createDataFrame(
        [
            (1, "Shelly Juwita", "Jl. Boulevard No. 31", 2, "25", "female", "shelly@gmail.com"),
            (2, "No City", "Jl. X", 99, "30", "male", "x@gmail.com"),
        ],
        ["customer_id", "customer_name", "address", "city_id", "age", "gender", "email"],
    )
    city = spark.createDataFrame([(2, "Kelapa Gading", 1)], ["city_id", "city_name", "state_id"])
    state = spark.createDataFrame([(1, "Jakarta Utara")], ["state_id", "state_name"])
    rows = {r.CustomerID: r for r in load_dim_customer.transform(customer, city, state).collect()}
    assert list(rows[1].asDict()) == list(load_dim_customer.TARGET_COLUMNS)
    assert rows[1].CustomerName == "SHELLY JUWITA"
    assert rows[1].Address == "JL. BOULEVARD NO. 31"
    assert rows[1].CityName == "KELAPA GADING"
    assert rows[1].StateName == "JAKARTA UTARA"
    assert rows[1].Gender == "FEMALE"
    assert rows[1].Email == "shelly@gmail.com"
    assert rows[1].Age == 25
    assert rows[2].CityName is None and rows[2].StateName is None


def test_dim_customer_uppercase_configurable(spark):
    customer = spark.createDataFrame(
        [(1, "Shelly", "Jl", 2, "25", "female", "e")],
        ["customer_id", "customer_name", "address", "city_id", "age", "gender", "email"],
    )
    city = spark.createDataFrame([(2, "Kelapa Gading", 1)], ["city_id", "city_name", "state_id"])
    state = spark.createDataFrame([(1, "Jakarta Utara")], ["state_id", "state_name"])
    row = load_dim_customer.transform(customer, city, state, ("CustomerName",)).collect()[0]
    assert row.CustomerName == "SHELLY" and row.CityName == "Kelapa Gading" and row.Gender == "female"
    with pytest.raises(ValueError):
        load_dim_customer.transform(customer, city, state, ("Nope",))


def _tx(spark, rows):
    from pyspark_jobs.load_fact_transaction import CSV_SCHEMA

    return spark.createDataFrame(rows, CSV_SCHEMA)


def test_fact_dedup_prefers_sql_then_excel_then_csv(spark):
    ts = dt.datetime(2024, 1, 18, 9)
    sql = _tx(spark, [(1, 1, ts, 100, "Deposit", 1)])
    excel = _tx(spark, [(1, 1, ts, 999, "Deposit", 1), (2, 2, ts, 200, "Payment", 1)])
    csv = _tx(
        spark, [(2, 2, ts, 888, "Payment", 1), (3, 3, ts, 300, "Withdrawal", 2), (3, 3, ts, 300, "Withdrawal", 2)]
    )
    out = {r.transaction_id: r for r in load_fact_transaction.unite_and_dedup(sql, excel, csv).collect()}
    assert sorted(out) == [1, 2, 3]
    assert out[1].amount == Decimal(100)
    assert out[2].amount == Decimal(200)
    assert out[3].amount == Decimal(300)


def test_fact_dedup_keeps_first_row_within_source(spark):
    ts = dt.datetime(2024, 1, 18, 9)
    csv = _tx(spark, [(14, 1, ts, 100, "Deposit", 1), (14, 1, ts, 200, "Deposit", 1), (15, 1, ts, 5, "Deposit", 1)])
    empty = _tx(spark, [])
    for _ in range(3):
        out = {r.transaction_id: r.amount for r in load_fact_transaction.unite_and_dedup(empty, empty, csv).collect()}
        assert out == {14: Decimal(100), 15: Decimal(5)}


def test_fact_fk_filter(spark):
    ts = dt.datetime(2024, 1, 18, 9)
    tx = load_fact_transaction.unite_and_dedup(
        _tx(spark, [(1, 1, ts, 100, "Deposit", 1), (2, 99, ts, 100, "Deposit", 1), (3, 1, ts, 100, "Deposit", 9)]),
        _tx(spark, [(4, None, ts, 100, "Deposit", None)]),
        spark.createDataFrame([], load_fact_transaction.CSV_SCHEMA),
    )
    accounts = spark.createDataFrame([(1,)], ["AccountID"])
    branches = spark.createDataFrame([(1,)], ["BranchID"])
    valid, rejected = load_fact_transaction.enforce_foreign_keys(tx, accounts, branches)
    assert sorted(r.transaction_id for r in valid.collect()) == [1, 4]
    assert sorted(r.transaction_id for r in rejected.collect()) == [2, 3]
    fact = load_fact_transaction.to_fact(valid)
    assert fact.columns == ["TransactionID", "AccountID", "TransactionDate", "Amount", "TransactionType", "BranchID"]


def test_file_sources_parse(spark):
    data = REPO_ROOT / "data_sources"
    csv = read_csv(spark, str(data / "transaction_csv.csv"), load_fact_transaction.CSV_SCHEMA, "dd-MM-yyyy HH:mm:ss")
    first = csv.orderBy("transaction_id").first()
    assert first.transaction_id == 14
    assert first.transaction_date == dt.datetime(2024, 1, 21, 14, 0)
    files = FileSources(
        str(data / "transaction_csv.csv"),
        str(data / "transaction_excel.xlsx"),
        "Sheet1",
        "dd-MM-yyyy HH:mm:ss",
        "pandas",
    )
    excel = load_fact_transaction.normalize(read_excel(spark, files), "excel")
    row = excel.orderBy("transaction_id").first()
    assert row.transaction_id == 6
    assert row.transaction_date == dt.datetime(2024, 1, 18, 13, 10)
    assert row.amount == Decimal(50000)


def _fact(spark):
    return spark.createDataFrame(
        [
            (1, 1, dt.datetime(2024, 1, 18, 9), Decimal(100), "Deposit", 1),
            (2, 1, dt.datetime(2024, 1, 18, 17), Decimal(30), "Withdrawal", 1),
            (3, 2, dt.datetime(2024, 1, 20, 9), Decimal(50), "Payment", 1),
            (4, 2, dt.datetime(2024, 1, 21, 9), Decimal(70), "Deposit", 1),
        ],
        ["TransactionID", "AccountID", "TransactionDate", "Amount", "TransactionType", "BranchID"],
    )


def test_daily_transaction(spark):
    rows = [r.asDict() for r in daily_transaction(_fact(spark), "2024-01-18", "2024-01-20").collect()]
    assert rows == [
        {"Date": dt.date(2024, 1, 18), "TotalTransactions": 2, "TotalAmount": Decimal(130)},
        {"Date": dt.date(2024, 1, 20), "TotalTransactions": 1, "TotalAmount": Decimal(50)},
    ]


def test_balance_per_customer(spark):
    customer = spark.createDataFrame([(1, "SHELLY JUWITA"), (2, "BOBI RINALDO")], ["CustomerID", "CustomerName"])
    account = spark.createDataFrame(
        [
            (1, 1, "saving", Decimal(1000), "active"),
            (2, 1, "checking", Decimal(500), "active"),
            (3, 1, "saving", Decimal(9), "terminated"),
            (4, 2, "saving", Decimal(1), "active"),
        ],
        ["AccountID", "CustomerID", "AccountType", "Balance", "Status"],
    )
    fact = _fact(spark).union(
        spark.createDataFrame([(5, 5, dt.datetime(2024, 1, 1), Decimal(1), "Deposit", 1)], _fact(spark).schema)
    )
    rows = sorted(tuple(r) for r in balance_per_customer(fact, customer, account, "shelly").collect())
    assert [list(r) for r in rows] == [
        ["SHELLY JUWITA", "checking", Decimal(500), Decimal(500) + 50 * -1 + 70],
        ["SHELLY JUWITA", "saving", Decimal(1000), Decimal(1070)],
    ]


def test_balance_per_customer_deposit_is_case_insensitive(spark):
    customer = spark.createDataFrame([(1, "SHELLY JUWITA")], ["CustomerID", "CustomerName"])
    account = spark.createDataFrame(
        [(1, 1, "saving", Decimal(100), "Active")], ["AccountID", "CustomerID", "AccountType", "Balance", "Status"]
    )
    fact = spark.createDataFrame(
        [
            (1, 1, dt.datetime(2024, 1, 1), Decimal(20), "deposit", 1),
            (2, 1, dt.datetime(2024, 1, 2), Decimal(5), "DEPOSIT ", 1),
            (3, 1, dt.datetime(2024, 1, 3), Decimal(3), " Deposit", 1),
        ],
        _fact(spark).schema,
    )
    rows = [tuple(r) for r in balance_per_customer(fact, customer, account, "SHELLY").collect()]
    assert rows == [("SHELLY JUWITA", "saving", Decimal(100), Decimal(122))]
