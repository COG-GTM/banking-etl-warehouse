from datetime import date, datetime
from decimal import Decimal

from etl.analytics import balance_per_customer, daily_transaction
from etl.jobs import load_dim_account, load_dim_branch, load_dim_customer, load_fact_transaction

TX_COLS = [
    "transaction_id",
    "account_id",
    "transaction_date",
    "amount",
    "transaction_type",
    "branch_id",
]


def test_dim_branch_maps_columns(spark):
    src = spark.createDataFrame(
        [(1, "KC Jakarta", "Jl. Gatot Subroto No 13")],
        ["branch_id", "branch_name", "branch_location"],
    )
    row = load_dim_branch.transform(src).collect()[0]
    assert row.asDict() == {
        "BranchID": 1,
        "BranchName": "KC Jakarta",
        "BranchLocation": "Jl. Gatot Subroto No 13",
    }


def test_dim_account_casts_date_and_balance(spark):
    src = spark.createDataFrame(
        [(1, 1, "saving", 1500000, datetime(2020, 5, 1, 9, 0), "active")],
        ["account_id", "customer_id", "account_type", "balance", "date_opened", "status"],
    )
    row = load_dim_account.transform(src).collect()[0]
    assert row.DateOpened == date(2020, 5, 1)
    assert row.Balance == Decimal("1500000")


def test_dim_customer_joins_and_uppercases(spark):
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
    assert rows[1].asDict() == {
        "CustomerID": 1,
        "CustomerName": "SHELLY JUWITA",
        "Address": "JL. BOULEVARD NO. 31",
        "CityName": "Kelapa Gading",
        "StateName": "Jakarta Utara",
        "Age": 25,
        "Gender": "FEMALE",
        "Email": "shelly@gmail.com",
    }
    assert rows[2].CityName is None and rows[2].StateName is None


def _fact_sources(spark):
    sql_src = spark.createDataFrame(
        [(6, 6, datetime(2022, 2, 21, 13, 10), 50000, "Withdrawal", 1)], TX_COLS
    )
    excel_src = spark.createDataFrame(
        [
            (6.0, 6.0, datetime(2024, 1, 18, 13, 10), 50000.0, "Withdrawal", 1.0),
            (14.0, 13.0, datetime(2024, 1, 21, 14, 0), 1500000.0, "Deposit", 4.0),
        ],
        TX_COLS,
    )
    csv_src = spark.createDataFrame(
        [
            ("14", "13", "21-01-2024 14:00:00", "1500000", "Deposit", "4"),
            ("16", "15", "22-01-2024 09:00:00", "100000", "Deposit", "1"),
        ],
        TX_COLS,
    )
    return sql_src, excel_src, csv_src


def test_fact_transaction_union_dedup_keeps_first_source(spark):
    fact = load_fact_transaction.transform(*_fact_sources(spark))
    assert fact.columns == [
        "TransactionID",
        "AccountID",
        "TransactionDate",
        "Amount",
        "TransactionType",
        "BranchID",
    ]
    rows = {r.TransactionID: r for r in fact.collect()}
    assert sorted(rows) == [6, 14, 16]
    assert rows[6].TransactionDate == datetime(2022, 2, 21, 13, 10)
    assert rows[16].TransactionDate == datetime(2024, 1, 22, 9, 0)
    assert rows[14].Amount == Decimal("1500000")
    assert str(fact.schema["Amount"].dataType) == "DecimalType(19,4)"


def test_fact_transaction_foreign_key_rejects(spark):
    fact = load_fact_transaction.transform(*_fact_sources(spark))
    accounts = spark.createDataFrame([(6,), (13,)], ["AccountID"])
    branches = spark.createDataFrame([(1,), (4,)], ["BranchID"])
    valid, rejected = load_fact_transaction.split_foreign_key_rejects(fact, accounts, branches)
    assert sorted(r.TransactionID for r in valid.collect()) == [6, 14]
    assert [r.TransactionID for r in rejected.collect()] == [16]


def _register_dwh_views(spark):
    spark.createDataFrame(
        [(1, "SHELLY JUWITA"), (2, "BOBI RINALDO")], ["CustomerID", "CustomerName"]
    ).createOrReplaceTempView("DimCustomer")
    spark.createDataFrame(
        [
            (1, 1, "saving", Decimal("1500000"), "active"),
            (3, 1, "checking", Decimal("25000000"), "active"),
            (4, 1, "checking", Decimal("100"), "terminated"),
            (2, 2, "saving", Decimal("500000"), "active"),
        ],
        "AccountID int, CustomerID int, AccountType string, Balance decimal(19,4), Status string",
    ).createOrReplaceTempView("DimAccount")
    spark.createDataFrame(
        [
            (1, 1, datetime(2024, 1, 18, 9, 0), Decimal("100000"), "Deposit"),
            (2, 3, datetime(2024, 1, 18, 17, 0), Decimal("1000000"), "Withdrawal"),
            (3, 3, datetime(2024, 1, 19, 8, 0), Decimal("500000"), "Transfer"),
            (4, 2, datetime(2024, 1, 21, 8, 0), Decimal("1"), "Deposit"),
        ],
        "TransactionID int, AccountID int, TransactionDate timestamp, Amount decimal(19,4), "
        "TransactionType string",
    ).createOrReplaceTempView("FactTransaction")


def test_daily_transaction(spark):
    _register_dwh_views(spark)
    rows = [tuple(r) for r in daily_transaction(spark, "2024-01-18", "2024-01-20").collect()]
    assert rows == [
        (date(2024, 1, 18), 2, Decimal("1100000")),
        (date(2024, 1, 19), 1, Decimal("500000")),
    ]


def test_balance_per_customer(spark):
    _register_dwh_views(spark)
    rows = sorted(tuple(r) for r in balance_per_customer(spark, "shelly").collect())
    assert rows == [
        ("SHELLY JUWITA", "checking", Decimal("25000000"), Decimal("23500000")),
        ("SHELLY JUWITA", "saving", Decimal("1500000"), Decimal("1600000")),
    ]
