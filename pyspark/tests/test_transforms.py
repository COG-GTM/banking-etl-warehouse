from __future__ import annotations

import datetime as dt
from decimal import Decimal

from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.transforms import (
    deduplicate_transactions,
    split_orphan_facts,
    transform_dim_account,
    transform_dim_branch,
    transform_dim_customer,
    transform_fact_transaction,
    unite_transactions,
)

from .conftest import shape

TS = dt.datetime(2024, 1, 20, 10, 0)


def test_dim_branch_maps_columns(spark: SparkSession) -> None:
    src = spark.createDataFrame([(1, "KC Jakarta", "Jl. Gatot Subroto No 13")], schemas.SRC_BRANCH)
    out = transform_dim_branch(src)
    assert out.schema == schemas.DIM_BRANCH.struct
    assert out.collect()[0].asDict() == {
        "BranchID": 1,
        "BranchName": "KC Jakarta",
        "BranchLocation": "Jl. Gatot Subroto No 13",
    }


def test_dim_account_casts_money_and_date(spark: SparkSession) -> None:
    src = spark.createDataFrame(
        [(1, 1, "saving", Decimal("1500000"), dt.datetime(2020, 5, 1, 9, 0), "active")], schemas.SRC_ACCOUNT
    )
    out = transform_dim_account(src)
    assert out.schema == schemas.DIM_ACCOUNT.struct
    row = out.collect()[0]
    assert row.Balance == Decimal("1500000.0000")
    assert row.DateOpened == dt.date(2020, 5, 1)


def test_dim_customer_left_joins_and_uppercases(spark: SparkSession) -> None:
    customer = spark.createDataFrame(
        [
            (1, "Shelly Juwita", "Jl. Boulevard No. 31", 2, "25", "female", "shelly@gmail.com"),
            (2, "No City", "Jl. X", 99, " 40 ", "male", "Mixed@Case.com"),
        ],
        schemas.SRC_CUSTOMER,
    )
    city = spark.createDataFrame([(2, "Kelapa Gading", 1)], schemas.SRC_CITY)
    state = spark.createDataFrame([(1, "Jakarta Utara")], schemas.SRC_STATE)
    out = transform_dim_customer(customer, city, state)
    assert out.schema == schemas.DIM_CUSTOMER.struct
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
    assert rows[2]["CityName"] is None and rows[2]["StateName"] is None
    assert rows[2]["Age"] == 40
    assert rows[2]["Email"] == "Mixed@Case.com"


def test_union_and_dedupe_prefers_sql_then_excel_then_csv(spark: SparkSession) -> None:
    def frame(rows: list[tuple]):  # type: ignore[type-arg]
        return spark.createDataFrame(rows, schemas.SRC_TRANSACTION)

    sql = frame([(6, 6, dt.datetime(2022, 2, 21, 13, 10), Decimal(50000), "Withdrawal", 1)])
    excel = frame(
        [
            (6, 6, dt.datetime(2024, 1, 18, 13, 10), Decimal(50000), "Withdrawal", 1),
            (14, 13, TS, Decimal(1), "Deposit", 4),
        ]
    )
    csv = frame([(14, 13, TS, Decimal(999), "Deposit", 4), (16, 15, TS, Decimal(100000), "Deposit", 1)])

    out = transform_fact_transaction(deduplicate_transactions(unite_transactions(sql, excel, csv)))
    assert shape(out.schema) == shape(schemas.FACT_TRANSACTION.struct)
    rows = {r.TransactionID: r for r in out.collect()}
    assert sorted(rows) == [6, 14, 16]
    assert rows[6].TransactionDate == dt.datetime(2022, 2, 21, 13, 10)
    assert rows[14].Amount == Decimal("1.0000")


def test_dedupe_keeps_first_row_within_a_source(spark: SparkSession) -> None:
    csv = spark.createDataFrame(
        [(1, 1, TS, Decimal(10), "Deposit", 1), (1, 1, TS, Decimal(20), "Deposit", 1)],
        schemas.SRC_TRANSACTION,
    ).coalesce(1)
    out = deduplicate_transactions(unite_transactions(csv)).collect()
    assert [r.amount for r in out] == [Decimal("10.0000")]


def test_split_orphan_facts_applies_foreign_keys(spark: SparkSession) -> None:
    fact = spark.createDataFrame(
        [
            (1, 1, TS, Decimal(1), "Deposit", 1),
            (2, 99, TS, Decimal(1), "Deposit", 1),
            (3, 1, TS, Decimal(1), "Deposit", 99),
            (4, None, TS, Decimal(1), "Deposit", None),
            (None, 1, TS, Decimal(1), "Deposit", 1),
        ],
        schemas.FACT_TRANSACTION.struct.fromJson(
            {
                **schemas.FACT_TRANSACTION.struct.jsonValue(),
                "fields": [
                    {**f, "nullable": True} for f in schemas.FACT_TRANSACTION.struct.jsonValue()["fields"]
                ],
            }
        ),
    )
    accounts = spark.createDataFrame([(1,)], "AccountID INT")
    branches = spark.createDataFrame([(1,)], "BranchID INT")
    valid, rejected = split_orphan_facts(fact, accounts, branches)
    assert sorted(r.TransactionID for r in valid.collect()) == [1, 4]
    assert sorted((r.TransactionID or 0) for r in rejected.collect()) == [0, 2, 3]
