from __future__ import annotations

import datetime
from decimal import Decimal

from conftest import bronze_df

from databricks.silver.dim_account import transform_dim_account

ACCOUNT_COLUMNS = [
    "account_id",
    "customer_id",
    "account_type",
    "balance",
    "date_opened",
    "status",
]


def test_account_casts_to_gold_contract_types(spark):
    source = bronze_df(
        spark,
        [
            {
                "account_id": "10",
                "customer_id": "100",
                "account_type": "Savings",
                "balance": "1234.5678",
                "date_opened": "2020-01-15",
                "status": "Active",
            },
            {
                "account_id": "11",
                "customer_id": "101",
                "account_type": "Checking",
                "balance": "$2,500.00",
                "date_opened": "15-01-2020",
                "status": "Closed",
            },
        ],
        ACCOUNT_COLUMNS,
    )

    result = transform_dim_account(source)

    assert dict(result.dtypes) == {
        "account_id": "int",
        "customer_id": "int",
        "account_type": "string",
        "balance": "decimal(19,4)",
        "date_opened": "date",
        "status": "string",
    }
    rows = {r.account_id: r for r in result.collect()}
    assert rows[10].balance == Decimal("1234.5678")
    assert rows[10].date_opened == datetime.date(2020, 1, 15)
    assert rows[11].balance == Decimal("2500.0000")
    assert rows[11].date_opened == datetime.date(2020, 1, 15)


def test_account_unparseable_values_become_null_not_errors(spark):
    source = bronze_df(
        spark,
        [
            {
                "account_id": "12",
                "customer_id": None,
                "account_type": None,
                "balance": "",
                "date_opened": "not-a-date",
                "status": "  ",
            }
        ],
        ACCOUNT_COLUMNS,
    )

    row = transform_dim_account(source).collect()[0]

    assert row.account_id == 12
    assert row.customer_id is None
    assert row.balance is None
    assert row.date_opened is None
    assert row.status is None
