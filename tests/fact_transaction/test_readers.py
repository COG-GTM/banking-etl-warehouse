from __future__ import annotations

from decimal import Decimal

from conftest import ts
from fact_transaction.readers import (
    NORMALIZED_SCHEMA,
    SOURCE_CSV,
    SOURCE_EXCEL,
    read_csv_transactions,
    read_excel_transactions,
)


def test_read_csv_matches_normalized_schema(spark, csv_path):
    df = read_csv_transactions(spark, csv_path)
    assert df.schema.fieldNames() == NORMALIZED_SCHEMA.fieldNames()
    assert [f.dataType for f in df.schema.fields] == [
        f.dataType for f in NORMALIZED_SCHEMA.fields
    ]
    assert df.count() == 12
    assert {r["_source"] for r in df.select("_source").distinct().collect()} == {SOURCE_CSV}


def test_read_csv_parses_dd_mm_yyyy_timestamps(spark, csv_path):
    row = (
        read_csv_transactions(spark, csv_path)
        .filter("TransactionID = 14")
        .collect()[0]
    )
    assert row["TransactionDate"] == ts(2024, 1, 21, 14, 0)
    assert row["Amount"] == Decimal("1500000.0000")
    assert (row["AccountID"], row["TransactionType"], row["BranchID"]) == (13, "Deposit", 4)


def test_read_csv_has_no_unparsed_dates(spark, csv_path):
    df = read_csv_transactions(spark, csv_path)
    assert df.filter("TransactionDate IS NULL").count() == 0


def test_read_excel_matches_normalized_schema(spark, excel_path):
    df = read_excel_transactions(spark, excel_path)
    assert [f.dataType for f in df.schema.fields] == [
        f.dataType for f in NORMALIZED_SCHEMA.fields
    ]
    assert df.count() == 7
    assert {r["_source"] for r in df.select("_source").distinct().collect()} == {SOURCE_EXCEL}
    assert sorted(r["TransactionID"] for r in df.collect()) == [6, 7, 11, 12, 13, 14, 15]


def test_read_excel_values(spark, excel_path):
    row = read_excel_transactions(spark, excel_path).filter("TransactionID = 6").collect()[0]
    assert row["TransactionDate"] == ts(2024, 1, 18, 13, 10)
    assert row["Amount"] == Decimal("50000.0000")
    assert row["TransactionType"] == "Withdrawal"


def test_db_fixture_is_normalized(db_df):
    assert db_df.schema.fieldNames() == NORMALIZED_SCHEMA.fieldNames()
    assert db_df.count() == 6
