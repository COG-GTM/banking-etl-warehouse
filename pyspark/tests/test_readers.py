from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.config import CsvSource, ExcelSource, SqlServerSource
from banking_etl.readers import read_transaction_csv, read_transaction_excel, sqlserver_query

from .conftest import DATA_SOURCES


def _csv_source(path: Path) -> CsvSource:
    return CsvSource(
        path=str(path),
        delimiter=",",
        timestamp_format="dd-MM-yyyy HH:mm:ss",
        encoding="ISO-8859-15",
        mode="DROPMALFORMED",
    )


def test_sqlserver_query_mirrors_talend_column_list() -> None:
    source = SqlServerSource(jdbc_url="jdbc:sqlserver://x", db_schema="dbo")
    assert sqlserver_query(source, "branch", schemas.SRC_BRANCH) == (
        "SELECT [branch].[branch_id],\n       [branch].[branch_name],\n       [branch].[branch_location]\n"
        "FROM [dbo].[branch]"
    )


def test_read_transaction_csv_parses_dd_mm_yyyy(spark: SparkSession) -> None:
    df = read_transaction_csv(
        spark, _csv_source(DATA_SOURCES / "transaction_csv.csv"), schemas.SRC_TRANSACTION
    )
    assert df.schema == schemas.SRC_TRANSACTION
    rows = {r.transaction_id: r for r in df.collect()}
    assert sorted(rows) == list(range(14, 26))
    assert rows[14].transaction_date == dt.datetime(2024, 1, 21, 14, 0, 0)
    assert rows[17].amount == Decimal("100000.0000")
    assert rows[17].transaction_type == "Withdrawal"


def test_read_transaction_csv_drops_malformed_rows(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text(
        "transaction_id,account_id,transaction_date,amount,transaction_type,branch_id\n"
        "1,1,21-01-2024 14:00:00,10,Deposit,1\n"
        "2,1,2024/01/21,10,Deposit,1\n"
        "\n"
        "3,1,22-01-2024 09:30:00,5,Payment,2\n"
    )
    df = read_transaction_csv(spark, _csv_source(path), schemas.SRC_TRANSACTION)
    assert sorted(r.transaction_id for r in df.collect()) == [1, 3]


def test_read_transaction_excel_with_pandas(spark: SparkSession) -> None:
    source = ExcelSource(
        path=str(DATA_SOURCES / "transaction_excel.xlsx"),
        sheet="Sheet1",
        engine="pandas",
        timestamp_format="dd-MM-yyyy HH:mm:ss",
    )
    df = read_transaction_excel(spark, source, schemas.SRC_TRANSACTION)
    assert df.schema == schemas.SRC_TRANSACTION
    rows = {r.transaction_id: r for r in df.collect()}
    assert sorted(rows) == [6, 7, 11, 12, 13, 14, 15]
    assert rows[6].transaction_date == dt.datetime(2024, 1, 18, 13, 10, 0)
    assert rows[11].amount == Decimal("1000000.0000")
    assert rows[15].transaction_type == "Transfer"


def test_read_transaction_excel_parses_text_dates(spark: SparkSession, tmp_path: Path) -> None:
    import pandas as pd

    path = tmp_path / "text_dates.xlsx"
    pd.DataFrame(
        {
            "transaction_id": [1, 2],
            "account_id": [1, 2],
            "transaction_date": ["21-01-2024 14:00:00", None],
            "amount": [10.0, 20.5],
            "transaction_type": ["Deposit", "Payment"],
            "branch_id": [1, 2],
        }
    ).to_excel(path, sheet_name="Sheet1", index=False)
    source = ExcelSource(
        path=str(path), sheet="Sheet1", engine="pandas", timestamp_format="dd-MM-yyyy HH:mm:ss"
    )
    rows = {
        r.transaction_id: r for r in read_transaction_excel(spark, source, schemas.SRC_TRANSACTION).collect()
    }
    assert rows[1].transaction_date == dt.datetime(2024, 1, 21, 14, 0, 0)
    assert rows[2].transaction_date is None
    assert rows[2].amount == Decimal("20.5000")
