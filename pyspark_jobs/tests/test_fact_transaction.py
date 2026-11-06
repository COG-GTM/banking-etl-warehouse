import datetime as dt
from decimal import Decimal

from pyspark_jobs import load_fact_transaction as fact
from pyspark_jobs.common.config import REPO_ROOT
from pyspark_jobs.common.io import read_csv_as_strings, read_excel_as_strings

CSV = str(REPO_ROOT / "data_sources" / "transaction_csv.csv")
XLSX = str(REPO_ROOT / "data_sources" / "transaction_excel.xlsx")
SQL_SCHEMA = (
    "transaction_id int, account_id int, transaction_date timestamp, amount int, transaction_type string, branch_id int"
)


def _sql_rows(spark):
    return spark.createDataFrame(
        [
            (6, 6, dt.datetime(2022, 2, 21, 13, 10), 50000, "Withdrawal", 1),
            (7, 6, dt.datetime(2022, 2, 21, 14, 0), 100000, "Payment", 1),
        ],
        SQL_SCHEMA,
    )


def _dims(spark, accounts=range(1, 22), branches=range(1, 6)):
    return (
        spark.createDataFrame([(a,) for a in accounts], "AccountID int"),
        spark.createDataFrame([(b,) for b in branches], "BranchID int"),
    )


def test_reads_repo_csv_with_dd_mm_yyyy_timestamps(spark):
    df = fact.normalize(read_csv_as_strings(spark, CSV, fact.SOURCE_COLUMNS), "csv", "dd-MM-yyyy HH:mm:ss")
    rows = {r.transaction_id: r for r in df.collect()}
    assert len(rows) == 12
    assert rows[14].transaction_date == dt.datetime(2024, 1, 21, 14, 0)
    assert rows[14].amount == Decimal("1500000")


def test_reads_repo_excel_with_pandas_engine(spark):
    raw = read_excel_as_strings(spark, XLSX, fact.SOURCE_COLUMNS, engine="pandas")
    df = fact.normalize(raw, "excel", fact.EXCEL_TIMESTAMP_FORMAT)
    rows = {r.transaction_id: r for r in df.collect()}
    assert sorted(rows) == [6, 7, 11, 12, 13, 14, 15]
    assert rows[6].transaction_date == dt.datetime(2024, 1, 18, 13, 10)


def test_unparseable_file_rows_are_dropped(spark):
    raw = spark.createDataFrame(
        [
            ("1", "1", "21-01-2024 14:00:00", "100", "Deposit", "1"),
            ("2", "x", "21-01-2024 14:00:00", "100", "Deposit", "1"),
            ("3", "1", "not a date", "100", "Deposit", "1"),
            ("", "1", "21-01-2024 14:00:00", "100", "Deposit", "1"),
            ("5", "1", "21-01-2024 14:00:00", "100", "Deposit", ""),
        ],
        ",".join(f"{c} string" for c in fact.SOURCE_COLUMNS),
    )
    ids = sorted(r.transaction_id for r in fact.normalize(raw, "csv", "dd-MM-yyyy HH:mm:ss").collect())
    assert ids == [1, 5]


def test_end_to_end_transform_matches_talend_semantics(spark):
    raw = {
        "sqlserver": _sql_rows(spark),
        "excel": read_excel_as_strings(spark, XLSX, fact.SOURCE_COLUMNS),
        "csv": read_csv_as_strings(spark, CSV, fact.SOURCE_COLUMNS),
    }
    dim_account, dim_branch = _dims(spark)
    out = fact.transform(raw, dim_account, dim_branch)
    assert out.columns == ["TransactionID", "AccountID", "TransactionDate", "Amount", "TransactionType", "BranchID"]
    rows = {r.TransactionID: r for r in out.collect()}

    # 6 and 7 exist in SQL Server and Excel: tUnite puts SQL Server first, tUniqRow keeps it.
    assert rows[6].TransactionDate == dt.datetime(2022, 2, 21, 13, 10)
    assert rows[7].TransactionDate == dt.datetime(2022, 2, 21, 14, 0)
    # 14 and 15 exist in Excel and CSV: Excel wins.
    assert rows[14].TransactionDate == dt.datetime(2024, 1, 21, 14, 0)
    # 23, 24, 25 reference accounts 22/23 which are not in DimAccount -> rejected like Talend's FK failures.
    assert not {23, 24, 25} & set(rows)
    assert sorted(rows) == [6, 7, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]


def test_null_foreign_keys_are_kept(spark):
    facts = spark.createDataFrame(
        [(1, None, None), (2, 99, 1), (3, 1, 99)], "transaction_id int, account_id int, branch_id int"
    )
    dim_account, dim_branch = _dims(spark, accounts=[1], branches=[1])
    valid, orphans = fact.reject_orphans(facts, dim_account, dim_branch)
    assert [r.transaction_id for r in valid.collect()] == [1]
    assert sorted(r.transaction_id for r in orphans.collect()) == [2, 3]
