from datetime import datetime
from decimal import Decimal

import pytest

import load_fact_transaction as job
from conftest import REPO_ROOT

EXCEL_PATH = str(REPO_ROOT / "data_sources" / "transaction_excel.xlsx")
CSV_PATH = str(REPO_ROOT / "data_sources" / "transaction_csv.csv")

# Contents of sample.dbo.transaction_db restored from data_sources/sample.bak.
SQL_ROWS = [
    (1, 1, datetime(2022, 1, 1, 9, 10), 100000, "Deposit", 1),
    (2, 2, datetime(2022, 1, 1, 10, 10), 1000000, "Deposit", 1),
    (3, 3, datetime(2022, 1, 11, 8, 30), 10000000, "Transfer", 1),
    (4, 3, datetime(2022, 1, 11, 10, 45), 1000000, "Withdrawal", 1),
    (5, 5, datetime(2022, 2, 21, 11, 10), 200000, "Deposit", 1),
    (6, 6, datetime(2022, 2, 21, 13, 10), 50000, "Withdrawal", 1),
    (7, 6, datetime(2022, 2, 21, 14, 0), 100000, "Payment", 1),
    (8, 7, datetime(2022, 3, 5, 9, 10), 5000000, "Deposit", 1),
    (9, 8, datetime(2022, 3, 15, 10, 40), 300000, "Withdrawal", 2),
    (10, 9, datetime(2022, 3, 24, 12, 10), 2000000, "Deposit", 1),
]
SQL_COLUMNS = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id"]


@pytest.fixture
def sql_df(spark):
    return job.standardize(spark.createDataFrame(SQL_ROWS, SQL_COLUMNS), "sqlserver")


@pytest.fixture
def config(tmp_path):
    return job.JobConfig(
        jdbc_url="unused",
        jdbc_table="dbo.transaction_db",
        secret_scope="unused",
        jdbc_user_key="unused",
        jdbc_password_key="unused",
        excel_path=EXCEL_PATH,
        csv_path=CSV_PATH,
        target_table=f"dwh_test_{tmp_path.name.replace('-', '_')}.fact_transaction",
    )


def test_csv_dates_parsed_with_dd_mm_yyyy(spark):
    df = job.read_csv_transactions(spark, CSV_PATH)
    assert df.count() == 12
    assert df.filter("TransactionDate IS NULL").count() == 0
    row = df.filter("TransactionID = 14").first()
    assert row.TransactionDate == datetime(2024, 1, 21, 14, 0, 0)


def test_excel_source_standardised(spark):
    df = job.read_excel_transactions(spark, EXCEL_PATH)
    assert df.count() == 7
    assert df.filter("TransactionID = 6").first().TransactionDate == datetime(2024, 1, 18, 13, 10)


def test_sources_share_fact_schema(spark, sql_df):
    excel = job.read_excel_transactions(spark, EXCEL_PATH)
    csv = job.read_csv_transactions(spark, CSV_PATH)
    expected = [(f.name, f.dataType) for f in job.FACT_TRANSACTION_SCHEMA.fields]
    for df in (sql_df, excel, csv):
        assert [(f.name, f.dataType) for f in df.schema.fields][:6] == expected


def test_union_and_dedup_counts(spark, sql_df):
    unioned = job.union_sources(
        sql_df,
        job.read_excel_transactions(spark, EXCEL_PATH),
        job.read_csv_transactions(spark, CSV_PATH),
    )
    deduped = job.deduplicate(unioned)
    stats = job.validate_dedup(unioned, deduped)

    assert (stats.rows_before, stats.rows_after, stats.duplicates_removed) == (29, 25, 4)
    assert deduped.columns == job.FACT_COLUMNS
    assert sorted(r.TransactionID for r in deduped.collect()) == list(range(1, 26))


def test_dedup_keeps_first_source_like_tuniqrow(spark, sql_df):
    """IDs 6/7 conflict between SQL Server and Excel; Talend keeps the SQL Server row."""
    unioned = job.union_sources(sql_df, job.read_excel_transactions(spark, EXCEL_PATH))
    rows = {r.TransactionID: r for r in job.deduplicate(unioned).collect()}
    assert rows[6].TransactionDate == datetime(2022, 2, 21, 13, 10)
    assert rows[7].TransactionDate == datetime(2022, 2, 21, 14, 0)


def test_dedup_collapses_duplicates_within_one_source(spark):
    df = job.standardize(spark.createDataFrame(SQL_ROWS[:2] + SQL_ROWS[:1], SQL_COLUMNS), "sqlserver")
    assert job.deduplicate(df).count() == 2


def test_run_merges_idempotently(spark, sql_df, config):
    stats = job.run(spark, config, sql_df=sql_df)
    assert stats.duplicates_removed == 4

    target = spark.table(config.target_table)
    assert target.columns == job.FACT_COLUMNS
    assert target.count() == 25
    row = target.filter("TransactionID = 25").first()
    assert row.Amount == Decimal("400000.0000")
    assert row.TransactionType == "Deposit"

    job.run(spark, config, sql_df=sql_df)
    assert spark.table(config.target_table).count() == 25


def test_run_merge_updates_changed_rows(spark, sql_df, config):
    job.run(spark, config, sql_df=sql_df)
    changed = [(1, 1, datetime(2022, 1, 1, 9, 10), 999, "Deposit", 1)] + SQL_ROWS[1:]
    changed_df = job.standardize(spark.createDataFrame(changed, SQL_COLUMNS), "sqlserver")
    job.run(spark, config, sql_df=changed_df)

    target = spark.table(config.target_table)
    assert target.count() == 25
    assert target.filter("TransactionID = 1").first().Amount == Decimal("999.0000")


def test_rerun_rewrites_nothing(spark, sql_df, config):
    job.run(spark, config, sql_df=sql_df)
    job.run(spark, config, sql_df=sql_df)
    metrics = (
        spark.sql(f"DESCRIBE HISTORY {config.target_table}")
        .orderBy("version", ascending=False)
        .first()
        .operationMetrics
    )
    assert metrics["numTargetRowsUpdated"] == "0"
    assert metrics["numTargetRowsInserted"] == "0"
