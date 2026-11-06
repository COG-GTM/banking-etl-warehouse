"""End-to-end parity against a real SQL Server (sample restored, DWH DDL + procedures applied).

Skipped unless RUN_SQLSERVER_TESTS=1. Uses the same env vars as the jobs (SOURCE_DB_*, DWH_DB_*)
and needs the mssql-jdbc driver (SPARK_JDBC_PACKAGES / SPARK_JDBC_JARS) resolvable by Spark.
"""

import datetime as dt
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("RUN_SQLSERVER_TESTS") != "1", reason="RUN_SQLSERVER_TESTS!=1")


@pytest.fixture(scope="module")
def etl():
    from pyspark_jobs.common.config import EtlConfig
    from pyspark_jobs.common.spark_session import get_spark
    from pyspark_jobs.run_all import run_all

    cfg = EtlConfig.from_env()
    spark = get_spark("parity-tests", cfg)
    results = run_all(spark, cfg, full_refresh=True)
    return spark, cfg, results


def _exec_proc(spark, cfg, sql, params):
    from pyspark_jobs.common.io import _jdbc_connection

    conn = _jdbc_connection(spark, cfg.dwh_db)
    try:
        stmt = conn.prepareStatement(sql)
        for i, value in enumerate(params, start=1):
            stmt.setString(i, str(value))
        rs = stmt.executeQuery()
        cols = rs.getMetaData().getColumnCount()
        rows = []
        while rs.next():
            rows.append(tuple(rs.getString(i) for i in range(1, cols + 1)))
        return rows
    finally:
        conn.close()


def _as_strings(df):
    return [tuple(None if v is None else str(v) for v in row) for row in df.collect()]


def test_row_counts_match_talend_output(etl):
    _, _, results = etl
    assert results == {"Load_DimBranch": 5, "Load_DimAccount": 21, "Load_DimCustomer": 20, "Load_FactTransaction": 22}


def test_rerun_without_full_refresh_is_idempotent(etl):
    from pyspark_jobs.run_all import run_all

    spark, cfg, _ = etl
    assert run_all(spark, cfg) == {
        "Load_DimBranch": 0,
        "Load_DimAccount": 0,
        "Load_DimCustomer": 0,
        "Load_FactTransaction": 22,
    }


@pytest.mark.parametrize(
    "start,end",
    [
        ("2024-01-18", "2024-01-20"),
        ("2022-01-01", "2024-12-31"),
        ("2024-01-22", "2024-01-22"),
        ("2030-01-01", "2030-01-02"),
    ],
)
def test_daily_transaction_matches_procedure(etl, start, end):
    from pyspark_jobs.reports.daily_transaction import run

    spark, cfg, _ = etl
    expected = _exec_proc(spark, cfg, "EXEC sp_DailyTransaction @start_date = ?, @end_date = ?", [start, end])
    actual = _as_strings(run(spark, cfg, dt.date.fromisoformat(start), dt.date.fromisoformat(end)))
    assert actual == expected


@pytest.mark.parametrize("name", ["shelly", "SHELLY JUWITA", "a", "Rinaldo", "nobody"])
def test_balance_per_customer_matches_procedure(etl, name):
    from pyspark_jobs.reports.balance_per_customer import run

    spark, cfg, _ = etl
    expected = sorted(_exec_proc(spark, cfg, "EXEC sp_BalancePerCustomer @customer_name = ?", [name]))
    actual = sorted(_as_strings(run(spark, cfg, name)))
    assert actual == expected
