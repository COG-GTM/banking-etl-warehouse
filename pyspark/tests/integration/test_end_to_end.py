"""End-to-end run of all jobs against a real SQL Server source database, with parity
checks of the analytics against the legacy T-SQL stored procedures.

Requires the legacy ``sample`` source database (restore ``data_sources/sample.bak``) and
``BANKING_ETL_IT=1`` plus ``SOURCE_DB_USER`` / ``SOURCE_DB_PASSWORD`` (see pyspark/README.md).
"""

from __future__ import annotations

import datetime as dt
import os
import re
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymssql
import pytest
from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.analytics import balance_per_customer, daily_transaction
from banking_etl.config import EtlConfig, load_config
from banking_etl.jobs import load_dim_account, load_dim_branch, load_dim_customer, load_fact_transaction
from banking_etl.writers import read_table

from ..conftest import REPO_ROOT, shape

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("BANKING_ETL_IT") != "1", reason="set BANKING_ETL_IT=1 to run"),
]

LEGACY_DB = "banking_legacy_parity"
SQL_DIR = REPO_ROOT / "sql_scripts"


@pytest.fixture(scope="module")
def config(tmp_path_factory: pytest.TempPathFactory) -> EtlConfig:
    base = tmp_path_factory.mktemp("warehouse") / "dwh"
    return load_config(env={**os.environ, "DWH_STORAGE": "path", "DWH_BASE_PATH": str(base)})


@pytest.fixture(scope="module")
def loaded(spark: SparkSession, config: EtlConfig) -> dict[str, int]:
    return {
        "dim_branch": load_dim_branch.run(spark, config),
        "dim_account": load_dim_account.run(spark, config),
        "dim_customer": load_dim_customer.run(spark, config),
        "fact_transaction": load_fact_transaction.run(spark, config),
    }


def _mssql(database: str = "master") -> pymssql.Connection:
    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "localhost"),
        port=os.environ.get("MSSQL_PORT", "1433"),
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        database=database,
        autocommit=True,
    )


def _run_script(cursor: Any, path: Path) -> None:
    sql = path.read_text().replace("DWH", LEGACY_DB)
    # 02_create_procedures.sql puts a PRINT before each CREATE PROCEDURE in the same batch,
    # which SQL Server rejects; drop the PRINTs so the procedures are created unchanged.
    sql = re.sub(r"^\s*PRINT\s+'[^']*';\s*$", "", sql, flags=re.MULTILINE)
    for batch in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE):
        if batch.strip():
            cursor.execute(batch)


@pytest.fixture(scope="module")
def legacy(spark: SparkSession, config: EtlConfig, loaded: dict[str, int]) -> Iterator[Any]:
    """Legacy DWH built from sql_scripts/*.sql and populated with the Delta tables."""
    with _mssql() as conn:
        cursor = conn.cursor()
        cursor.execute(f"IF DB_ID('{LEGACY_DB}') IS NOT NULL DROP DATABASE {LEGACY_DB}")
        _run_script(cursor, SQL_DIR / "01_create_tables.sql")
        _run_script(cursor, SQL_DIR / "02_create_procedures.sql")
    conn = _mssql(LEGACY_DB)
    cursor = conn.cursor()
    for spec, table in (
        (schemas.DIM_BRANCH, "DimBranch"),
        (schemas.DIM_CUSTOMER, "DimCustomer"),
        (schemas.DIM_ACCOUNT, "DimAccount"),
        (schemas.FACT_TRANSACTION, "FactTransaction"),
    ):
        rows = [tuple(r) for r in read_table(spark, config.warehouse, spec).collect()]
        placeholders = ", ".join(["%s"] * len(spec.columns))
        cursor.executemany(
            f"INSERT INTO {table} ({', '.join(spec.column_names)}) VALUES ({placeholders})", rows
        )
    yield cursor
    conn.close()
    with _mssql() as admin:
        admin.cursor().execute(f"DROP DATABASE {LEGACY_DB}")


def test_row_counts(loaded: dict[str, int]) -> None:
    # 10 SQL + 7 Excel + 12 CSV rows -> 25 unique ids; 23-25 reference accounts 22/23,
    # which do not exist in dbo.account, and are rejected like the legacy foreign keys.
    assert loaded == {"dim_branch": 5, "dim_account": 21, "dim_customer": 20, "fact_transaction": 22}


def test_tables_match_tsql_ddl(spark: SparkSession, config: EtlConfig, loaded: dict[str, int]) -> None:
    for spec in schemas.WAREHOUSE_TABLES:
        assert shape(read_table(spark, config.warehouse, spec).schema) == shape(spec.struct)


def test_dimension_reload_is_idempotent(
    spark: SparkSession, config: EtlConfig, loaded: dict[str, int]
) -> None:
    before = sorted(read_table(spark, config.warehouse, schemas.DIM_CUSTOMER).collect())
    assert load_dim_customer.run(spark, config) == 20
    assert sorted(read_table(spark, config.warehouse, schemas.DIM_CUSTOMER).collect()) == before


def test_customer_cleansing(spark: SparkSession, config: EtlConfig, loaded: dict[str, int]) -> None:
    customers = read_table(spark, config.warehouse, schemas.DIM_CUSTOMER).collect()
    assert all(c.CustomerName == c.CustomerName.upper() for c in customers)
    assert all(c.Gender in ("MALE", "FEMALE") for c in customers)
    assert all(c.CityName and c.StateName for c in customers)


def test_fact_dedupe_prefers_sql_server(
    spark: SparkSession, config: EtlConfig, loaded: dict[str, int]
) -> None:
    fact = {
        r.TransactionID: r for r in read_table(spark, config.warehouse, schemas.FACT_TRANSACTION).collect()
    }
    assert sorted(fact) == list(range(1, 23))
    assert fact[6].TransactionDate == dt.datetime(2022, 2, 21, 13, 10)  # SQL Server copy, not Excel
    assert fact[14].TransactionDate == dt.datetime(2024, 1, 21, 14, 0)  # Excel copy, not CSV


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2022-01-01", "2024-12-31"),
        ("2022-02-21", "2022-02-21"),
        ("2024-01-19", "2024-01-22"),
        ("2030-01-01", "2030-12-31"),
    ],
)
def test_daily_transaction_matches_stored_procedure(
    spark: SparkSession, config: EtlConfig, legacy: Any, start: str, end: str
) -> None:
    legacy.execute("EXEC sp_DailyTransaction @start_date=%s, @end_date=%s", (start, end))
    expected = [(r[0], r[1], Decimal(r[2]).quantize(Decimal("0.0001"))) for r in legacy.fetchall()]
    fact = read_table(spark, config.warehouse, schemas.FACT_TRANSACTION)
    actual = [tuple(r) for r in daily_transaction(fact, start, end).collect()]
    assert actual == expected
    if start == "2022-01-01":
        assert len(actual) > 5


@pytest.mark.parametrize("name", ["Shelly", "shelly juwita", "a", "nobody-at-all", "%"])
def test_balance_per_customer_matches_stored_procedure(
    spark: SparkSession, config: EtlConfig, legacy: Any, name: str
) -> None:
    legacy.execute("EXEC sp_BalancePerCustomer @customer_name=%s", (name,))
    expected = sorted((r[0], r[1], Decimal(r[2]), Decimal(r[3])) for r in legacy.fetchall())
    actual = sorted(
        tuple(r)
        for r in balance_per_customer(
            read_table(spark, config.warehouse, schemas.FACT_TRANSACTION),
            read_table(spark, config.warehouse, schemas.DIM_ACCOUNT),
            read_table(spark, config.warehouse, schemas.DIM_CUSTOMER),
            name,
        ).collect()
    )
    assert actual == expected
    if name == "a":
        assert len(actual) > 3
