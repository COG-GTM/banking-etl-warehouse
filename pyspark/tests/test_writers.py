from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.config import WarehouseSettings
from banking_etl.writers import ensure_table, read_table, table_identifier, write_table

from .conftest import shape


def _branches(spark: SparkSession, rows: list[tuple[int, str, str]]):  # type: ignore[no-untyped-def]
    return spark.createDataFrame(rows, schemas.DIM_BRANCH.struct)


def test_table_identifier_modes(warehouse: WarehouseSettings) -> None:
    assert table_identifier(warehouse, schemas.DIM_BRANCH) == f"delta.`{warehouse.base_path}/dim_branch`"
    uc = WarehouseSettings(storage="table", catalog="banking", db_schema="dwh", base_path="")
    assert table_identifier(uc, schemas.DIM_BRANCH) == "banking.dwh.dim_branch"


def test_ensure_table_matches_tsql_ddl(spark: SparkSession, warehouse: WarehouseSettings) -> None:
    for spec in schemas.WAREHOUSE_TABLES:
        ensure_table(spark, warehouse, spec)
        assert shape(read_table(spark, warehouse, spec).schema) == shape(spec.struct)
    fact = dict(read_table(spark, warehouse, schemas.FACT_TRANSACTION).dtypes)
    assert fact == {
        "TransactionID": "int",
        "AccountID": "int",
        "TransactionDate": "timestamp",
        "Amount": "decimal(19,4)",
        "TransactionType": "string",
        "BranchID": "int",
    }


def test_merge_is_idempotent_upsert(spark: SparkSession, warehouse: WarehouseSettings) -> None:
    write_table(
        spark, _branches(spark, [(1, "A", "x"), (2, "B", "y")]), warehouse, schemas.DIM_BRANCH, "merge"
    )
    write_table(
        spark, _branches(spark, [(1, "A", "x"), (2, "B", "y")]), warehouse, schemas.DIM_BRANCH, "merge"
    )
    write_table(
        spark, _branches(spark, [(2, "B2", "y2"), (3, "C", "z")]), warehouse, schemas.DIM_BRANCH, "merge"
    )
    rows = sorted(tuple(r) for r in read_table(spark, warehouse, schemas.DIM_BRANCH).collect())
    assert rows == [(1, "A", "x"), (2, "B2", "y2"), (3, "C", "z")]


def test_overwrite_replaces_rows(spark: SparkSession, warehouse: WarehouseSettings) -> None:
    ts = dt.datetime(2024, 1, 1, 9, 0)
    first = spark.createDataFrame(
        [(1, 1, ts, Decimal(1), "Deposit", 1), (2, 1, ts, Decimal(2), "Deposit", 1)],
        schemas.FACT_TRANSACTION.struct,
    )
    second = spark.createDataFrame(
        [(3, 1, ts, Decimal("3.5"), "Payment", 1)], schemas.FACT_TRANSACTION.struct
    )
    assert write_table(spark, first, warehouse, schemas.FACT_TRANSACTION, "overwrite") == 2
    assert write_table(spark, second, warehouse, schemas.FACT_TRANSACTION, "overwrite") == 1
    rows = read_table(spark, warehouse, schemas.FACT_TRANSACTION).collect()
    assert [(r.TransactionID, r.Amount) for r in rows] == [(3, Decimal("3.5000"))]


def test_varchar_length_is_enforced(spark: SparkSession, warehouse: WarehouseSettings) -> None:
    too_long = _branches(spark, [(1, "X" * 101, "x")])
    with pytest.raises(Exception, match="(?i)char|varchar|length"):
        write_table(spark, too_long, warehouse, schemas.DIM_BRANCH, "merge")
