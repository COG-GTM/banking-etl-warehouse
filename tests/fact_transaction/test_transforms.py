from __future__ import annotations

from decimal import Decimal

import pytest
from fact_transaction.readers import read_csv_transactions, read_excel_transactions
from fact_transaction.transforms import (
    FACT_SCHEMA,
    build_fact_transaction,
    dedupe_transactions,
    unify_transactions,
    validate_referential_integrity,
)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType


@pytest.fixture()
def unified(spark, db_df, csv_path, excel_path):
    return unify_transactions(
        db_df,
        read_excel_transactions(spark, excel_path),
        read_csv_transactions(spark, csv_path),
    )


def test_unify_keeps_every_source_row(unified):
    assert unified.count() == 6 + 7 + 12
    counts = {r["_source"]: r["n"] for r in unified.groupBy("_source").count().withColumnRenamed("count", "n").collect()}
    assert counts == {"db": 6, "excel": 7, "csv": 12}


def test_unify_requires_at_least_one_dataframe():
    with pytest.raises(ValueError):
        unify_transactions()


def test_dedupe_removes_cross_source_duplicates(unified):
    deduped = dedupe_transactions(unified)
    ids = sorted(r["TransactionID"] for r in deduped.collect())
    assert ids == [1, 2, 3, 4, 5, 6, 7] + list(range(11, 26))
    assert deduped.count() == 22
    assert unified.count() - deduped.count() == 3


def test_dedupe_prefers_db_then_excel_then_csv(unified):
    deduped = dedupe_transactions(unified)
    by_id = {r["TransactionID"]: r for r in deduped.collect()}
    # 6 exists in db + excel -> db wins (and keeps the db amount)
    assert by_id[6]["_source"] == "db"
    assert by_id[6]["Amount"] == Decimal("999999.0000")
    # 14 and 15 exist in excel + csv -> excel wins
    assert by_id[14]["_source"] == "excel"
    assert by_id[15]["_source"] == "excel"
    # ids only present in csv keep the csv row
    assert by_id[25]["_source"] == "csv"


def test_dedupe_is_deterministic(unified):
    first = sorted(
        (r["TransactionID"], r["_source"]) for r in dedupe_transactions(unified).collect()
    )
    second = sorted(
        (r["TransactionID"], r["_source"])
        for r in dedupe_transactions(unified.repartition(4, "TransactionType")).collect()
    )
    assert first == second


def test_build_fact_transaction_schema(unified):
    fact = build_fact_transaction(dedupe_transactions(unified))
    assert fact.schema.fieldNames() == list(FACT_SCHEMA.fieldNames())
    assert [f.dataType for f in fact.schema.fields] == [f.dataType for f in FACT_SCHEMA.fields]
    assert "_source" not in fact.columns
    assert fact.count() == 22


def test_validate_referential_integrity_detects_orphans(spark, unified):
    fact = build_fact_transaction(dedupe_transactions(unified))

    account_ids = [r["AccountID"] for r in fact.select("AccountID").distinct().collect()]
    dim_account = spark.createDataFrame(
        [(a,) for a in account_ids if a != 23],
        schema=StructType([StructField("AccountID", IntegerType())]),
    )
    dim_branch = spark.createDataFrame(
        [(b,) for b in (1, 2, 3, 4)],
        schema=StructType([StructField("BranchID", IntegerType())]),
    )

    orphans = validate_referential_integrity(fact, dim_account, dim_branch)
    assert orphans.filter(F.col("_orphan_account")).count() == 2  # accounts 23 -> ids 24, 25
    assert orphans.filter(F.col("_orphan_branch")).count() > 0
    assert set(orphans.columns) == set(FACT_SCHEMA.fieldNames()) | {
        "_orphan_account",
        "_orphan_branch",
    }


def test_validate_referential_integrity_clean_load(spark, unified):
    fact = build_fact_transaction(dedupe_transactions(unified))
    dim_account = fact.select("AccountID").distinct()
    dim_branch = fact.select("BranchID").distinct()
    assert validate_referential_integrity(fact, dim_account, dim_branch).count() == 0
