from __future__ import annotations

import datetime as dt
import decimal

from gold.config import GoldConfig, TransactionSource
from gold.fact_transaction import (
    FACT_COLUMNS,
    build_fact_transaction,
    dedupe_transactions,
    normalize_source,
    union_sources,
    upsert_by_key,
)


def _sorted_rows(df):
    return sorted((tuple(str(v) for v in r) for r in df.collect()))


def _sources(mssql_source, excel_source, csv_source):
    config = GoldConfig()
    specs = {s.name: s for s in config.sources()}
    return [
        (specs["mssql"], mssql_source),
        (specs["excel"], excel_source),
        (specs["csv"], csv_source),
    ]


def test_normalize_parses_timestamps_and_decimal(mssql_source):
    df = normalize_source(mssql_source, "mssql", 1)
    row = df.filter("transaction_id = 2").collect()[0]

    assert row.transaction_date == dt.datetime(2024, 1, 21, 8, 0, 0)
    assert row.amount == decimal.Decimal("500000.5000")
    assert df.schema["amount"].dataType.simpleString() == "decimal(19,4)"
    assert row._source_priority == 1


def test_normalize_accepts_pascal_case_columns(spark, mssql_source):
    pascal = mssql_source.toDF(
        "TransactionID",
        "AccountID",
        "TransactionDate",
        "Amount",
        "TransactionType",
        "BranchID",
        "_ingest_ts",
        "_source_file",
    )
    df = normalize_source(pascal, "mssql", 1)
    assert set(FACT_COLUMNS).issubset(set(df.columns))


def test_unparseable_date_becomes_null_without_dropping_the_row(csv_source):
    df = normalize_source(csv_source, "csv", 3)
    row = df.filter("transaction_id = 9").collect()[0]
    assert row.transaction_date is None
    assert row.amount == decimal.Decimal("30000.0000")


def test_dedupe_winner_is_highest_priority_source(mssql_source, excel_source, csv_source):
    normalized = [
        normalize_source(mssql_source, "mssql", 1),
        normalize_source(excel_source, "excel", 2),
        normalize_source(csv_source, "csv", 3),
    ]
    unique, duplicates = dedupe_transactions(union_sources(normalized))

    winner = unique.filter("transaction_id = 3").collect()
    assert len(winner) == 1
    assert winner[0]._source_name == "mssql"
    assert winner[0].amount == decimal.Decimal("100000.0000")

    losers = {r._source_name for r in duplicates.filter("transaction_id = 3").collect()}
    assert losers == {"excel", "csv"}
    assert unique.count() == 9
    assert duplicates.count() == 2


def test_dedupe_is_deterministic_regardless_of_union_order(mssql_source, excel_source, csv_source):
    forward = union_sources(
        [
            normalize_source(mssql_source, "mssql", 1),
            normalize_source(excel_source, "excel", 2),
            normalize_source(csv_source, "csv", 3),
        ]
    )
    reversed_order = union_sources(
        [
            normalize_source(csv_source, "csv", 3),
            normalize_source(excel_source, "excel", 2),
            normalize_source(mssql_source, "mssql", 1),
        ]
    )
    a = _sorted_rows(dedupe_transactions(forward)[0])
    b = _sorted_rows(dedupe_transactions(reversed_order)[0])
    assert a == b


def test_orphans_are_quarantined_not_dropped(
    mssql_source, excel_source, csv_source, dim_account, dim_branch
):
    result = build_fact_transaction(
        _sources(mssql_source, excel_source, csv_source), dim_account, dim_branch
    )

    fact_ids = sorted(r.transaction_id for r in result.fact.collect())
    orphans = {r.transaction_id: r._orphan_reason for r in result.orphans.collect()}

    assert fact_ids == [1, 2, 3, 4, 6, 8, 9]
    assert orphans == {5: "missing_branch_id", 7: "missing_account_id"}
    # NULL foreign keys are not orphans
    assert 8 in fact_ids


def test_fact_schema_matches_contract(
    mssql_source, excel_source, csv_source, dim_account, dim_branch
):
    result = build_fact_transaction(
        _sources(mssql_source, excel_source, csv_source), dim_account, dim_branch
    )
    assert result.fact.columns == list(FACT_COLUMNS)
    types = {f.name: f.dataType.simpleString() for f in result.fact.schema.fields}
    assert types == {
        "transaction_id": "int",
        "account_id": "int",
        "transaction_date": "timestamp",
        "amount": "decimal(19,4)",
        "transaction_type": "string",
        "branch_id": "int",
    }


def test_merge_is_idempotent(mssql_source, excel_source, csv_source, dim_account, dim_branch):
    result = build_fact_transaction(
        _sources(mssql_source, excel_source, csv_source), dim_account, dim_branch
    )
    fact = result.fact

    first = upsert_by_key(fact.limit(0), fact)
    second = upsert_by_key(first, fact)

    assert first.count() == second.count() == fact.count()
    assert _sorted_rows(first) == _sorted_rows(second)


def test_merge_updates_changed_rows(spark, mssql_source, excel_source, csv_source, dim_account, dim_branch):
    result = build_fact_transaction(
        _sources(mssql_source, excel_source, csv_source), dim_account, dim_branch
    )
    fact = result.fact
    updated = fact.filter("transaction_id = 1").withColumn(
        "amount", (fact["amount"] * 2).cast("decimal(19,4)")
    )

    merged = upsert_by_key(fact, updated)

    assert merged.count() == fact.count()
    row = merged.filter("transaction_id = 1").collect()[0]
    assert row.amount == decimal.Decimal("3000000.0000")


def test_source_priority_follows_talend_merge_order():
    sources = GoldConfig().sources()
    assert [s.name for s in sources] == ["mssql", "excel", "csv"]
    assert [s.priority for s in sources] == [1, 2, 3]
    assert isinstance(sources[0], TransactionSource)
    assert sources[0].table == "banking.bronze.mssql_transaction"
