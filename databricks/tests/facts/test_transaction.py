from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from banking_etl.facts.transaction import (
    BUSINESS_COLUMNS,
    FACT_SCHEMA,
    _to_int,
    build_silver,
    merge_into_gold,
    normalize_source,
    parity_diff,
    read_parity_csv,
    resolve_tables,
    run_gold,
    run_silver,
    split_fk,
)

from .conftest import CSV_DDL, EXCEL_DDL, PARITY_CSV, dim_frames


def _rows(df, *cols):
    return sorted(tuple(r) for r in df.select(*(cols or BUSINESS_COLUMNS)).collect())


def test_resolve_tables_defaults():
    t = resolve_tables()
    assert t.bronze_sqlserver == "migration_demo.banking_mig_bronze.sqlserver_transaction_db"
    assert t.bronze_excel == "migration_demo.banking_mig_bronze.file_transaction_excel"
    assert t.bronze_csv == "migration_demo.banking_mig_bronze.file_transaction_csv"
    assert t.silver == "migration_demo.banking_mig_silver.transaction"
    assert t.gold == "migration_demo.banking_mig_gold.fact_transaction"
    assert t.rejects == "migration_demo.banking_mig_ops.fact_transaction_rejects"
    assert t.dim_account == "migration_demo.banking_mig_gold.dim_account"
    assert t.dim_branch == "migration_demo.banking_mig_gold.dim_branch"


def test_resolve_tables_single_ticket_schema():
    t = resolve_tables(
        bronze_schema="banking_mig_t7",
        silver_schema="banking_mig_t7",
        gold_schema="banking_mig_t7",
        ops_schema="banking_mig_t7",
    )
    names = [t.bronze_sqlserver, t.bronze_excel, t.bronze_csv, t.silver, t.gold, t.rejects, t.dim_account, t.dim_branch]
    assert all(n.startswith("migration_demo.banking_mig_t7.") for n in names)
    assert len(set(names)) == len(names)


def test_csv_dates_and_amount_are_typed(spark, bronze_frames):
    out = normalize_source(bronze_frames[2], "csv")
    types = {f.name: f.dataType for f in out.schema.fields}
    assert types["transaction_id"] == T.IntegerType()
    assert types["transaction_date"] == T.TimestampType()
    assert types["amount"] == T.DecimalType(19, 4)
    row = out.filter("transaction_id = 14").first()
    assert row.transaction_date == datetime(2024, 1, 21, 14, 0, 0)
    assert row.amount == Decimal("1500000.0000")
    assert out.filter("reject_reason IS NOT NULL").count() == 0


def test_csv_rejects_non_dd_mm_yyyy_dates(spark):
    df = spark.createDataFrame(
        [
            ("1", "1", "2024-01-21 14:00:00", "1", "Deposit", "1"),
            ("2", "1", "13-21-2024 14:00:00", "1", "Deposit", "1"),
        ],
        CSV_DDL,
    )
    assert _rows(normalize_source(df, "csv"), "transaction_id", "reject_reason") == [
        (1, "INVALID_TRANSACTION_DATE"),
        (2, "INVALID_TRANSACTION_DATE"),
    ]


def test_silver_unions_and_dedupes(spark, bronze_frames):
    silver, rejects = build_silver(*bronze_frames, run_id="r1")
    assert silver.count() == 25
    assert sorted(r.transaction_id for r in silver.collect()) == list(range(1, 26))
    assert silver.groupBy("transaction_id").count().filter("count > 1").count() == 0
    assert _rows(silver.groupBy("source_system").count(), "source_system", "count") == [
        ("csv", 10),
        ("excel", 5),
        ("sqlserver", 10),
    ]
    dups = rejects.filter("reject_reason = 'DUPLICATE_TRANSACTION_ID'")
    assert _rows(dups, "transaction_id", "source_system") == [(6, "excel"), (7, "excel"), (14, "csv"), (15, "csv")]
    assert rejects.count() == 4
    assert {r.run_id for r in rejects.collect()} == {"r1"}


def test_tie_break_keeps_first_source_in_merge_order(spark, bronze_frames):
    silver, _ = build_silver(*bronze_frames)
    six = silver.filter("transaction_id = 6").first()
    assert six.source_system == "sqlserver"
    assert six.transaction_date == datetime(2022, 2, 21, 13, 10)
    assert silver.filter("transaction_id = 14").first().source_system == "excel"


def test_tie_break_within_source_uses_ingest_order(spark):
    sql = spark.createDataFrame(
        [
            (1, 1, datetime(2024, 1, 2), 5, "Deposit", 1, datetime(2026, 1, 2)),
            (1, 1, datetime(2024, 1, 1), 9, "Deposit", 1, datetime(2026, 1, 1)),
        ],
        "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount INT, transaction_type STRING, branch_id INT, _ingested_at TIMESTAMP",
    )
    empty_x = spark.createDataFrame([], EXCEL_DDL)
    empty_c = spark.createDataFrame([], CSV_DDL)
    silver, rejects = build_silver(sql, empty_x, empty_c)
    assert silver.first().amount == Decimal("9.0000")
    assert rejects.count() == 1


def test_bad_rows_are_quarantined(spark, bronze_frames):
    bad = spark.createDataFrame(
        [
            ("", "1", "01-01-2024 10:00:00", "1", "Deposit", "1"),
            ("x9", "1", "01-01-2024 10:00:00", "1", "Deposit", "1"),
            ("101", "1", "not a date", "1", "Deposit", "1"),
            ("102", "1", "01-01-2024 10:00:00", "12abc", "Deposit", "1"),
            ("103", "1.5", "01-01-2024 10:00:00", "1", "Deposit", "1"),
            ("104", "1", "01-01-2024 10:00:00", "1", "Deposit", "b"),
            ("105", "2.0", "01-01-2024 10:00:00", " 7 ", "Deposit", None),
        ],
        CSV_DDL,
    )
    silver, rejects = build_silver(bronze_frames[0], bronze_frames[1], bad)
    got = _rows(rejects.filter("reject_reason <> 'DUPLICATE_TRANSACTION_ID'"), "reject_reason", "transaction_id")
    assert got == [
        ("INVALID_ACCOUNT_ID", 103),
        ("INVALID_AMOUNT", 102),
        ("INVALID_BRANCH_ID", 104),
        ("INVALID_TRANSACTION_DATE", 101),
        ("INVALID_TRANSACTION_ID", None),
        ("MISSING_TRANSACTION_ID", None),
    ]
    raw = rejects.filter("reject_reason = 'INVALID_TRANSACTION_ID'").first().raw_record
    assert '"transaction_id":"x9"' in raw
    ok = silver.filter("transaction_id = 105").first()
    assert (ok.account_id, ok.amount, ok.branch_id) == (2, Decimal("7.0000"), None)
    assert silver.filter("transaction_id BETWEEN 101 AND 104").count() == 0


def test_excel_string_typed_bronze_gives_same_silver(spark, bronze_frames):
    sql_df, excel_df, csv_df = bronze_frames
    as_strings = excel_df.select(
        *[
            F.date_format(c, "yyyy-MM-dd HH:mm:ss").alias(c) if c == "transaction_date" else F.col(c).cast("string")
            for c in excel_df.columns
        ]
    )
    a, _ = build_silver(sql_df, excel_df, csv_df)
    b, _ = build_silver(sql_df, as_strings, csv_df)
    assert _rows(a) == _rows(b)


def test_fk_split_matches_sqlserver_fk_rejects(spark, bronze_frames):
    silver, _ = build_silver(*bronze_frames)
    acc, br = dim_frames(spark)
    loadable, rejects = split_fk(silver, acc, br)
    assert loadable.count() == 22
    assert _rows(rejects, "transaction_id", "reject_reason") == [
        (23, "FK_ACCOUNT_NOT_FOUND"),
        (24, "FK_ACCOUNT_NOT_FOUND"),
        (25, "FK_ACCOUNT_NOT_FOUND"),
    ]


def test_end_to_end_exact_parity_with_legacy_fact_transaction(spark, seeded):
    s = run_silver(spark, seeded, run_id="parity")
    g = run_gold(spark, seeded, run_id="parity")
    assert s == {"silver_rows": 25, "silver_rejects": 4}
    assert g == {"inserted": 22, "updated": 0, "deleted": 0, "gold_rows": 22, "gold_rejects": 3}

    expected = read_parity_csv(spark, str(PARITY_CSV))
    actual = spark.table(seeded.gold)
    assert expected.count() == 22
    assert parity_diff(actual, expected) == (0, 0)
    assert _rows(actual) == _rows(expected)
    assert {f.name: f.dataType for f in actual.schema.fields} == {f.name: f.dataType for f in FACT_SCHEMA.fields}
    totals = actual.agg(F.sum("amount").alias("s")).first().s
    assert totals == expected.agg(F.sum("amount").alias("s")).first().s


def test_rerun_is_idempotent_and_rejects_are_replaced(spark, seeded):
    run_silver(spark, seeded, run_id="a")
    run_gold(spark, seeded, run_id="a")
    run_silver(spark, seeded, run_id="b")
    g = run_gold(spark, seeded, run_id="b")
    assert (g["inserted"], g["updated"], g["deleted"], g["gold_rows"]) == (0, 0, 0, 22)
    rej = spark.table(seeded.rejects)
    assert rej.count() == 7
    assert {r.run_id for r in rej.collect()} == {"b"}


def test_silver_rerun_keeps_gold_rejects(spark, seeded):
    run_silver(spark, seeded, run_id="a")
    run_gold(spark, seeded, run_id="a")
    run_silver(spark, seeded, run_id="b")
    stages = _rows(spark.table(seeded.rejects).groupBy("stage", "run_id").count(), "stage", "run_id", "count")
    assert stages == [("gold", "a", 3), ("silver", "b", 4)]


def test_merge_updates_inserts_and_deletes(spark, seeded):
    run_silver(spark, seeded)
    run_gold(spark, seeded)
    changed = (
        spark.table(seeded.gold)
        .filter("transaction_id <> 1")
        .withColumn("amount", F.when(F.col("transaction_id") == 2, F.lit(Decimal("1.2345"))).otherwise(F.col("amount")))
        .unionByName(spark.createDataFrame([(99, 1, datetime(2025, 1, 1), Decimal(10), "Deposit", 1)], FACT_SCHEMA))
    )
    changed = spark.createDataFrame(changed.collect(), FACT_SCHEMA)
    m = merge_into_gold(spark, changed, seeded.gold)
    assert m == {"inserted": 1, "updated": 1, "deleted": 1}
    gold = spark.table(seeded.gold)
    assert gold.filter("transaction_id = 1").count() == 0
    assert gold.filter("transaction_id = 2").first().amount == Decimal("1.2345")

    keep = merge_into_gold(spark, changed.filter("transaction_id <> 99"), seeded.gold, delete_missing=False)
    assert keep == {"inserted": 0, "updated": 0, "deleted": 0}
    assert gold.filter("transaction_id = 99").count() == 1


def test_merge_into_existing_gold_with_identity_and_audit_columns(spark, schema, seeded):
    from delta.tables import DeltaTable, IdentityGenerator

    (
        DeltaTable.create(spark)
        .tableName(seeded.gold)
        .addColumn("fact_transaction_sk", "BIGINT", generatedByDefaultAs=IdentityGenerator())
        .addColumn("transaction_id", "INT", nullable=False)
        .addColumn("account_id", "INT")
        .addColumn("transaction_date", "TIMESTAMP")
        .addColumn("amount", "DECIMAL(19,4)")
        .addColumn("transaction_type", "STRING")
        .addColumn("branch_id", "INT")
        .addColumn("_loaded_at", "TIMESTAMP")
        .execute()
    )
    run_silver(spark, seeded)
    g = run_gold(spark, seeded)
    assert (g["inserted"], g["gold_rows"]) == (22, 22)
    gold = spark.table(seeded.gold)
    assert gold.filter("fact_transaction_sk IS NULL").count() == 0
    assert gold.select("fact_transaction_sk").distinct().count() == 22
    assert parity_diff(gold, read_parity_csv(spark, str(PARITY_CSV))) == (0, 0)


def test_run_gold_requires_dims(spark, schema, bronze_frames):
    tables = resolve_tables(
        catalog=None, bronze_schema=schema, silver_schema=schema, gold_schema=schema, ops_schema=schema
    )
    for df, name in zip(bronze_frames, (tables.bronze_sqlserver, tables.bronze_excel, tables.bronze_csv)):
        df.write.format("delta").saveAsTable(name)
    run_silver(spark, tables)
    with pytest.raises(RuntimeError, match="dim_account"):
        run_gold(spark, tables)


def test_unknown_source_and_missing_columns(spark):
    df = spark.createDataFrame([("1",)], "transaction_id STRING")
    with pytest.raises(ValueError, match="unknown source_system"):
        normalize_source(df, "mainframe")
    with pytest.raises(ValueError, match="missing columns"):
        normalize_source(df, "csv")


def test_auto_loader_rescued_rows_are_quarantined(spark, bronze_frames):
    typed_csv = spark.createDataFrame(
        [
            (30, 1, datetime(2024, 2, 1, 9), Decimal(10), "Deposit", 1, None, datetime(2026, 1, 1)),
            (
                31,
                1,
                None,
                Decimal(10),
                "Deposit",
                1,
                '{"transaction_date":"31-02-2024 25:00:00"}',
                datetime(2026, 1, 1),
            ),
        ],
        "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount DECIMAL(19,4), transaction_type STRING, "
        "branch_id INT, _rescued_data STRING, _ingested_at TIMESTAMP",
    )
    silver, rejects = build_silver(bronze_frames[0], bronze_frames[1], typed_csv)
    assert silver.filter("transaction_id = 30").count() == 1
    assert silver.filter("transaction_id = 31").count() == 0
    rescued = rejects.filter("reject_reason = 'RESCUED_DATA'").first()
    assert rescued.transaction_id == 31 and "31-02-2024" in rescued.raw_record


def test_ids_with_hidden_fractions_are_rejected(spark):
    df = spark.createDataFrame(
        [
            ("6.0000001", "1", "01-01-2024 10:00:00", "1", "Deposit", "1"),
            ("7.0", "1.00", "01-01-2024 10:00:00", "1", "Deposit", "1.0000000001"),
            ("8", "1", "01-01-2024 10:00:00", "1", "Deposit", "1"),
        ],
        CSV_DDL,
    )
    out = normalize_source(df, "csv").fillna("OK", subset=["reject_reason"])
    assert _rows(out, "reject_reason", "transaction_id") == [
        ("INVALID_BRANCH_ID", 7),
        ("INVALID_TRANSACTION_ID", None),
        ("OK", 8),
    ]
    typed = spark.createDataFrame(
        [(Decimal("9.0000001"), 1.0), (Decimal("10.0000000"), 1.5)], "transaction_id DECIMAL(20,7), account_id DOUBLE"
    )
    rows = typed.select(_to_int(typed, "transaction_id").alias("t"), _to_int(typed, "account_id").alias("a")).collect()
    assert [(r.t, r.a) for r in rows] == [(None, 1), (10, None)]


def test_tie_break_orders_row_numbers_numerically(spark):
    sql = spark.createDataFrame(
        [
            (42, 1, datetime(2024, 1, 1), 2, "Deposit", 1, 2),
            (42, 1, datetime(2024, 1, 1), 10, "Deposit", 1, 10),
        ],
        "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount INT, transaction_type STRING, "
        "branch_id INT, _source_row_number INT",
    )
    silver, rejects = build_silver(sql, spark.createDataFrame([], EXCEL_DDL), spark.createDataFrame([], CSV_DDL))
    assert silver.first().amount == Decimal("2.0000")
    assert rejects.first().reject_reason == "DUPLICATE_TRANSACTION_ID"


def test_parity_diff_does_not_round_away_extra_precision(spark):
    row = (1, 1, datetime(2024, 1, 1), "Deposit", 1)
    cols = "transaction_id INT, account_id INT, transaction_date TIMESTAMP, transaction_type STRING, branch_id INT"
    actual = spark.createDataFrame([(*row, Decimal("1.23456"))], cols + ", amount DECIMAL(20,5)")
    expected = spark.createDataFrame([(*row, Decimal("1.2346"))], cols + ", amount DECIMAL(19,4)")
    assert parity_diff(actual, expected) == (1, 1)
    same = spark.createDataFrame([(*row, Decimal("1.23460"))], cols + ", amount DECIMAL(20,5)")
    assert parity_diff(same, expected) == (0, 0)
