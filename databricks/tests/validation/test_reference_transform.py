from pyspark.sql import functions as F

from banking_etl.validation import reference_transform, specs


def test_gold_row_counts_match_legacy(built, legacy):
    gold, rejects = built
    for spec in specs.GOLD_TABLES:
        assert gold[spec.gold].count() == legacy[spec.gold].count(), spec.gold
    assert rejects.count() == 3


def test_gold_schema_matches_spec(built):
    gold, _ = built
    for spec in specs.GOLD_TABLES:
        assert gold[spec.gold].columns == list(spec.gold_columns)
        assert [f.dataType for f in gold[spec.gold].schema.fields] == [c.dtype for c in spec.columns]


def test_rejects_are_fk_violations_from_csv(built):
    _, rejects = built
    rows = sorted((r.transaction_id, r.account_id, r.source, r.reason) for r in rejects.collect())
    assert rows == [(23, 22, "csv", "FK_VIOLATION"), (24, 23, "csv", "FK_VIOLATION"), (25, 23, "csv", "FK_VIOLATION")]


def test_customer_strings_uppercased(built):
    cust = built[0]["dim_customer"]
    assert cust.filter(F.col("customer_name") != F.upper("customer_name")).count() == 0
    assert cust.filter(F.col("city_name").isNull() | F.col("state_name").isNull()).count() == 0


def test_dedup_prefers_sqlserver_then_excel_then_csv(spark):
    cols = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id"]
    src = {
        "sqlserver_transaction_db": spark.createDataFrame(
            [(1, 1, "2024-01-01 00:00:00", 1.0, "Deposit", 1)], cols
        ).withColumn("transaction_date", F.to_timestamp("transaction_date")),
        "file_transaction_excel": spark.createDataFrame(
            [(1, 2, "2024-01-01 00:00:00", 2.0, "Deposit", 1), (2, 2, "2024-01-01 00:00:00", 2.0, "Deposit", 1)], cols
        ).withColumn("transaction_date", F.to_timestamp("transaction_date")),
        "file_transaction_csv": spark.createDataFrame(
            [(2, 3, "05-01-2024 10:11:12", 3.0, "Deposit", 1), (3, 3, "05-01-2024 10:11:12", 3.0, "Deposit", 1)], cols
        ),
    }
    out = {r.transaction_id: (r.account_id, r.source, str(r.transaction_date))
           for r in reference_transform.transaction_candidates(src).collect()}
    assert out == {1: (1, "sqlserver", "2024-01-01 00:00:00"),
                   2: (2, "excel", "2024-01-01 00:00:00"),
                   3: (3, "csv", "2024-01-05 10:11:12")}
