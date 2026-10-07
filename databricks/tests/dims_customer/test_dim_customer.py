import csv
import uuid

import pytest
from pyspark.sql import functions as F

from banking_etl.dims import customer as dim_customer

from .conftest import FIXTURES, PARITY_CSV


def _load_fixture(spark, name):
    with open(FIXTURES / f"{name}.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        rows = [tuple(None if r[c] == "" else r[c] for c in cols) for r in reader]
    df = spark.createDataFrame(rows, ", ".join(f"{c} STRING" for c in cols))
    for c in cols:
        if c.endswith("_id"):
            df = df.withColumn(c, F.col(c).cast("int"))
    return df


@pytest.fixture()
def tables(spark):
    schema = f"t6_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {schema}")
    t = dim_customer.Tables.build(catalog=None, schema_override=schema)
    for name, target in [
        ("sqlserver_customer", t.bronze_customer),
        ("sqlserver_city", t.bronze_city),
        ("sqlserver_state", t.bronze_state),
    ]:
        _load_fixture(spark, name).write.format("delta").saveAsTable(target)
    yield t
    spark.sql(f"DROP DATABASE {schema} CASCADE")


def _gold_rows(spark, t):
    return {r.customer_id: r.asDict() for r in spark.table(t.gold_dim_customer).collect()}


def test_table_names_follow_shared_convention():
    t = dim_customer.Tables.build(catalog="migration_demo", schema_prefix="banking_mig_")
    assert t.bronze_customer == "migration_demo.banking_mig_bronze.sqlserver_customer"
    assert t.bronze_city == "migration_demo.banking_mig_bronze.sqlserver_city"
    assert t.bronze_state == "migration_demo.banking_mig_bronze.sqlserver_state"
    assert t.silver_customer == "migration_demo.banking_mig_silver.customer"
    assert t.gold_dim_customer == "migration_demo.banking_mig_gold.dim_customer"
    o = dim_customer.Tables.build(catalog="migration_demo", schema_override="banking_mig_t6")
    assert {o.bronze_customer.rsplit(".", 1)[0], o.gold_dim_customer.rsplit(".", 1)[0]} == {
        "migration_demo.banking_mig_t6"
    }


def test_parity_with_legacy_sqlserver_dimcustomer(spark, tables):
    counts = dim_customer.run(spark, tables)
    assert counts == {"bronze_customer": 20, "silver_customer": 20, "gold_dim_customer": 20}
    expected = dim_customer.read_parity_csv(spark, str(PARITY_CSV))
    diff = dim_customer.assert_parity(spark.table(tables.gold_dim_customer), expected)
    assert diff["expected_rows"] == diff["actual_rows"] == 20
    assert diff["missing_rows"] == diff["unexpected_rows"] == 0
    # Row-for-row equality with the CSV, typed (not just set-equality on strings).
    actual = sorted(tuple(r) for r in spark.table(tables.gold_dim_customer).select(*dim_customer.GOLD_COLUMNS).collect())
    assert actual == sorted(tuple(r) for r in expected.collect())


def test_gold_schema_matches_legacy_columns(spark, tables):
    dim_customer.run(spark, tables)
    fields = [(f.name, f.dataType.simpleString()) for f in spark.table(tables.gold_dim_customer).schema]
    assert fields == [
        ("customer_id", "int"),
        ("customer_name", "string"),
        ("address", "string"),
        ("city_name", "string"),
        ("state_name", "string"),
        ("age", "int"),
        ("gender", "string"),
        ("email", "string"),
    ]
    silver_cols = spark.table(tables.silver_customer).columns
    assert silver_cols == dim_customer.SILVER_COLUMNS


def test_uppercase_cleansing_matches_tmap_expressions(spark, tables):
    dim_customer.run(spark, tables)
    row = _gold_rows(spark, tables)[1]
    # UPCASE: customer_name, address, gender. Pass-through: email, city_name, state_name.
    assert row == {
        "customer_id": 1,
        "customer_name": "SHELLY JUWITA",
        "address": "JL. BOULEVARD NO. 31",
        "city_name": "Kelapa Gading",
        "state_name": "Jakarta Utara",
        "age": 25,
        "gender": "FEMALE",
        "email": "shelly@gmail.com",
    }


def test_lookups_are_left_outer_joins(spark):
    customer = spark.createDataFrame(
        [(1, "a", "x", 1, "20", "male", "a@x"), (2, "b", "y", 99, "30", "female", "b@x"), (3, "c", None, None, None, None, None)],
        "customer_id INT, customer_name STRING, address STRING, city_id INT, age STRING, gender STRING, email STRING",
    )
    city = spark.createDataFrame([(1, "Koja", 7)], "city_id INT, city_name STRING, state_id INT")
    state = spark.createDataFrame([(1, "Jakarta Utara")], "state_id INT, state_name STRING")
    out = {r.customer_id: r for r in dim_customer.build_silver_customer(customer, city, state).collect()}
    assert set(out) == {1, 2, 3}
    assert (out[1].city_name, out[1].state_id, out[1].state_name) == ("Koja", 7, None)
    assert (out[2].city_name, out[2].state_name) == (None, None)
    assert (out[3].address, out[3].gender, out[3].age, out[3].city_id) == (None, None, None, None)


def test_age_varchar_is_converted_to_int(spark):
    customer = spark.createDataFrame(
        [(1, "a", "x", 1, " 42 ", "m", "e"), (2, "b", "y", 1, "n/a", "f", "e")],
        "customer_id INT, customer_name STRING, address STRING, city_id INT, age STRING, gender STRING, email STRING",
    )
    city = spark.createDataFrame([(1, "c", 1)], "city_id INT, city_name STRING, state_id INT")
    state = spark.createDataFrame([(1, "s")], "state_id INT, state_name STRING")
    out = {r.customer_id: r.age for r in dim_customer.build_silver_customer(customer, city, state).collect()}
    assert out == {1: 42, 2: None}


def test_duplicate_source_keys_are_collapsed(spark):
    customer = spark.createDataFrame(
        [
            (1, "old", "x", 1, "20", "m", "e", "2024-01-01 00:00:00"),
            (1, "new", "x", 1, "21", "m", "e", "2024-02-01 00:00:00"),
        ],
        "customer_id INT, customer_name STRING, address STRING, city_id INT, age STRING, gender STRING, email STRING, _ingested_at STRING",
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    city = spark.createDataFrame([(1, "c", 1), (1, "c", 1)], "city_id INT, city_name STRING, state_id INT")
    state = spark.createDataFrame([(1, "s")], "state_id INT, state_name STRING")
    rows = dim_customer.build_silver_customer(customer, city, state).collect()
    assert len(rows) == 1
    assert (rows[0].customer_name, rows[0].age) == ("NEW", 21)


def test_extra_bronze_columns_are_ignored(spark, tables):
    spark.table(tables.bronze_customer).withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.lit("sample.bak")
    ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables.bronze_customer)
    dim_customer.run(spark, tables)
    expected = dim_customer.read_parity_csv(spark, str(PARITY_CSV))
    dim_customer.assert_parity(spark.table(tables.gold_dim_customer), expected)


def test_rerun_is_idempotent_and_skips_unchanged_rows(spark, tables):
    dim_customer.run(spark, tables)
    before = spark.sql(f"DESCRIBE HISTORY {tables.gold_dim_customer}").count()
    first = _gold_rows(spark, tables)
    counts = dim_customer.run(spark, tables)
    assert counts["gold_dim_customer"] == 20
    assert _gold_rows(spark, tables) == first
    last = spark.sql(f"DESCRIBE HISTORY {tables.gold_dim_customer}").orderBy(F.col("version").desc()).first()
    assert spark.sql(f"DESCRIBE HISTORY {tables.gold_dim_customer}").count() == before + 1
    metrics = last.operationMetrics
    assert int(metrics.get("numTargetRowsUpdated", "0")) == 0
    assert int(metrics.get("numTargetRowsInserted", "0")) == 0


def test_scd1_merge_updates_changed_and_inserts_new(spark, tables):
    dim_customer.run(spark, tables)
    src = spark.table(tables.bronze_customer)
    changed = src.withColumn(
        "customer_name", F.when(F.col("customer_id") == 2, F.lit("Bobi Updated")).otherwise(F.col("customer_name"))
    ).withColumn("city_id", F.when(F.col("customer_id") == 2, F.lit(48)).otherwise(F.col("city_id")))
    new = spark.createDataFrame(
        [(21, "New Person", "Jl. Baru No. 1", 1, "33", "female", "new@gmail.com")], src.schema
    )
    changed.unionByName(new).write.format("delta").mode("overwrite").saveAsTable(tables.bronze_customer)

    counts = dim_customer.run(spark, tables)
    assert counts["gold_dim_customer"] == 21
    rows = _gold_rows(spark, tables)
    assert (rows[2]["customer_name"], rows[2]["city_name"], rows[2]["state_name"]) == ("BOBI UPDATED", "Babelan", "Bekasi")
    assert rows[21] == {
        "customer_id": 21,
        "customer_name": "NEW PERSON",
        "address": "JL. BARU NO. 1",
        "city_name": "Cilincing",
        "state_name": "Jakarta Utara",
        "age": 33,
        "gender": "FEMALE",
        "email": "new@gmail.com",
    }
    last = spark.sql(f"DESCRIBE HISTORY {tables.gold_dim_customer}").orderBy(F.col("version").desc()).first()
    assert int(last.operationMetrics["numTargetRowsUpdated"]) == 1
    assert int(last.operationMetrics["numTargetRowsInserted"]) == 1


def test_parity_check_detects_drift(spark, tables):
    dim_customer.run(spark, tables)
    expected = dim_customer.read_parity_csv(spark, str(PARITY_CSV))
    drifted = spark.table(tables.gold_dim_customer).withColumn(
        "email", F.when(F.col("customer_id") == 5, F.upper("email")).otherwise(F.col("email"))
    )
    with pytest.raises(AssertionError, match="parity failed"):
        dim_customer.assert_parity(drifted, expected)
    diff = dim_customer.parity_diff(drifted, expected)
    assert diff["missing_rows"] == diff["unexpected_rows"] == 1


def test_latest_ingested_row_wins_over_lexical_order(spark):
    customer = spark.createDataFrame(
        [
            (1, "Zed Old", "x", 1, "20", "m", "e", "2024-01-01 00:00:00"),
            (1, "Alice New", "x", 1, "20", "m", "e", "2024-02-01 00:00:00"),
        ],
        "customer_id INT, customer_name STRING, address STRING, city_id INT, age STRING, gender STRING, email STRING, _ingested_at STRING",
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    city = spark.createDataFrame(
        [(1, "Zzz Stale", 1, "2024-01-01 00:00:00"), (1, "Aaa Fresh", 1, "2024-03-01 00:00:00")],
        "city_id INT, city_name STRING, state_id INT, _ingested_at STRING",
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    state = spark.createDataFrame(
        [(1, "Zzz Stale", "2024-01-01 00:00:00"), (1, "Aaa Fresh", "2024-03-01 00:00:00")],
        "state_id INT, state_name STRING, _ingested_at STRING",
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    out = dim_customer.build_silver_customer(customer, city, state)
    assert out.columns == dim_customer.SILVER_COLUMNS
    rows = out.collect()
    assert len(rows) == 1
    assert (rows[0].customer_name, rows[0].city_name, rows[0].state_name) == ("ALICE NEW", "Aaa Fresh", "Aaa Fresh")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "migration_demo; DROP SCHEMA x"},
        {"catalog": "migration_demo", "schema_override": "t6 CASCADE"},
        {"catalog": "migration_demo", "schema_prefix": "banking-mig."},
    ],
)
def test_table_builder_rejects_unsafe_identifiers(kwargs):
    with pytest.raises(ValueError, match="invalid"):
        dim_customer.Tables.build(**kwargs)
