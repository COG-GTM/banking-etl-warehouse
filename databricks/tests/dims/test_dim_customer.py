import csv
import itertools

import pytest
from conftest import ROOT
from pyspark.sql import functions as F

from banking_etl.bronze.fixtures import stage_fixtures
from banking_etl.bronze.sqlserver import ingest_sqlserver
from banking_etl.config import Settings
from banking_etl.dims.customer import (
    GOLD_MAPPING,
    SILVER_COLUMNS,
    gold_table,
    join_customer,
    load_dim_customer,
    load_silver_customer,
    parse_age,
    rejects_table,
    silver_table,
    to_dim_customer,
    transform_customer,
)
from banking_etl.gold.merge import identity_columns

EXPECTED_CSV = ROOT / "tests" / "dims" / "expected_dim_customer_sqlserver.csv"
GOLD_COLUMNS = [
    "CustomerID",
    "CustomerName",
    "Address",
    "CityName",
    "StateName",
    "Age",
    "Gender",
    "Email",
]

CUSTOMER_DDL = "customer_id INT, customer_name STRING, address STRING, city_id INT, age STRING, gender STRING, email STRING"
CITY_DDL = "city_id INT, city_name STRING, state_id INT"
STATE_DDL = "state_id INT, state_name STRING"

_prefix = itertools.count()


def frames(
    spark, customers, cities=((1, "Kelapa Gading", 1),), states=((1, "Jakarta Utara"),)
):
    return (
        spark.createDataFrame(list(customers), CUSTOMER_DDL),
        spark.createDataFrame(list(cities), CITY_DDL),
        spark.createDataFrame(list(states), STATE_DDL),
    )


def by_id(df):
    return {r["customer_id"]: r.asDict() for r in df.collect()}


def expected_rows():
    with EXPECTED_CSV.open(newline="", encoding="utf-8") as fh:
        return [
            {**r, "CustomerID": int(r["CustomerID"]), "Age": int(r["Age"])}
            for r in csv.DictReader(fh)
        ]


# --------------------------------------------------------------------------- tMap logic


def test_join_and_uppercase_cleansing(spark):
    c, ci, st = frames(
        spark,
        [
            (
                1,
                "Shelly Juwita",
                "Jl. Boulevard No. 31",
                1,
                "25",
                "female",
                "Shelly@Gmail.com",
            )
        ],
    )
    row = by_id(join_customer(c, ci, st))[1]
    assert row["customer_name"] == "SHELLY JUWITA"
    assert row["address"] == "JL. BOULEVARD NO. 31"
    assert row["gender"] == "FEMALE"
    # Not uppercased by the tMap.
    assert row["email"] == "Shelly@Gmail.com"
    assert (row["city_name"], row["state_name"]) == ("Kelapa Gading", "Jakarta Utara")
    assert (row["city_id"], row["state_id"], row["age"]) == (1, 1, 25)


def test_no_trim_and_null_passthrough(spark):
    c, ci, st = frames(spark, [(1, "  ani ", None, 1, None, None, None)])
    row = by_id(join_customer(c, ci, st))[1]
    assert row["customer_name"] == "  ANI "  # TRIM_COLUMN=false on every input column
    assert row["address"] is None and row["gender"] is None and row["email"] is None
    assert row["age"] is None and row["age_raw"] is None


def test_unmatched_city_is_left_outer(spark):
    c, ci, st = frames(
        spark,
        [
            (1, "a", "x", 99, "30", "male", "a@x"),
            (2, "b", "y", None, "31", "male", "b@x"),
        ],
    )
    silver, rejects = transform_customer(c, ci, st)
    rows = by_id(silver)
    assert set(rows) == {1, 2} and rejects.count() == 0
    for r in rows.values():
        assert (
            r["city_name"] is None and r["state_id"] is None and r["state_name"] is None
        )
    assert rows[1]["city_id"] == 99


def test_unmatched_state_keeps_city(spark):
    c, ci, st = frames(
        spark, [(1, "a", "x", 7, "30", "male", "a@x")], cities=[(7, "Cilandak", 42)]
    )
    row = by_id(transform_customer(c, ci, st)[0])[1]
    assert (row["city_name"], row["state_id"], row["state_name"]) == (
        "Cilandak",
        42,
        None,
    )


def test_duplicate_lookup_keys_do_not_multiply_customers(spark):
    c, ci, st = frames(
        spark,
        [(1, "a", "x", 1, "30", "male", "a@x")],
        cities=[(1, "A-city", 1), (1, "B-city", 1)],
        states=[(1, "S1"), (1, "S2")],
    )
    silver, _ = transform_customer(c, ci, st)
    assert silver.count() == 1
    row = silver.first()
    assert (row["city_name"], row["state_name"]) == (
        "B-city",
        "S2",
    )  # deterministic tie-break


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("25", 25),
        (" 25", 25),
        ("7 ", 7),
        ("+7", 7),
        ("", 0),
        ("   ", 0),
        (None, None),
        ("abc", None),
        ("2.5", None),
        ("1 2", None),
        ("007", 7),
        ("-12", -12),
        ("2147483647", 2147483647),
        ("2147483648", None),
    ],
)
def test_parse_age_matches_sql_server_int_conversion(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "age STRING")
    assert df.select(parse_age(F.col("age")).alias("a")).first()["a"] == expected


def test_rejects_for_rows_sql_server_would_refuse(spark):
    long_address = "x" * 256
    c, ci, st = frames(
        spark,
        [
            (1, "ok", "x", 1, "30", "male", "a@x"),
            (None, "no id", "x", 1, "30", "male", "a@x"),
            (2, "dup a", "x", 1, "30", "male", "a@x"),
            (2, "dup b", "x", 1, "31", "male", "a@x"),
            (3, "bad age", "x", 1, "abc", "male", "a@x"),
            (4, "old", "x", 1, "999", "male", "a@x"),
            (5, "long", long_address, 1, "40", "male", "a@x"),
            (6, "blank age", "x", 1, "", "male", "a@x"),
        ],
    )
    silver, rejects = transform_customer(c, ci, st)
    assert sorted(r["customer_id"] for r in silver.collect()) == [1, 6]
    assert by_id(silver)[6]["age"] == 0
    reasons = sorted(
        (r["customer_id"] or -1, r["reject_reason"]) for r in rejects.collect()
    )
    assert reasons == [
        (-1, "null_customer_id"),
        (2, "duplicate_customer_id"),
        (2, "duplicate_customer_id"),
        (3, "age_not_int"),
        (4, "age_out_of_range"),
        (5, "too_long_address"),
    ]
    assert rejects.where("customer_id = 3").first()["age_raw"] == "abc"
    assert list(silver.columns) == list(SILVER_COLUMNS)


def test_to_dim_customer_column_mapping(spark):
    c, ci, st = frames(spark, [(1, "a", "x", 1, "30", "male", "a@x")])
    gold = to_dim_customer(transform_customer(c, ci, st)[0])
    assert gold.columns == list(GOLD_MAPPING) == GOLD_COLUMNS
    assert gold.first().asDict() == {
        "CustomerID": 1,
        "CustomerName": "A",
        "Address": "X",
        "CityName": "Kelapa Gading",
        "StateName": "Jakarta Utara",
        "Age": 30,
        "Gender": "MALE",
        "Email": "a@x",
    }


# --------------------------------------------------------------------------- end to end (fixtures -> gold)


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    """Isolated bronze/silver/gold/ops databases (schema_prefix) loaded from the sample_db fixtures."""
    s = Settings(catalog=None, schema_prefix=f"t7_{next(_prefix)}_")
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {s.schema(layer)}")
    root = tmp_path_factory.mktemp("t7_landing") / "sample_db"
    stage_fixtures(root, ["customer", "city", "state"])
    ingest_sqlserver(spark, s, ["customer", "city", "state"], fixture_root=str(root))
    yield s
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"DROP DATABASE IF EXISTS {s.schema(layer)} CASCADE")


def test_silver_from_fixtures(spark, pipeline):
    result = load_silver_customer(spark, pipeline)
    assert (
        result["rows"],
        result["rejects"],
        result["unmatched_city"],
        result["unmatched_state"],
    ) == (20, 0, 0, 0)
    silver = spark.table(silver_table(pipeline))
    assert silver.columns == [*SILVER_COLUMNS, "_loaded_at"]
    assert dict(silver.dtypes)["age"] == "int"
    rejects = spark.table(rejects_table(pipeline))
    assert {"reject_reason", "rejected_at", "age_raw"} <= set(rejects.columns)


def test_gold_parity_idempotent_and_scd1(spark, pipeline):
    load_silver_customer(spark, pipeline)
    first = load_dim_customer(spark, pipeline)
    assert first["created"] is True
    assert (
        first["numTargetRowsInserted"],
        first["numTargetRowsUpdated"],
        first["rows"],
    ) == (20, 0, 20)

    target = gold_table(pipeline)
    assert identity_columns(spark, target) == ["CustomerKey"]
    gold = spark.table(target)
    # Parity with DWH.dbo.DimCustomer as loaded on SQL Server 2022 from sample.bak.
    actual = [
        r.asDict() for r in gold.select(*GOLD_COLUMNS).orderBy("CustomerID").collect()
    ]
    assert actual == expected_rows()
    keys = {r["CustomerID"]: r["CustomerKey"] for r in gold.collect()}
    assert None not in keys.values() and len(set(keys.values())) == 20

    # Re-run with the same source: no-op, keys unchanged.
    again = load_dim_customer(spark, pipeline)
    assert (
        again["created"],
        again["numTargetRowsInserted"],
        again["numTargetRowsUpdated"],
        again["rows"],
    ) == (False, 0, 0, 20)

    # SCD-1: change one customer's source row in bronze, add a new one; the key of the changed row is stable.
    bronze = pipeline.table("bronze", "sqlserver_customer")
    changed = spark.table(bronze).withColumn(
        "address",
        F.when(F.col("customer_id") == 3, F.lit("Jl. Baru No. 1")).otherwise(
            F.col("address")
        ),
    )
    extra = (
        spark.createDataFrame(
            [(21, "New Person", "Jl. Satu", 999, "33", "male", "new@x.com")],
            CUSTOMER_DDL,
        )
        .select(*[F.col(c) for c in changed.columns if not c.startswith("_")])
        .withColumns({"_ingested_at": F.current_timestamp(), "_source": F.lit("test")})
    )
    changed.unionByName(extra).localCheckpoint().write.format("delta").mode(
        "overwrite"
    ).saveAsTable(bronze)

    load_silver_customer(spark, pipeline)
    third = load_dim_customer(spark, pipeline)
    assert (
        third["numTargetRowsInserted"],
        third["numTargetRowsUpdated"],
        third["numTargetRowsDeleted"],
        third["rows"],
    ) == (1, 1, 0, 21)
    after = {r["CustomerID"]: r.asDict() for r in spark.table(target).collect()}
    assert after[3]["Address"] == "JL. BARU NO. 1"
    for cid, key in keys.items():
        assert after[cid]["CustomerKey"] == key
    assert after[21]["CustomerKey"] not in keys.values()
    assert (after[21]["CityName"], after[21]["StateName"]) == (None, None)
