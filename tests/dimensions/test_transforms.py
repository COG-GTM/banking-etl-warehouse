import datetime as dt
from decimal import Decimal

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from databricks.jobs.dimensions.transforms import (
    DIM_ACCOUNT_SCHEMA,
    DIM_BRANCH_SCHEMA,
    DIM_CUSTOMER_SCHEMA,
    build_dim_account,
    build_dim_branch,
    build_dim_customer,
)


def _rows(df, key):
    return {r[key]: r.asDict() for r in df.collect()}


def test_dim_branch_schema_and_values(spark):
    src = spark.createDataFrame(
        [(1, "Main", "Jakarta"), (2, None, "Bandung")],
        "branch_id long, branch_name string, branch_location string",
    )
    out = build_dim_branch(src)
    assert out.schema == DIM_BRANCH_SCHEMA
    assert out.schema == StructType(
        [
            StructField("BranchID", IntegerType(), False),
            StructField("BranchName", StringType(), True),
            StructField("BranchLocation", StringType(), True),
        ]
    )
    rows = _rows(out, "BranchID")
    assert rows[1]["BranchName"] == "Main"
    assert rows[2]["BranchName"] is None


def test_dim_account_casts_types(spark):
    src = spark.createDataFrame(
        [
            ("10", "5", "Savings", "1500", "2020-01-15", "active"),
            ("11", None, "Checking", None, None, "closed"),
        ],
        "account_id string, customer_id string, account_type string, "
        "balance string, date_opened string, status string",
    )
    out = build_dim_account(src)
    assert out.schema == DIM_ACCOUNT_SCHEMA
    assert out.schema["Balance"].dataType == DecimalType(19, 4)
    assert out.schema["DateOpened"].dataType == DateType()
    rows = _rows(out, "AccountID")
    assert rows[10]["CustomerID"] == 5
    assert rows[10]["Balance"] == Decimal("1500.0000")
    assert rows[10]["DateOpened"] == dt.date(2020, 1, 15)
    assert rows[11]["CustomerID"] is None
    assert rows[11]["Balance"] is None
    assert rows[11]["DateOpened"] is None


def _customer_inputs(spark):
    customer = spark.createDataFrame(
        [
            (1, "alice smith", "1 main st", 100, "30", "f", "Alice@Ex.com"),
            (2, "bob", "2 side rd", 101, None, "m", None),
            (3, "carol", "3 no city", 999, "41", None, "c@x.com"),
            (4, "dave", "4 null city", None, "22", "m", "d@x.com"),
        ],
        "customer_id int, customer_name string, address string, city_id int, "
        "age string, gender string, email string",
    )
    city = spark.createDataFrame(
        [(100, "jakarta", 10), (101, "bandung", 20), (102, "medan", 30)],
        "city_id int, city_name string, state_id int",
    )
    state = spark.createDataFrame(
        [(10, "dki jakarta"), (30, "sumatera utara")],
        "state_id int, state_name string",
    )
    return customer, city, state


def test_dim_customer_joins_and_uppercase(spark):
    out = build_dim_customer(*_customer_inputs(spark))
    assert out.schema == DIM_CUSTOMER_SCHEMA
    rows = _rows(out, "CustomerID")
    assert set(rows) == {1, 2, 3, 4}

    # full join path: customer -> city -> state
    assert rows[1]["CityName"] == "jakarta"
    assert rows[1]["StateName"] == "dki jakarta"
    # UPCASE applied to name/address/gender only; email passed through
    assert rows[1]["CustomerName"] == "ALICE SMITH"
    assert rows[1]["Address"] == "1 MAIN ST"
    assert rows[1]["Gender"] == "F"
    assert rows[1]["Email"] == "Alice@Ex.com"
    assert rows[1]["Age"] == 30

    # city exists but state missing -> StateName null
    assert rows[2]["CityName"] == "bandung"
    assert rows[2]["StateName"] is None
    assert rows[2]["Age"] is None
    assert rows[2]["Email"] is None

    # city id not present in city table
    assert rows[3]["CityName"] is None
    assert rows[3]["StateName"] is None
    assert rows[3]["Gender"] is None

    # null city id
    assert rows[4]["CityName"] is None
    assert rows[4]["StateName"] is None


def test_dim_customer_unique_match_on_duplicate_lookup(spark):
    customer, city, state = _customer_inputs(spark)
    dup_city = city.union(
        spark.createDataFrame([(100, "jakarta-dup", 10)], city.schema)
    )
    out = build_dim_customer(customer, dup_city, state)
    assert out.count() == customer.count()


def test_dim_customer_drops_null_key(spark):
    customer, city, state = _customer_inputs(spark)
    with_null = customer.union(
        spark.createDataFrame(
            [(None, "x", "y", 100, "1", "f", "e")], customer.schema
        )
    )
    out = build_dim_customer(with_null, city, state)
    assert out.count() == customer.count()
