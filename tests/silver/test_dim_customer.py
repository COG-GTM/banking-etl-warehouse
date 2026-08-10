"""Unit tests for the silver ``dim_customer`` build (Talend ``Load_DimCustomer`` parity).

Run with a local SparkSession:  pip install pyspark delta-spark pytest && pytest tests/silver
The Delta-backed merge test is skipped automatically when the Delta jars are unavailable.
"""

import importlib.util
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = REPO_ROOT / "databricks" / "silver" / "build_dim_customer.py"


def _load_notebook_module():
    spec = importlib.util.spec_from_file_location("build_dim_customer", NOTEBOOK)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


dc = _load_notebook_module()


def _base_builder():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dim_customer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )


def _with_delta(builder):
    """Enable Delta, preferring pre-downloaded jars (``DELTA_JARS``) over a Maven fetch."""
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    jars = os.environ.get("DELTA_JARS")
    if jars:
        return builder.config("spark.jars", jars).getOrCreate()

    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def spark():
    try:
        session = _with_delta(_base_builder())
    except Exception:  # pragma: no cover - Delta jars cannot be resolved offline
        SparkSession.builder._options.clear()
        session = _base_builder().getOrCreate()
    yield session
    session.stop()


CUSTOMER_SCHEMA = "customer_id int, customer_name string, address string, city_id int, age string, gender string, email string"
CITY_SCHEMA = "city_id int, city_name string, state_id int"
STATE_SCHEMA = "state_id int, state_name string"


@pytest.fixture
def sources(spark):
    customer = spark.createDataFrame(
        [
            (1, "Alice Smith", "12 Main st", 10, "30", "female", "Alice@Example.COM"),
            (2, "bob jones", "9 Oak Ave", 20, "45", "Male", "BOB@example.com"),
            # city_id 99 has no matching city row -> unmatched lookup
            (3, "carol white", "3 Elm Rd", 99, "27", "female", "carol@example.com"),
        ],
        schema=CUSTOMER_SCHEMA,
    )
    city = spark.createDataFrame(
        [(10, "Jakarta", 100), (20, "Bandung", 200)], schema=CITY_SCHEMA
    )
    state = spark.createDataFrame(
        [(100, "DKI Jakarta", ), (200, "Jawa Barat", )], schema=STATE_SCHEMA
    )
    return customer, city, state


def _by_id(df):
    return {row["customer_id"]: row.asDict() for row in df.collect()}


def test_schema_matches_ticket3_contract(sources):
    dim = dc.build_dim_customer(*sources)
    assert dim.columns == dc.SILVER_COLUMNS
    types = dict(dim.dtypes)
    assert types["customer_id"] == "int"
    assert types["age"] == "int"
    assert all(
        types[c] == "string"
        for c in ("customer_name", "address", "city_name", "state_name", "gender", "email")
    )


def test_join_resolves_city_and_state(sources):
    rows = _by_id(dc.build_dim_customer(*sources))
    assert rows[1]["city_name"] == "Jakarta"
    assert rows[1]["state_name"] == "DKI Jakarta"
    assert rows[2]["city_name"] == "Bandung"
    assert rows[2]["state_name"] == "Jawa Barat"


def test_unmatched_lookup_keeps_row_with_null_lookups(sources):
    """Talend's default left-outer lookup keeps the customer and nulls the lookup fields."""
    dim = dc.build_dim_customer(*sources)
    assert dim.count() == 3
    row = _by_id(dim)[3]
    assert row["city_name"] is None
    # city miss nulls row2.state_id, so the chained state lookup misses too
    assert row["state_name"] is None
    assert row["customer_name"] == "CAROL WHITE"


def test_upcase_applied_to_exactly_the_talend_columns(sources):
    row = _by_id(dc.build_dim_customer(*sources))[1]
    assert row["customer_name"] == "ALICE SMITH"
    assert row["address"] == "12 MAIN ST"
    assert row["gender"] == "FEMALE"
    # email is NOT wrapped in StringHandling.UPCASE in the tMap
    assert row["email"] == "Alice@Example.COM"
    # lookup columns are passed through verbatim
    assert row["city_name"] == "Jakarta"
    assert row["state_name"] == "DKI Jakarta"


def test_no_trimming_since_talend_trims_nothing(spark):
    customer = spark.createDataFrame(
        [(1, "  padded name  ", "  padded address ", 10, "30", " f ", " a@b.com ")],
        schema=CUSTOMER_SCHEMA,
    )
    city = spark.createDataFrame([(10, "  Jakarta ", 100)], schema=CITY_SCHEMA)
    state = spark.createDataFrame([(100, " DKI ")], schema=STATE_SCHEMA)
    row = _by_id(dc.build_dim_customer(customer, city, state))[1]
    assert row["customer_name"] == "  PADDED NAME  "
    assert row["address"] == "  PADDED ADDRESS "
    assert row["gender"] == " F "
    assert row["email"] == " a@b.com "
    assert row["city_name"] == "  Jakarta "


def test_null_text_fields_survive_upcase(spark):
    customer = spark.createDataFrame(
        [(1, None, None, 10, None, None, None)], schema=CUSTOMER_SCHEMA
    )
    city = spark.createDataFrame([(10, "Jakarta", 100)], schema=CITY_SCHEMA)
    state = spark.createDataFrame([(100, "DKI Jakarta")], schema=STATE_SCHEMA)
    row = _by_id(dc.build_dim_customer(customer, city, state))[1]
    assert row["customer_name"] is None
    assert row["age"] is None


def test_dq_passes_on_clean_data(sources):
    customer, _, _ = sources
    dim = dc.build_dim_customer(*sources)
    stats = dc.run_dq_checks(dim, customer.count(), max_unmatched_lookup_rate=0.5)
    assert stats["row_count"] == 3
    assert stats["unmatched_city"] == 1


def test_dq_fails_on_excessive_unmatched_lookups(sources):
    customer, _, _ = sources
    dim = dc.build_dim_customer(*sources)
    with pytest.raises(dc.DataQualityError, match="Unmatched city lookup rate"):
        dc.run_dq_checks(dim, customer.count(), max_unmatched_lookup_rate=0.01)


def test_dq_fails_on_duplicate_customer_id(spark):
    customer = spark.createDataFrame(
        [
            (1, "a", "x", 10, "30", "f", "a@b.com"),
            (1, "b", "y", 10, "31", "m", "b@b.com"),
        ],
        schema=CUSTOMER_SCHEMA,
    )
    city = spark.createDataFrame([(10, "Jakarta", 100)], schema=CITY_SCHEMA)
    state = spark.createDataFrame([(100, "DKI Jakarta")], schema=STATE_SCHEMA)
    dim = dc.build_dim_customer(customer, city, state)
    with pytest.raises(dc.DataQualityError, match="not unique"):
        dc.run_dq_checks(dim, customer.count())


def test_dq_fails_on_null_customer_id(spark):
    customer = spark.createDataFrame(
        [(None, "a", "x", 10, "30", "f", "a@b.com")], schema=CUSTOMER_SCHEMA
    )
    city = spark.createDataFrame([(10, "Jakarta", 100)], schema=CITY_SCHEMA)
    state = spark.createDataFrame([(100, "DKI Jakarta")], schema=STATE_SCHEMA)
    dim = dc.build_dim_customer(customer, city, state)
    with pytest.raises(dc.DataQualityError, match="NULL customer_id"):
        dc.run_dq_checks(dim, customer.count())


def test_dq_fails_on_row_count_mismatch(sources):
    dim = dc.build_dim_customer(*sources)
    with pytest.raises(dc.DataQualityError, match="Row-count mismatch"):
        dc.run_dq_checks(dim, 99, max_unmatched_lookup_rate=0.5)


def _delta_available(spark):
    return "delta" in spark.conf.get("spark.sql.extensions", "")


def test_merge_is_idempotent_and_updates_in_place(spark, sources, tmp_path):
    if not _delta_available(spark):
        pytest.skip("Delta Lake jars unavailable")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver_test")
    target = "silver_test.dim_customer_merge"
    spark.sql(f"DROP TABLE IF EXISTS {target}")

    dim = dc.build_dim_customer(*sources)
    dc.write_dim_customer(spark, dim, target)
    first = spark.table(target).count()
    assert first == 3

    # re-running the same batch must not duplicate rows
    dc.write_dim_customer(spark, dim, target)
    assert spark.table(target).count() == 3

    # a changed attribute for an existing key updates rather than inserts
    customer, city, state = sources
    changed = customer.replace("Alice Smith", "Alice Smythe", "customer_name")
    dc.write_dim_customer(spark, dc.build_dim_customer(changed, city, state), target)
    rows = _by_id(spark.table(target))
    assert len(rows) == 3
    assert rows[1]["customer_name"] == "ALICE SMYTHE"

    spark.sql(f"DROP TABLE IF EXISTS {target}")
