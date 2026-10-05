import pytest
from pyspark.sql.types import DateType, DecimalType, IntegerType, LongType, StringType, TimestampType

from banking_etl.config import Settings
from banking_etl.gold.ddl import apply_star_schema, ddl_parameters, render_ddl
from banking_etl.gold.merge import identity_columns

EXPECTED = {
    "dim_branch": [
        ("BranchKey", LongType(), True),
        ("BranchID", IntegerType(), False),
        ("BranchName", StringType(), True),
        ("BranchLocation", StringType(), True),
    ],
    "dim_account": [
        ("AccountKey", LongType(), True),
        ("AccountID", IntegerType(), False),
        ("CustomerID", IntegerType(), True),
        ("AccountType", StringType(), True),
        ("Balance", DecimalType(19, 4), True),
        ("DateOpened", DateType(), True),
        ("Status", StringType(), True),
    ],
    "dim_customer": [
        ("CustomerKey", LongType(), True),
        ("CustomerID", IntegerType(), False),
        ("CustomerName", StringType(), True),
        ("Address", StringType(), True),
        ("CityName", StringType(), True),
        ("StateName", StringType(), True),
        ("Age", IntegerType(), True),
        ("Gender", StringType(), True),
        ("Email", StringType(), True),
    ],
    "fact_transaction": [
        ("TransactionID", IntegerType(), False),
        ("AccountID", IntegerType(), True),
        ("TransactionDate", TimestampType(), True),
        ("Amount", DecimalType(19, 4), True),
        ("TransactionType", StringType(), True),
        ("BranchID", IntegerType(), True),
        ("AccountKey", LongType(), True),
        ("BranchKey", LongType(), True),
    ],
}
FACT_COLS = [c for c, _, _ in EXPECTED["fact_transaction"]]


@pytest.fixture(scope="module")
def star(spark, settings):
    apply_star_schema(spark, settings)
    return ddl_parameters(settings)


def test_render_substitutes_three_part_names_and_keeps_uc_constraints():
    sql = "\n".join(render_ddl(Settings(catalog="migration_demo", schema_prefix="banking_etl_")))
    assert "CREATE TABLE IF NOT EXISTS migration_demo.banking_etl_gold.dim_branch" in sql
    assert "CREATE TABLE IF NOT EXISTS migration_demo.banking_etl_ops.fact_transaction_rejects" in sql
    assert "REFERENCES migration_demo.banking_etl_gold.dim_account (AccountID)" in sql
    assert sql.count("PRIMARY KEY (") == 4
    assert sql.count("FOREIGN KEY (") == 2
    assert "${" not in sql and "uc:begin" not in sql


def test_render_local_strips_uc_only_clauses(settings):
    statements = render_ddl(settings)
    sql = "\n".join(statements)
    assert "PRIMARY KEY (" not in sql and "FOREIGN KEY (" not in sql and "CONSTRAINT pk_" not in sql
    assert "CREATE TABLE IF NOT EXISTS gold.fact_transaction" in sql
    assert len(statements) == 7  # 5 CREATE TABLE + DROP/ADD CHECK on dim_customer
    assert "CONSTRAINT pk_dim_branch PRIMARY KEY (BranchID)" in "\n".join(render_ddl(settings, unity_catalog=True))


@pytest.mark.parametrize("table", sorted(EXPECTED))
def test_gold_schema_matches_legacy_ddl(spark, star, table):
    fields = spark.table(star[table]).schema.fields
    assert [(f.name, f.dataType, f.nullable) for f in fields] == EXPECTED[table]
    assert all(f.metadata.get("comment") for f in fields)


def test_identity_surrogate_keys(spark, star):
    assert identity_columns(spark, star["dim_branch"]) == ["BranchKey"]
    assert identity_columns(spark, star["dim_account"]) == ["AccountKey"]
    assert identity_columns(spark, star["dim_customer"]) == ["CustomerKey"]
    assert identity_columns(spark, star["fact_transaction"]) == []


def test_table_properties_and_comments(spark, star):
    for table in ("dim_branch", "dim_account", "dim_customer", "fact_transaction"):
        props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {star[table]}").collect()}
        assert props["delta.autoOptimize.optimizeWrite"] == "true"
        assert props.get("delta.enableChangeDataFeed") == ("true" if table.startswith("dim_") else None)
        detail = spark.sql(f"DESCRIBE DETAIL {star[table]}").first()
        assert detail["format"] == "delta" and detail["description"]


def test_rejects_table_has_fact_columns_plus_metadata(spark, star):
    fields = spark.table(star["fact_transaction_rejects"]).schema.fields
    assert [f.name for f in fields] == FACT_COLS + ["reject_reason", "rejected_at"]
    assert all(f.nullable for f in fields[: len(FACT_COLS)])
    assert [(f.dataType, f.nullable) for f in fields[-2:]] == [(StringType(), False), (TimestampType(), False)]


def test_apply_is_idempotent(spark, settings, star):
    spark.sql(f"INSERT INTO {star['dim_branch']} (BranchID, BranchName) VALUES (999, 'keep me')")
    apply_star_schema(spark, settings)
    assert spark.table(star["dim_branch"]).filter("BranchID = 999").count() == 1
    spark.sql(f"DELETE FROM {star['dim_branch']} WHERE BranchID = 999")


def test_delta_enforces_not_null_and_check(spark, star):
    with pytest.raises(Exception, match="(?i)null"):
        spark.sql(f"INSERT INTO {star['dim_account']} (AccountID) VALUES (NULL)")
    with pytest.raises(Exception, match="ck_dim_customer_age"):
        spark.sql(f"INSERT INTO {star['dim_customer']} (CustomerID, Age) VALUES (1, -1)")
    assert spark.table(star["dim_account"]).count() == 0
    assert spark.table(star["dim_customer"]).count() == 0
