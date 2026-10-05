import pytest
from pyspark.sql.types import IntegerType, StringType, TimestampType

from banking_etl.bronze.fixtures import fixture_targets, stage_fixtures
from banking_etl.bronze.sqlserver import (
    SOURCE_TABLES,
    JdbcConnection,
    JdbcPartitioning,
    bronze_table,
    ingest_sqlserver,
    ingest_table,
    parse_tables,
)
from banking_etl.config import SQLSERVER_SOURCE_TABLES, Settings

from conftest import FIXTURES

EXPECTED_ROWS = {"customer": 20, "city": 52, "state": 9, "account": 21, "branch": 5, "transaction_db": 10}
AUDIT = ["_ingested_at", "_source"]


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    root = tmp_path_factory.mktemp("landing") / "sample_db"
    stage_fixtures(root)
    return str(root)


@pytest.fixture(scope="module")
def ingested(spark, settings, landing):
    return {r["table"]: r for r in ingest_sqlserver(spark, settings, fixture_root=landing)}


def test_source_tables_cover_config_in_order():
    assert tuple(SOURCE_TABLES) == SQLSERVER_SOURCE_TABLES


def test_queries_match_talend_tmssqlinput():
    assert SOURCE_TABLES["branch"].query == (
        "SELECT dbo.branch.branch_id,\n\t\tdbo.branch.branch_name,\n\t\tdbo.branch.branch_location\nFROM\tdbo.branch"
    )
    assert SOURCE_TABLES["transaction_db"].column_names == [
        "transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id",
    ]


def test_parse_tables():
    assert parse_tables(None) == SQLSERVER_SOURCE_TABLES
    assert parse_tables("") == SQLSERVER_SOURCE_TABLES
    assert parse_tables("all") == SQLSERVER_SOURCE_TABLES
    assert parse_tables(" Customer, city,customer ") == ("customer", "city")
    assert parse_tables(["branch"]) == ("branch",)
    with pytest.raises(ValueError, match="unknown source table"):
        parse_tables("customer,transactions")


def test_bronze_table_names():
    s = Settings(catalog="migration_demo", schema_prefix="banking_etl_")
    assert bronze_table(s, "transaction_db") == "migration_demo.banking_etl_bronze.sqlserver_transaction_db"


def test_stage_fixtures_layout(tmp_path):
    (tmp_path / "city").mkdir()
    (tmp_path / "city" / "old.csv").write_text("stale")
    written = stage_fixtures(tmp_path, "city,state")
    assert written == list(fixture_targets(str(tmp_path), "city,state").values())
    assert sorted(p.name for p in (tmp_path / "city").iterdir()) == ["city.csv"]
    assert (tmp_path / "state" / "state.csv").read_bytes() == (FIXTURES / "sample_db" / "state.csv").read_bytes()


def test_row_counts(spark, settings, ingested):
    assert {t: r["rows"] for t, r in ingested.items()} == EXPECTED_ROWS
    for t, n in EXPECTED_ROWS.items():
        assert spark.table(settings.table("bronze", f"sqlserver_{t}")).count() == n


def test_source_names_and_types_preserved(spark, settings, ingested):
    for t, src in SOURCE_TABLES.items():
        schema = spark.table(ingested[t]["target"]).schema
        assert schema.fieldNames() == src.column_names + AUDIT
        assert [f.dataType for f in schema.fields[: len(src.columns)]] == [f.dataType for f in src.schema.fields]
    customer = spark.table(ingested["customer"]["target"]).schema
    assert customer["age"].dataType == StringType()
    assert customer["customer_id"].dataType == IntegerType()
    account = spark.table(ingested["account"]["target"]).schema
    assert account["date_opened"].dataType == TimestampType()
    assert account["balance"].dataType == IntegerType()


def test_values_and_audit_columns(spark, ingested):
    row = spark.table(ingested["transaction_db"]["target"]).where("transaction_id = 3").first()
    assert (row.account_id, str(row.transaction_date), row.amount, row.transaction_type, row.branch_id) == (
        3, "2022-01-11 08:30:00", 10000000, "Transfer", 1,
    )
    assert row._source.startswith("fixture:") and row._source.endswith("/sample_db/transaction_db/")
    assert row._ingested_at is not None
    assert spark.table(ingested["customer"]["target"]).where("customer_id = 1").first().age == "25"


def test_rerun_overwrites_snapshot(spark, settings, landing, ingested):
    target = ingested["branch"]["target"]
    r = ingest_table(spark, settings, "branch", fixture_root=landing)
    assert r["rows"] == 5
    assert spark.table(target).count() == 5
    history = spark.sql(f"DESCRIBE HISTORY {target}").select("operation").collect()
    assert len(history) >= 2 and all("WRITE" in h.operation or "CREATE" in h.operation for h in history)


def test_mode_validation(spark, settings):
    with pytest.raises(ValueError, match="unknown source_mode"):
        ingest_table(spark, Settings(source_mode="odbc"), "branch", fixture_root="/x")
    with pytest.raises(ValueError, match="needs a JdbcConnection"):
        ingest_table(spark, Settings(source_mode="jdbc"), "branch")
    with pytest.raises(ValueError, match="needs dbutils"):
        ingest_sqlserver(spark, Settings(source_mode="jdbc"), "branch")
    with pytest.raises(ValueError, match="volume paths require"):
        ingest_sqlserver(spark, Settings(), "branch")


def test_jdbc_connection_options_keep_credentials_out_of_url():
    c = JdbcConnection("db.local", "1433", "sample", "etl", "s3cr3t")
    assert c.url == "jdbc:sqlserver://db.local:1433;databaseName=sample;encrypt=true;trustServerCertificate=true"
    assert "s3cr3t" not in c.url and "s3cr3t" not in repr(c)
    assert c.options()["password"] == "s3cr3t"
    assert c.options()["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def test_jdbc_connection_from_secrets_and_env():
    class Secrets:
        def get(self, scope, key):
            assert scope == "banking-etl-sqlserver"
            return {"jdbc-host": "h", "jdbc-port": "1", "jdbc-database": "d", "jdbc-user": "u", "jdbc-password": "p"}[key]

    class DbUtils:
        secrets = Secrets()

    assert JdbcConnection.from_secrets(DbUtils(), "banking-etl-sqlserver") == JdbcConnection("h", "1", "d", "u", "p")
    env = {"BANKING_ETL_JDBC_HOST": "h", "BANKING_ETL_JDBC_PORT": "1", "BANKING_ETL_JDBC_DATABASE": "d",
           "BANKING_ETL_JDBC_USER": "u", "BANKING_ETL_JDBC_PASSWORD": "p"}
    assert JdbcConnection.from_env(env) == JdbcConnection("h", "1", "d", "u", "p")
    with pytest.raises(ValueError, match="BANKING_ETL_JDBC_PASSWORD"):
        JdbcConnection.from_env({k: v for k, v in env.items() if not k.endswith("PASSWORD")})


def test_partitioning_options():
    assert JdbcPartitioning("customer_id", 1, 20, 4).options() == {
        "partitionColumn": "customer_id", "lowerBound": "1", "upperBound": "20", "numPartitions": "4",
    }
