import pytest

from banking_dwh.common.config import EnvConfig, validate_identifier
from banking_dwh.common.uc_setup import landing_folders, setup_statements


def test_prod_target_is_banking_dwh_catalog():
    cfg = EnvConfig.from_target("prod")
    assert cfg.catalog == "banking_dwh"
    assert [cfg.schema(layer) for layer in ("bronze", "silver", "gold")] == [
        "banking_dwh.bronze", "banking_dwh.silver", "banking_dwh.gold"]
    assert cfg.table("silver", "dim_account") == "banking_dwh.silver.dim_account"


def test_dev_target_prefixes_schemas():
    cfg = EnvConfig.from_target("dev")
    assert cfg.catalog == "migration_demo"
    assert cfg.schema("bronze") == "migration_demo.bdwh_bronze"
    assert cfg.ticket_schema(4) == "migration_demo.bdwh_t4"


def test_volume_and_landing_paths():
    cfg = EnvConfig.from_target("prod")
    assert cfg.landing_path("csv_transaction") == "/Volumes/banking_dwh/bronze/landing/csv/transaction"
    assert cfg.landing_path("excel_transaction") == "/Volumes/banking_dwh/bronze/landing/excel/transaction"
    assert cfg.landing_path("sqlserver", "customer") == "/Volumes/banking_dwh/bronze/landing/sqlserver/customer"
    assert cfg.landing_path("csv", "transaction") == cfg.landing_path("csv_transaction")
    assert cfg.landing_path("excel", "transaction") == cfg.landing_path("excel_transaction")
    assert cfg.landing_path("csv", "transaction", "2024") == "/Volumes/banking_dwh/bronze/landing/csv/transaction/2024"
    with pytest.raises(ValueError):
        cfg.landing_path("parquet", "transaction")
    assert cfg.checkpoint_path("bronze_csv") == "/Volumes/banking_dwh/ops/checkpoints/bronze_csv/checkpoint"
    assert cfg.checkpoint_path("bronze_csv", "schema").endswith("/bronze_csv/schema")
    with pytest.raises(ValueError):
        cfg.volume_path("bronze", "landing", "../../etc")


@pytest.mark.parametrize("bad", ["a-b", "x; DROP TABLE y", "", "1abc", "a.b", "`x`"])
def test_identifiers_are_validated(bad):
    with pytest.raises(ValueError):
        validate_identifier(bad)
    with pytest.raises(ValueError):
        EnvConfig.from_target("dev", catalog=bad or "a b")


def test_bad_schema_prefix_rejected():
    with pytest.raises(ValueError):
        EnvConfig.from_target("dev", schema_prefix="x;--")


def test_setup_statements_prod_creates_catalog_schemas_volumes():
    stmts = setup_statements(EnvConfig.from_target("prod"), apply_grants=True)
    assert stmts[0].startswith("CREATE CATALOG IF NOT EXISTS banking_dwh")
    for layer in ("bronze", "silver", "gold", "ops"):
        assert any(s.startswith(f"CREATE SCHEMA IF NOT EXISTS banking_dwh.{layer} ") for s in stmts)
    assert any("CREATE VOLUME IF NOT EXISTS banking_dwh.bronze.landing" in s for s in stmts)
    assert any("CREATE VOLUME IF NOT EXISTS banking_dwh.ops.checkpoints" in s for s in stmts)
    assert any(s == "GRANT USE SCHEMA, SELECT ON SCHEMA banking_dwh.gold TO `account users`" for s in stmts)


def test_setup_statements_dev_skips_catalog_and_catalog_grants():
    stmts = setup_statements(EnvConfig.from_target("dev"), apply_grants=True)
    assert not any("CATALOG" in s.split(" ON ")[0] and s.startswith(("CREATE CATALOG", "GRANT USE CATALOG")) for s in stmts)
    assert "CREATE SCHEMA IF NOT EXISTS migration_demo.bdwh_silver" in stmts[1]


def test_landing_folders_cover_all_sources():
    folders = landing_folders(EnvConfig.from_target("dev"))
    assert "/Volumes/migration_demo/bdwh_bronze/landing/csv/transaction" in folders
    assert "/Volumes/migration_demo/bdwh_bronze/landing/excel/transaction" in folders
    for t in ("branch", "account", "customer", "city", "state", "transaction_db"):
        assert f"/Volumes/migration_demo/bdwh_bronze/landing/sqlserver/{t}" in folders


def test_setup_statements_run_on_local_spark(spark):
    cfg = EnvConfig.from_target("dev", catalog="spark_catalog", schema_prefix="t1_")
    for s in setup_statements(cfg):
        if s.startswith("CREATE SCHEMA"):
            spark.sql(s)
    names = {r[0] for r in spark.sql("SHOW SCHEMAS").collect()}
    assert {"t1_bronze", "t1_silver", "t1_gold", "t1_ops"} <= names


class _FakeSecrets:
    def get(self, scope, key):
        assert scope == "banking-etl-sqlserver"
        return {"jdbc-host": "sql.example", "jdbc-port": "1433", "jdbc-database": "sample",
                "jdbc-user": "u", "jdbc-password": "p"}[key]


class _FakeDbutils:
    secrets = _FakeSecrets()


def test_jdbc_options_verify_tls_by_default():
    from banking_dwh.common.config import jdbc_options

    cfg = EnvConfig.from_target("prod")
    opts = jdbc_options(cfg, _FakeDbutils())
    assert opts["url"].startswith("jdbc:sqlserver://sql.example:1433;databaseName=sample;")
    assert "encrypt=true" in opts["url"] and "trustServerCertificate=false" in opts["url"]
    local = jdbc_options(cfg, _FakeDbutils(), trust_server_certificate=True)
    assert "trustServerCertificate=true" in local["url"]
