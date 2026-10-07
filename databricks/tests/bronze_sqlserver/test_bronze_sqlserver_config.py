import pytest

from banking_etl.bronze.sqlserver import (
    TableConfig,
    check_coverage,
    load_config,
    resolve_connection,
)

SAMPLE_TABLES = {"branch", "state", "city", "customer", "account", "transaction_db"}


class FakeSecrets:
    def __init__(self, values):
        self.values = values

    def get(self, scope, key):
        if (scope, key) not in self.values:
            raise KeyError(key)
        return self.values[(scope, key)]


class FakeDbutils:
    def __init__(self, values):
        self.secrets = FakeSecrets(values)


def test_config_covers_every_sample_table():
    config = load_config()
    assert {t.name for t in config.tables} == SAMPLE_TABLES
    assert config.exclude_tables == ["sysdiagrams"]
    assert {t.bronze_name for t in config.tables} == {f"sqlserver_{n}" for n in SAMPLE_TABLES}
    assert all(t.primary_key for t in config.tables)


def test_transaction_db_is_watermark_incremental_and_dims_full():
    config = load_config()
    tx = config.table("transaction_db")
    assert (tx.mode, tx.watermark_column) == ("incremental", "transaction_id")
    assert {t.name for t in config.tables if t.mode == "full"} == SAMPLE_TABLES - {"transaction_db"}


def test_invalid_table_config_rejected():
    with pytest.raises(ValueError, match="mode"):
        TableConfig(name="x", primary_key=["id"], mode="merge")
    with pytest.raises(ValueError, match="watermark_column"):
        TableConfig(name="x", primary_key=["id"], mode="incremental")


def test_duplicate_tables_rejected(tmp_path):
    p = tmp_path / "s.yml"
    p.write_text("tables:\n  - {name: a, primary_key: [id]}\n  - {name: a, primary_key: [id]}\n")
    with pytest.raises(ValueError, match="duplicate"):
        load_config(p)


def test_select_unknown_table_raises():
    with pytest.raises(KeyError):
        load_config().select(["nope"])


def test_connection_defaults_and_env():
    config = load_config()
    conn = resolve_connection(config, env={"SQLSERVER_USER": "etl", "SQLSERVER_PASSWORD": "pw"})
    assert (conn.host, conn.port, conn.database, conn.user, conn.password) == (
        "localhost", "1433", "sample", "etl", "pw",
    )
    assert conn.url.startswith("jdbc:sqlserver://localhost:1433;databaseName=sample;")
    assert "encrypt=true" in conn.url and "trustServerCertificate=true" in conn.url
    assert conn.reader_options()["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert "pw" not in repr(conn)


def test_secret_scope_wins_over_env_and_override_wins_over_both():
    config = load_config()
    dbutils = FakeDbutils(
        {
            ("banking-etl-sqlserver", "jdbc-host"): "sql.internal",
            ("banking-etl-sqlserver", "jdbc-password"): "from-scope",
        }
    )
    env = {"SQLSERVER_HOST": "env-host", "SQLSERVER_PASSWORD": "from-env", "SQLSERVER_USER": "u"}
    conn = resolve_connection(config, dbutils=dbutils, env=env)
    assert (conn.host, conn.password, conn.user) == ("sql.internal", "from-scope", "u")
    conn = resolve_connection(config, dbutils=dbutils, env=env, overrides={"host": "cli-host"})
    assert conn.host == "cli-host"


def test_missing_required_connection_field_fails(tmp_path):
    p = tmp_path / "s.yml"
    p.write_text("connection: {host: {env: H}}\ntables:\n  - {name: a, primary_key: [id]}\n")
    with pytest.raises(ValueError, match="host/port/database"):
        resolve_connection(load_config(p), env={})


def test_check_coverage_flags_unconfigured_and_missing_tables():
    config = load_config()
    discovered = sorted(SAMPLE_TABLES | {"sysdiagrams"})
    assert check_coverage(config, discovered) == {"unconfigured": [], "missing_in_source": []}
    result = check_coverage(config, sorted((SAMPLE_TABLES - {"city"}) | {"loan", "sysdiagrams"}))
    assert result == {"unconfigured": ["loan"], "missing_in_source": ["city"]}
