import pytest

from banking_etl.bronze.federation import FederationOptions, federation_statements, foreign_table
from banking_etl.setup.provision import plan
from banking_etl.config import Settings


def test_federation_sql_renders_with_secrets():
    stmts = federation_statements(FederationOptions(host="sql.example.internal"))
    assert len(stmts) == 2
    conn, cat = stmts
    assert conn.startswith("CREATE CONNECTION IF NOT EXISTS `banking_etl_sqlserver` TYPE sqlserver")
    assert "host 'sql.example.internal'" in conn and "port '1433'" in conn
    assert "user secret('banking-etl-sqlserver', 'jdbc-user')" in conn
    assert "password secret('banking-etl-sqlserver', 'jdbc-password')" in conn
    assert cat.startswith(
        "CREATE FOREIGN CATALOG IF NOT EXISTS `banking_etl_sqlserver_sample` USING CONNECTION `banking_etl_sqlserver`"
    )
    assert cat.endswith("OPTIONS (database 'sample')")


def test_federation_rejects_injection():
    with pytest.raises(ValueError):
        federation_statements(FederationOptions(host="x' OR 1=1"))
    with pytest.raises(ValueError):
        federation_statements(FederationOptions(host="h", connection="a`b"))


def test_federation_is_not_part_of_provisioning():
    s = Settings(catalog="migration_demo", schema_prefix="banking_etl_")
    from banking_etl.setup.provision import ProvisionOptions

    sql = "\n".join(st for step in plan(s, ProvisionOptions(True, "account users", "sp")) for st in step.statements)
    assert "CONNECTION" not in sql and "FOREIGN CATALOG" not in sql


def test_foreign_table():
    assert foreign_table(FederationOptions(host="h"), "account") == "`banking_etl_sqlserver_sample`.dbo.`account`"
