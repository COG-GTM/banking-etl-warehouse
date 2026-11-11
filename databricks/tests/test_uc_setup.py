import pytest

from banking_etl import EtlConfig
from banking_etl.uc_setup import main, run_setup, setup_statements


def test_statements_create_catalog_schemas_and_volume_in_order():
    stmts = setup_statements(EtlConfig())
    assert [s.split(" IF NOT EXISTS ")[0] for s in stmts] == [
        "CREATE CATALOG",
        "CREATE SCHEMA",
        "CREATE SCHEMA",
        "CREATE SCHEMA",
        "CREATE VOLUME",
    ]
    assert stmts[0].startswith("CREATE CATALOG IF NOT EXISTS `banking_dwh`")
    assert stmts[1].startswith("CREATE SCHEMA IF NOT EXISTS `banking_dwh`.`bronze`")
    assert stmts[2].startswith("CREATE SCHEMA IF NOT EXISTS `banking_dwh`.`silver`")
    assert stmts[3].startswith("CREATE SCHEMA IF NOT EXISTS `banking_dwh`.`gold`")
    assert stmts[4].startswith("CREATE VOLUME IF NOT EXISTS `banking_dwh`.`bronze`.`landing`")
    assert all("IF NOT EXISTS" in s for s in stmts)


def test_statements_respect_flags_and_prefix():
    cfg = EtlConfig(catalog="migration_demo", schema_prefix="run1_")
    stmts = setup_statements(cfg, create_catalog=False, create_volume=False)
    assert len(stmts) == 3
    assert not any("CATALOG" in s or "VOLUME" in s for s in stmts)
    assert "`migration_demo`.`run1_gold`" in stmts[2]


def test_print_sql_cli(capsys):
    assert main(["--catalog", "cat_x", "--schema-prefix", "p_", "--create-catalog", "false", "--print-sql"]) == 0
    out = capsys.readouterr().out
    assert "CREATE CATALOG" not in out
    assert "`cat_x`.`p_silver`" in out
    assert "CREATE VOLUME IF NOT EXISTS `cat_x`.`p_bronze`.`landing`" in out


def test_cli_rejects_bad_catalog():
    with pytest.raises(ValueError):
        main(["--catalog", "bad-name", "--print-sql"])


@pytest.mark.spark
def test_run_setup_is_idempotent_on_local_spark(spark, etl_config):
    try:
        for _ in range(2):
            ran = run_setup(spark, etl_config, create_catalog=False, create_volume=False)
        assert len(ran) == 3
        for schema in etl_config.schemas.values():
            assert spark.catalog.databaseExists(schema)
    finally:
        for schema in etl_config.schemas.values():
            spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
