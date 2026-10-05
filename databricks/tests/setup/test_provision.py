import pytest

from banking_etl.config import Settings
from banking_etl.setup.provision import (
    ProvisionOptions,
    landing_dirs,
    parse_bool,
    plan,
    provision,
    quote,
    split_statements,
)

SHARED = Settings(catalog="migration_demo", schema_prefix="banking_etl_")


def _all(steps):
    return [s for step in steps for s in step.statements]


def test_shared_catalog_plan_honours_prefix_and_skips_catalog():
    steps = plan(SHARED)
    assert [s.name for s in steps] == ["schemas", "volumes"]
    sql = _all(steps)
    assert sql[0].startswith("CREATE SCHEMA IF NOT EXISTS `migration_demo`.`banking_etl_bronze`")
    for layer in ("bronze", "silver", "gold", "ops"):
        assert any(f"`migration_demo`.`banking_etl_{layer}`" in s for s in sql)
    assert any(s.startswith("CREATE VOLUME IF NOT EXISTS `migration_demo`.`banking_etl_bronze`.`landing`") for s in sql)
    assert any(s.startswith("CREATE VOLUME IF NOT EXISTS `migration_demo`.`banking_etl_ops`.`checkpoints`") for s in sql)
    assert not any("CATALOG" in s or "GRANT" in s for s in sql)


def test_full_plan_with_catalog_and_grants():
    opts = ProvisionOptions(create_catalog=True, data_engineers="data-engineers", jobs_principal="1234-app-id")
    steps = plan(Settings(catalog="banking_etl_dev"), opts)
    assert [s.name for s in steps] == [
        "catalog", "schemas", "volumes", "grants_catalog", "grants_data_engineers", "grants_jobs_principal",
    ]
    assert [s.optional for s in steps] == [True, False, False, True, False, False]
    by = {s.name: s.statements for s in steps}
    assert by["catalog"][0].startswith("CREATE CATALOG IF NOT EXISTS `banking_etl_dev`")
    assert by["grants_catalog"] == ("GRANT USE CATALOG ON CATALOG `banking_etl_dev` TO `data-engineers`",)
    assert by["grants_data_engineers"] == (
        "GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA `banking_etl_dev`.`silver` TO `data-engineers`",
        "GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA `banking_etl_dev`.`gold` TO `data-engineers`",
    )
    assert len(by["grants_jobs_principal"]) == 4
    assert all(s.startswith("GRANT ALL PRIVILEGES ON SCHEMA") and s.endswith("TO `1234-app-id`")
               for s in by["grants_jobs_principal"])


def test_optional_step_failure_is_tolerated_but_required_step_raises():
    executed = []

    def execute(sql):
        if sql.startswith("CREATE CATALOG"):
            raise RuntimeError("PERMISSION_DENIED: User does not have CREATE CATALOG on Metastore\nmore")
        executed.append(sql)

    results = provision(execute, SHARED, ProvisionOptions(create_catalog=True))
    assert results[0] == ("catalog", "skipped: PERMISSION_DENIED: User does not have CREATE CATALOG on Metastore")
    assert [r[1] for r in results[1:]] == ["ok", "ok"]
    assert len(executed) == 6

    def fail_volumes(sql):
        if "VOLUME" in sql:
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        provision(fail_volumes, SHARED)


def test_quote_rejects_injection():
    assert quote("a", "b-c") == "`a`.`b-c`"
    with pytest.raises(ValueError):
        quote("x` TO `admins")
    with pytest.raises(ValueError):
        quote("")


def test_split_statements_strips_comments():
    assert split_statements("-- c; x\nSELECT 1; -- t\n\nSELECT 2;\n") == ["SELECT 1", "SELECT 2"]


def test_landing_dirs_and_parse_bool():
    assert landing_dirs(SHARED) == [
        "/Volumes/migration_demo/banking_etl_bronze/landing/transactions/csv",
        "/Volumes/migration_demo/banking_etl_bronze/landing/transactions/excel",
        "/Volumes/migration_demo/banking_etl_bronze/landing/sample_db",
    ]
    assert parse_bool("True") and parse_bool(" yes ") and not parse_bool("false") and not parse_bool("")


def test_local_provision_is_idempotent(spark):
    local = Settings(schema_prefix="t1_")
    for _ in range(2):
        assert provision(spark.sql, local) == [("schemas", "ok")]
    dbs = {r[0] for r in spark.sql("SHOW DATABASES").collect()}
    assert {"t1_bronze", "t1_silver", "t1_gold", "t1_ops"} <= dbs
