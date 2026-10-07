import os
import re

import pytest

import setup_unity_catalog as setup
from banking_etl.common.config import LANDING_SUBDIRS, EnvConfig

from conftest import DATABRICKS_ROOT


def _sqls(cfg=None, uc=True):
    return [s.sql for s in setup.build_statements(cfg or EnvConfig(), uc)]


def test_creates_all_four_schemas_idempotently():
    creates = [s for s in _sqls() if s.startswith("CREATE SCHEMA")]
    assert [re.search(r"`(banking_mig_\w+)`", s).group(1) for s in creates] == [
        "banking_mig_bronze",
        "banking_mig_silver",
        "banking_mig_gold",
        "banking_mig_ops",
    ]
    assert all("IF NOT EXISTS `migration_demo`." in s for s in creates)


def test_volume_and_grants():
    sqls = _sqls()
    assert "CREATE VOLUME IF NOT EXISTS `migration_demo`.`banking_mig_bronze`.`landing`" in "\n".join(sqls)
    schema_grants = [s for s in sqls if s.startswith("GRANT") and " ON SCHEMA " in s]
    assert len(schema_grants) == 4
    assert all(s.endswith("TO `account users`") for s in schema_grants)
    assert all("USE SCHEMA, SELECT, EXECUTE" in s for s in schema_grants)
    assert any(s.startswith("GRANT READ VOLUME ON VOLUME") for s in sqls)


def test_statement_order_schemas_then_volume_then_grants():
    kinds = [s.kind for s in setup.build_statements(EnvConfig()) if not s.optional]
    assert kinds.index("volume") > max(i for i, k in enumerate(kinds) if k == "schema")
    assert min(i for i, k in enumerate(kinds) if k == "grant") > kinds.index("volume")


def test_only_catalog_grant_is_optional():
    optional = [s for s in setup.build_statements(EnvConfig()) if s.optional]
    assert [s.sql for s in optional] == ["GRANT USE CATALOG ON CATALOG `migration_demo` TO `account users`"]


def test_parametrised_names_flow_through():
    sqls = "\n".join(_sqls(EnvConfig(catalog="cat_x", schema_prefix="p_", grant_principal="data-eng")))
    assert "`cat_x`.`p_gold`" in sqls
    assert "TO `data-eng`" in sqls
    assert "banking_mig_" not in sqls


def test_local_mode_has_no_catalog_volume_or_grants():
    sqls = _sqls(uc=False)
    assert all("migration_demo" not in s for s in sqls)
    assert not any(s.startswith(("GRANT", "CREATE VOLUME")) for s in sqls)


def test_comment_literal_escaping():
    assert setup._lit("it's") == "'it\\'s'"


def test_committed_sql_matches_renderer():
    path = os.path.join(DATABRICKS_ROOT, "setup", "unity_catalog_setup.sql")
    with open(path) as fh:
        assert fh.read() == setup.render_sql(EnvConfig())


def test_print_sql_cli(capsys):
    assert setup.main(["--print-sql", "--catalog", "c1", "--schema-prefix", "x_"]) == 0
    out = capsys.readouterr().out
    assert "CREATE SCHEMA IF NOT EXISTS `c1`.`x_ops`" in out


class _Recorder:
    def __init__(self, fail_on=None, error="PERMISSION_DENIED: User does not have MANAGE on Catalog"):
        self.sql = []
        self.fail_on = fail_on
        self.error = error

    def __call__(self, sql):
        self.sql.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(self.error)
        return []


def test_optional_permission_failure_is_skipped():
    rec = _Recorder(fail_on="ON CATALOG")
    results = setup.apply_statements(setup.build_statements(EnvConfig()), rec)
    assert results[0]["status"] == "skipped"
    assert all(r["status"] == "ok" for r in results[1:])
    assert len(rec.sql) == len(results)


def test_required_failure_raises():
    with pytest.raises(RuntimeError):
        setup.apply_statements(setup.build_statements(EnvConfig()), _Recorder(fail_on="CREATE VOLUME"))


def test_optional_non_permission_failure_raises():
    rec = _Recorder(fail_on="ON CATALOG", error="PARSE_SYNTAX_ERROR")
    with pytest.raises(RuntimeError):
        setup.apply_statements(setup.build_statements(EnvConfig()), rec)


def test_landing_dirs():
    assert setup.landing_dirs(EnvConfig()) == [
        f"/Volumes/migration_demo/banking_mig_bronze/landing/{d}" for d in LANDING_SUBDIRS
    ]


def test_local_spark_setup_is_idempotent(spark, tmp_path):
    cfg = EnvConfig(schema_prefix="banking_mig_t1_local_")
    first = setup.setup_with_spark(spark, cfg, unity_catalog=False, mkdirs=lambda p: os.makedirs(p, exist_ok=True), landing_base=str(tmp_path / "landing"))
    second = setup.setup_with_spark(spark, cfg, unity_catalog=False, mkdirs=lambda p: os.makedirs(p, exist_ok=True), landing_base=str(tmp_path / "landing"))
    expected = sorted(cfg.schema_name(layer) for layer in cfg.layers)
    assert first["schemas"] == second["schemas"] == expected
    assert all(r["status"] == "ok" for r in second["statements"])
    for sub in LANDING_SUBDIRS:
        assert (tmp_path / "landing" / sub).is_dir()
    desc = {r[0]: r[1] for r in spark.sql(f"DESCRIBE SCHEMA EXTENDED {cfg.schema_name('gold')}").collect()}
    assert desc.get("Comment", "").startswith("Gold:")


def test_local_spark_schemas_accept_delta_tables(spark):
    cfg = EnvConfig(schema_prefix="banking_mig_t1_delta_")
    setup.setup_with_spark(spark, cfg, unity_catalog=False)
    spark.sql(f"CREATE TABLE {cfg.schema_name('ops')}.probe (id INT) USING DELTA")
    spark.sql(f"INSERT INTO {cfg.schema_name('ops')}.probe VALUES (1)")
    assert spark.table(f"{cfg.schema_name('ops')}.probe").count() == 1


def _bundle_variables():
    with open(os.path.join(DATABRICKS_ROOT, "databricks.yml")) as fh:
        text = fh.read()
    block = text.split("\nvariables:\n", 1)[1].split("\ntargets:\n", 1)[0]
    return dict(re.findall(r"^  (\w+):\n(?:    .*\n)*?    default: (.+)$", block, re.M)), text


def test_bundle_defaults_match_config():
    variables, text = _bundle_variables()
    cfg = EnvConfig()
    assert variables == {
        "catalog": cfg.catalog,
        "schema_prefix": cfg.schema_prefix,
        "landing_volume": cfg.landing_volume,
        "grant_principal": cfg.grant_principal,
    }
    assert re.search(r"^  dev:\n    mode: development\n    default: true", text, re.M)
    assert "notebook_path: ./setup/setup_unity_catalog_notebook.py" in text
    assert "new_cluster" not in text
    assert os.path.isfile(os.path.join(DATABRICKS_ROOT, "setup", "setup_unity_catalog_notebook.py"))


def test_setup_with_spark_raises_when_mkdirs_reports_failure(spark, tmp_path):
    cfg = EnvConfig(schema_prefix="t1_mkfail_")
    with pytest.raises(RuntimeError, match="Failed to create landing directory"):
        setup.setup_with_spark(spark, cfg, unity_catalog=False, mkdirs=lambda p: False, landing_base=str(tmp_path / "landing"))
