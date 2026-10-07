import pytest

from banking_etl.common.config import (
    ENV_CATALOG,
    ENV_SCHEMA_PREFIX,
    LAYERS,
    EnvConfig,
    quote_identifier,
)


def test_defaults_match_shared_conventions():
    cfg = EnvConfig()
    assert cfg.catalog == "migration_demo"
    assert cfg.schema_prefix == "banking_mig_"
    assert cfg.bronze == "migration_demo.banking_mig_bronze"
    assert cfg.silver == "migration_demo.banking_mig_silver"
    assert cfg.gold == "migration_demo.banking_mig_gold"
    assert cfg.ops == "migration_demo.banking_mig_ops"
    assert list(cfg.schemas) == list(LAYERS)
    assert cfg.grant_principal == "account users"


def test_table_and_volume_names():
    cfg = EnvConfig()
    assert cfg.table("bronze", "sqlserver_customer") == "migration_demo.banking_mig_bronze.sqlserver_customer"
    assert cfg.table("gold", "fact_transaction") == "migration_demo.banking_mig_gold.fact_transaction"
    assert cfg.landing_volume_name == "migration_demo.banking_mig_bronze.landing"
    assert cfg.landing_path == "/Volumes/migration_demo/banking_mig_bronze/landing"
    assert cfg.landing_subpath("transactions", "/csv/") == "/Volumes/migration_demo/banking_mig_bronze/landing/transactions/csv"
    assert cfg.ticket_schema(3) == "migration_demo.banking_mig_t3"


def test_parametrised_catalog_and_prefix():
    cfg = EnvConfig(catalog="other_cat", schema_prefix="bank_")
    assert cfg.gold == "other_cat.bank_gold"
    assert cfg.landing_path == "/Volumes/other_cat/bank_bronze/landing"
    assert EnvConfig(schema_prefix="").bronze == "migration_demo.bronze"


def test_from_env_and_overrides():
    env = {ENV_CATALOG: "cat_a", ENV_SCHEMA_PREFIX: "pre_"}
    assert EnvConfig.from_env(env).silver == "cat_a.pre_silver"
    assert EnvConfig.from_env(env, catalog="cat_b").silver == "cat_b.pre_silver"
    assert EnvConfig.from_env(env, catalog=None).catalog == "cat_a"
    assert EnvConfig.from_env({}).catalog == "migration_demo"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "bad-name"},
        {"catalog": "x; DROP SCHEMA y"},
        {"schema_prefix": "has space_"},
        {"landing_volume": "1volume"},
        {"grant_principal": "users`; --"},
    ],
)
def test_rejects_unsafe_identifiers(kwargs):
    with pytest.raises(ValueError):
        EnvConfig(**kwargs)


def test_table_rejects_bad_name():
    with pytest.raises(ValueError):
        EnvConfig().table("gold", "dim-branch")


def test_quote_identifier_escapes_backticks():
    assert quote_identifier("account users") == "`account users`"
    assert quote_identifier("a`b") == "`a``b`"


class _FakeWidgets:
    def __init__(self, values):
        self.values = values
        self.defaults = {}

    def text(self, name, default):
        self.defaults[name] = default

    def get(self, name):
        return self.values.get(name, self.defaults[name])


class _FakeDbutils:
    def __init__(self, values):
        self.widgets = _FakeWidgets(values)


def test_from_widgets_uses_job_parameters_and_defaults():
    dbutils = _FakeDbutils({"schema_prefix": "banking_mig_t1_"})
    cfg = EnvConfig.from_widgets(dbutils)
    assert dbutils.widgets.defaults["catalog"] == "migration_demo"
    assert cfg.bronze == "migration_demo.banking_mig_t1_bronze"
