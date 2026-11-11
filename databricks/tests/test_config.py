import argparse

import pytest

from banking_etl import DEFAULT_CATALOG, EtlConfig
from banking_etl.config import parse_bool, quote_identifier


def test_defaults_match_shared_layout():
    cfg = EtlConfig()
    assert cfg.catalog == DEFAULT_CATALOG == "banking_dwh"
    assert cfg.schemas == {"bronze": "bronze", "silver": "silver", "gold": "gold"}
    assert cfg.table("gold", "dim_account") == "banking_dwh.gold.dim_account"
    assert cfg.table("bronze", "transaction_csv") == "banking_dwh.bronze.transaction_csv"
    assert cfg.landing_path == "/Volumes/banking_dwh/bronze/landing"
    assert cfg.landing_volume_fqn == "banking_dwh.bronze.landing"


def test_schema_prefix_applies_to_every_schema_and_landing_volume():
    cfg = EtlConfig(catalog="migration_demo", schema_prefix="ci_42_")
    assert cfg.schema_fqn("silver") == "migration_demo.ci_42_silver"
    assert cfg.table("gold", "fact_transaction") == "migration_demo.ci_42_gold.fact_transaction"
    assert cfg.landing_path == "/Volumes/migration_demo/ci_42_bronze/landing"
    assert cfg.landing_file("transaction_csv.csv") == (
        "/Volumes/migration_demo/ci_42_bronze/landing/transaction_csv.csv"
    )


def test_custom_schema_names():
    cfg = EtlConfig(bronze_schema="raw", gold_schema="mart")
    assert cfg.schema("bronze") == "raw"
    assert cfg.schema("gold") == "mart"
    assert cfg.landing_path.endswith("/raw/landing")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "bad-name"},
        {"catalog": "1abc"},
        {"catalog": ""},
        {"gold_schema": "gold; DROP"},
        {"landing_volume": "a.b"},
        {"schema_prefix": "x-"},
    ],
)
def test_invalid_identifiers_rejected(kwargs):
    with pytest.raises(ValueError):
        EtlConfig(**kwargs)


def test_unknown_layer_and_bad_table_name():
    cfg = EtlConfig()
    with pytest.raises(ValueError):
        cfg.schema("platinum")
    with pytest.raises(ValueError):
        cfg.table("gold", "dim account")


def test_from_env_and_mapping():
    env = {"BANKING_ETL_CATALOG": "dev_cat", "BANKING_ETL_SCHEMA_PREFIX": "me_", "OTHER": "x"}
    cfg = EtlConfig.from_env(env)
    assert cfg.catalog == "dev_cat"
    assert cfg.schema("bronze") == "me_bronze"
    assert EtlConfig.from_mapping({"catalog": "c1", "unrelated": "x"}).catalog == "c1"
    assert EtlConfig.from_env({}) == EtlConfig()


def test_cli_args_override_env():
    env = {"BANKING_ETL_CATALOG": "env_cat", "BANKING_ETL_GOLD_SCHEMA": "env_gold"}
    cfg = EtlConfig.from_args(["--catalog", "cli_cat", "--unrelated", "1"], environ=env)
    assert cfg.catalog == "cli_cat"
    assert cfg.schema("gold") == "env_gold"


def test_cli_override_wins_over_invalid_env_value():
    env = {"BANKING_ETL_CATALOG": "bad-name"}
    assert EtlConfig.from_args(["--catalog", "good_cat"], environ=env).catalog == "good_cat"
    with pytest.raises(ValueError):
        EtlConfig.from_args([], environ=env)


def test_add_arguments_reusable_by_other_entry_points():
    parser = EtlConfig.add_arguments(argparse.ArgumentParser())
    parser.add_argument("--source-file")
    ns = parser.parse_args(["--catalog=x", "--schema-prefix=", "--source-file", "a.csv"])
    cfg = EtlConfig.from_namespace(ns, environ={})
    assert (cfg.catalog, cfg.schema_prefix, ns.source_file) == ("x", "", "a.csv")


def test_helpers():
    assert quote_identifier("a`b") == "`a``b`"
    assert parse_bool("TRUE") is True and parse_bool("0") is False and parse_bool(False) is False
    with pytest.raises(ValueError):
        parse_bool("maybe")
