from __future__ import annotations

import json
from pathlib import Path

import pytest

from banking_etl import config as config_module
from banking_etl.config import interpolate_env, load_config, resolve_path

CONFIG_FILE = Path(__file__).resolve().parents[1] / "config" / "config.yaml"


def test_interpolate_env_uses_value_then_default() -> None:
    env = {"SET": "value", "EMPTY": ""}
    assert interpolate_env("${SET}", env) == "value"
    assert interpolate_env("${MISSING:-fallback}", env) == "fallback"
    assert interpolate_env("${EMPTY:-fallback}", env) == "fallback"
    assert interpolate_env("${MISSING}", env) == ""
    assert interpolate_env("a/${SET}/b", env) == "a/value/b"


def test_resolve_path_keeps_uris_and_absolute_paths(tmp_path: Path) -> None:
    assert resolve_path("s3a://bucket/key.csv", tmp_path) == "s3a://bucket/key.csv"
    assert resolve_path("/Volumes/main/landing/x.csv", tmp_path) == "/Volumes/main/landing/x.csv"
    assert resolve_path("data/x.csv", tmp_path) == str(tmp_path / "data" / "x.csv")


def test_default_config_points_at_repo_data_sources() -> None:
    cfg = load_config(CONFIG_FILE, env={})
    repo = CONFIG_FILE.parents[2]
    assert cfg.transaction_csv.path == str(repo / "data_sources" / "transaction_csv.csv")
    assert cfg.transaction_excel.path == str(repo / "data_sources" / "transaction_excel.xlsx")
    assert cfg.transaction_csv.timestamp_format == "dd-MM-yyyy HH:mm:ss"
    assert cfg.warehouse.path_based
    assert cfg.table_settings("dim_customer").write_mode == "merge"
    assert cfg.table_settings("fact_transaction").write_mode == "overwrite"
    assert cfg.orphan_policy == "reject"
    assert cfg.sqlserver.credentials() == ("", "")


def test_databricks_style_env_selects_unity_catalog_tables() -> None:
    cfg = load_config(
        CONFIG_FILE,
        env={
            "DWH_STORAGE": "table",
            "DWH_CATALOG": "banking",
            "TRANSACTION_CSV_PATH": "s3://landing/transactions/transaction_csv.csv",
        },
    )
    assert not cfg.warehouse.path_based
    assert cfg.warehouse.table_name("dim_branch") == "banking.dwh.dim_branch"
    assert cfg.transaction_csv.path == "s3://landing/transactions/transaction_csv.csv"


def test_invalid_choice_is_rejected() -> None:
    with pytest.raises(ValueError, match="orphan_policy"):
        load_config(CONFIG_FILE, env={"FACT_ORPHAN_POLICY": "ignore"})


def test_credentials_resolved_from_secrets_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []

    class FakeClient:
        def get_secret_value(self, SecretId: str) -> dict[str, str]:  # noqa: N803
            calls.append(("get", SecretId))
            return {"SecretString": json.dumps({"username": "etl_reader", "password": "s3cr3t"})}

    def fake_client(service: str, region_name: str | None = None) -> FakeClient:
        calls.append((service, region_name))
        return FakeClient()

    monkeypatch.setattr(config_module.boto3, "client", fake_client)
    cfg = load_config(
        CONFIG_FILE,
        env={
            "SOURCE_DB_SECRET_ID": "banking/sqlserver/etl",
            "AWS_REGION": "us-east-1",
            "SOURCE_DB_USER": "ignored",
        },
    )
    assert cfg.sqlserver.credentials() == ("etl_reader", "s3cr3t")
    assert calls == [("secretsmanager", "us-east-1"), ("get", "banking/sqlserver/etl")]
