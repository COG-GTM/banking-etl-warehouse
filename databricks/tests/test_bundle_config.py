"""Static checks of databricks.yml / resources/*.yml / pyproject.toml wiring."""

import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    tomllib = pytest.importorskip("tomli")


@pytest.fixture(scope="module")
def bundle(bundle_root: Path) -> dict:
    return yaml.safe_load((bundle_root / "databricks.yml").read_text())


def test_bundle_core_settings(bundle):
    assert bundle["bundle"]["name"] == "banking_etl_warehouse"
    assert bundle["include"] == ["resources/*.yml"]
    assert bundle["variables"]["catalog"]["default"] == "banking_dwh"
    assert set(bundle["targets"]) == {"dev", "prod"}
    assert bundle["targets"]["dev"]["mode"] == "development"
    assert bundle["targets"]["dev"]["default"] is True
    assert bundle["targets"]["prod"]["mode"] == "production"


def test_resource_files_are_valid_yaml_with_resources_key(bundle_root: Path):
    files = sorted((bundle_root / "resources").glob("*.yml"))
    assert files, "resources/ must contain at least the uc_setup job"
    for f in files:
        doc = yaml.safe_load(f.read_text())
        assert isinstance(doc, dict) and "resources" in doc, f.name


def test_uc_setup_job_wires_bundle_variables_to_wheel_entry_point(bundle_root: Path, bundle):
    job = yaml.safe_load((bundle_root / "resources" / "uc_setup.yml").read_text())["resources"]["jobs"]["uc_setup"]
    (task,) = job["tasks"]
    wheel = task["python_wheel_task"]
    assert wheel["package_name"] == "banking_etl"
    params = wheel["parameters"]
    for var in ("catalog", "schema_prefix", "create_catalog"):
        assert any(f"${{var.{var}}}" in p for p in params), var
        assert var in bundle["variables"], var
    env = {e["environment_key"]: e for e in job["environments"]}[task["environment_key"]]
    assert "../dist/*.whl" in env["spec"]["dependencies"]

    pyproject = tomllib.loads((bundle_root / "pyproject.toml").read_text())
    target = pyproject["project"]["scripts"][wheel["entry_point"]]
    module, func = target.split(":")
    mod = __import__(module, fromlist=[func])
    assert callable(getattr(mod, func))


def test_uc_setup_job_params_parse_with_defaults(bundle_root: Path, bundle):
    from banking_etl.config import EtlConfig
    from banking_etl.uc_setup import build_parser

    job = yaml.safe_load((bundle_root / "resources" / "uc_setup.yml").read_text())["resources"]["jobs"]["uc_setup"]
    params = job["tasks"][0]["python_wheel_task"]["parameters"]
    defaults = {k: str(v["default"]) for k, v in bundle["variables"].items()}
    resolved = [p for p in params]
    for name, value in defaults.items():
        resolved = [p.replace(f"${{var.{name}}}", value) for p in resolved]
    args = build_parser().parse_args(resolved)
    cfg = EtlConfig.from_namespace(args, environ={})
    assert cfg == EtlConfig()
    assert args.create_catalog is True
