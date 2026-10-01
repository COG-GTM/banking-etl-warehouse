"""Configuration loading.

Settings come from a YAML file (``config/config.yaml`` by default) whose values may
reference environment variables as ``${VAR}`` or ``${VAR:-default}``. Credentials are
never stored in the file: they are either injected as environment variables or
resolved at runtime from AWS Secrets Manager via ``secret_id``.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import boto3
import yaml

CONFIG_ENV_VAR = "BANKING_ETL_CONFIG"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def interpolate_env(value: str, env: Mapping[str, str]) -> str:
    def _sub(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        resolved = env.get(name)
        if resolved:
            return resolved
        if default is not None:
            return default
        return ""

    return _ENV_PATTERN.sub(_sub, value)


def _interpolate_tree(node: object, env: Mapping[str, str]) -> object:
    if isinstance(node, dict):
        return {key: _interpolate_tree(val, env) for key, val in node.items()}
    if isinstance(node, list):
        return [_interpolate_tree(item, env) for item in node]
    if isinstance(node, str):
        return interpolate_env(node, env)
    return node


def resolve_path(path: str, base_dir: Path) -> str:
    """Resolve relative local paths against the config file directory.

    URIs (``s3a://``, ``dbfs:/``, ``file:/``) and absolute paths are returned unchanged.
    """
    if not path or _URI_SCHEME.match(path) or os.path.isabs(path):
        return path
    return str((base_dir / path).resolve())


@dataclass(frozen=True)
class SqlServerSource:
    jdbc_url: str
    db_schema: str
    user: str = ""
    password: str = ""
    secret_id: str = ""
    aws_region: str = ""
    fetch_size: int = 10000

    def credentials(self) -> tuple[str, str]:
        if self.secret_id:
            return fetch_db_secret(self.secret_id, self.aws_region)
        return self.user, self.password


@dataclass(frozen=True)
class ExcelSource:
    path: str
    sheet: str
    engine: str
    timestamp_format: str


@dataclass(frozen=True)
class CsvSource:
    path: str
    delimiter: str
    timestamp_format: str
    encoding: str
    mode: str


@dataclass(frozen=True)
class TableSettings:
    write_mode: str


@dataclass(frozen=True)
class WarehouseSettings:
    storage: str
    catalog: str
    db_schema: str
    base_path: str

    @property
    def path_based(self) -> bool:
        return self.storage == "path"

    def schema_name(self) -> str:
        return ".".join(p for p in (self.catalog, self.db_schema) if p)

    def table_name(self, table: str) -> str:
        return f"{self.schema_name()}.{table}"

    def table_path(self, table: str) -> str:
        return f"{self.base_path.rstrip('/')}/{table}"


@dataclass(frozen=True)
class EtlConfig:
    app_name_prefix: str
    warehouse: WarehouseSettings
    sqlserver: SqlServerSource
    transaction_excel: ExcelSource
    transaction_csv: CsvSource
    tables: dict[str, TableSettings] = field(default_factory=dict)
    orphan_policy: str = "reject"

    def table_settings(self, table: str) -> TableSettings:
        return self.tables.get(table, TableSettings(write_mode="merge"))


def fetch_db_secret(secret_id: str, region: str = "") -> tuple[str, str]:
    """Return ``(username, password)`` from a Secrets Manager JSON secret."""
    client = boto3.client("secretsmanager", region_name=region or None)
    payload = json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
    return payload["username"], payload["password"]


def load_config(
    path: str | os.PathLike[str] | None = None, env: Mapping[str, str] | None = None
) -> EtlConfig:
    env = os.environ if env is None else env
    config_path = Path(path or env.get(CONFIG_ENV_VAR) or DEFAULT_CONFIG_PATH).resolve()
    with config_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    data = _interpolate_tree(raw, env)
    assert isinstance(data, dict)
    base_dir = config_path.parent

    wh = data["warehouse"]
    src = data["sources"]
    sql = src["sqlserver"]
    xls = src["transaction_excel"]
    csv = src["transaction_csv"]

    return EtlConfig(
        app_name_prefix=data.get("app_name_prefix", "banking-etl"),
        warehouse=WarehouseSettings(
            storage=_choice(wh.get("storage", "path"), ("path", "table"), "warehouse.storage"),
            catalog=wh.get("catalog", ""),
            db_schema=wh.get("schema", "dwh"),
            base_path=resolve_path(wh.get("base_path", ""), base_dir),
        ),
        sqlserver=SqlServerSource(
            jdbc_url=sql["jdbc_url"],
            db_schema=sql.get("schema", "dbo"),
            user=sql.get("user", ""),
            password=sql.get("password", ""),
            secret_id=sql.get("secret_id", ""),
            aws_region=sql.get("aws_region", ""),
            fetch_size=int(sql.get("fetch_size", 10000)),
        ),
        transaction_excel=ExcelSource(
            path=resolve_path(xls["path"], base_dir),
            sheet=xls.get("sheet", "Sheet1"),
            engine=_choice(xls.get("engine", "pandas"), ("pandas", "spark_excel"), "excel engine"),
            timestamp_format=xls.get("timestamp_format", "dd-MM-yyyy HH:mm:ss"),
        ),
        transaction_csv=CsvSource(
            path=resolve_path(csv["path"], base_dir),
            delimiter=csv.get("delimiter", ","),
            timestamp_format=csv.get("timestamp_format", "dd-MM-yyyy HH:mm:ss"),
            encoding=csv.get("encoding", "ISO-8859-15"),
            mode=csv.get("mode", "DROPMALFORMED"),
        ),
        tables={
            name: TableSettings(
                write_mode=_choice(
                    settings.get("write_mode", "merge"), ("merge", "overwrite"), f"{name}.write_mode"
                )
            )
            for name, settings in (data.get("tables") or {}).items()
        },
        orphan_policy=_choice(
            data.get("orphan_policy", "reject"), ("reject", "keep", "fail"), "orphan_policy"
        ),
    )


def _choice(value: object, allowed: tuple[str, ...], setting: str) -> str:
    text = str(value).strip().lower()
    if text not in allowed:
        raise ValueError(f"Invalid {setting}: {value!r} (expected one of {', '.join(allowed)})")
    return text
