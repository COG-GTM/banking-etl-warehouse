import json
import os
import socket
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from banking_etl.bronze.sqlserver import local_spark

    session = local_spark("bronze_sqlserver_tests", warehouse=str(tmp_path_factory.mktemp("warehouse")))
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session


@pytest.fixture(scope="session")
def fixture_meta():
    return json.loads((FIXTURES / "schemas.json").read_text())


@pytest.fixture(scope="session")
def source_frames(spark, fixture_meta):
    """The six `sample` DB tables as extracted from sample.bak, with JDBC-equivalent types."""
    return {
        name: spark.read.schema(ddl)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .json(str(FIXTURES / f"{name}.jsonl"))
        for name, ddl in fixture_meta["schemas"].items()
    }


@pytest.fixture
def schema_prefix(spark, request):
    """A fresh, test-scoped bronze schema prefix (<prefix>bronze)."""
    prefix = f"t3_{request.node.name.lower()[:40]}_".replace("[", "_").replace("]", "_").replace("-", "_")
    spark.sql(f"DROP SCHEMA IF EXISTS {prefix}bronze CASCADE")
    return prefix


def _sqlserver_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def sqlserver_conn():
    """Connection to a local SQL Server with sample.bak restored; skips when unavailable."""
    from banking_etl.bronze.sqlserver import load_config, resolve_connection

    if not os.environ.get("SQLSERVER_PASSWORD"):
        pytest.skip("SQLSERVER_PASSWORD not set (no local SQL Server with sample.bak)")
    # local Docker SQL Server presents a self-signed certificate
    trust = os.environ.get("SQLSERVER_TRUST_SERVER_CERTIFICATE", "true")
    conn = resolve_connection(load_config(), overrides={"trust_server_certificate": trust})
    if not _sqlserver_reachable(conn.host, int(conn.port)):
        pytest.skip(f"SQL Server not reachable at {conn.host}:{conn.port}")
    return conn
