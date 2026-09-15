import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "databricks" / "ddl"))

from run_ddl import local_session_builder, run_ddl  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = local_session_builder(str(warehouse), app_name="banking-ddl-tests").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def warehouse(spark):
    """A Spark session with the full DDL applied in local (two-level name) mode."""
    run_ddl(spark, local=True)
    # Re-running must be a no-op: the DDL is idempotent.
    run_ddl(spark, local=True)
    return spark
