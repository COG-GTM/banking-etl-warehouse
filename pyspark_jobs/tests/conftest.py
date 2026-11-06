import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    from pyspark_jobs.common.config import EtlConfig

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = SparkSession.builder
    if os.environ.get("RUN_SQLSERVER_TESTS") == "1":
        # One SparkSession serves every test module, so it must carry the JDBC driver
        # for the SQL Server parity tests.
        settings = EtlConfig.from_env().spark
        if settings.jars:
            builder = builder.config("spark.jars", settings.jars)
        else:
            builder = builder.config("spark.jars.packages", settings.jdbc_packages)
        if settings.repositories:
            builder = builder.config("spark.jars.repositories", settings.repositories)
    session = (
        builder.master("local[2]")
        .appName("banking-etl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
