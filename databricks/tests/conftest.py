import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "jobs"))

REPO_ROOT = os.path.abspath(os.path.join(ROOT, ".."))


class _Widgets:
    def __init__(self, values):
        self.values = dict(values)

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class _Secrets:
    def __init__(self, secrets):
        self.secrets = secrets

    def get(self, scope, key):
        return self.secrets[(scope, key)]


class FakeDbutils:
    def __init__(self, widgets=None, secrets=None):
        self.widgets = _Widgets(widgets or {})
        self.secrets = _Secrets(secrets or {})


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]").appName("dwh-tests").config("spark.ui.enabled", "false").getOrCreate()
    )
    yield session
    session.stop()
